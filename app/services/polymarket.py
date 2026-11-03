from __future__ import annotations

import json
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterator

import httpx

from ..config import get_settings

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
FUTURE_COMMON_QUERY = {
    "fp_did": "unknown",
    "from_app": "future-web",
    "tz_name": "Asia/Shanghai",
    "tz_offset": "28800",
    "app_lang": "en-US",
    "os": "web",
    "worker": "0",
}


class ApiError(RuntimeError):
    pass


class RateLimiter:
    """Simple thread-safe minimum-interval limiter."""

    def __init__(self, rate_per_second: float) -> None:
        self.interval = 1.0 / rate_per_second if rate_per_second > 0 else 0.0
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        if self.interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            delay = self._last + self.interval - now
            if delay > 0:
                time.sleep(delay)
                now = time.monotonic()
            self._last = now


class PolymarketClient:
    """Data API v2 client with retries and cursor pagination."""

    def __init__(
        self,
        base: str | None = None,
        rate: float | None = None,
        timeout: float = 45.0,
        retries: int = 6,
    ) -> None:
        settings = get_settings()
        self.base = (base or settings.api_base).rstrip("/")
        self.rate = RateLimiter(rate if rate is not None else settings.request_rate)
        self.timeout = timeout
        self.retries = retries
        self.client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        self.request_count = 0

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "PolymarketClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base}/{path.lstrip('/')}"
        delay = 1.0
        last_error: str | None = None
        for attempt in range(self.retries):
            self.rate.wait()
            try:
                response = self.client.get(url, params=params or {})
                self.request_count += 1
                if response.status_code == 200:
                    return response.json()
                if response.status_code == 429 or 500 <= response.status_code < 600:
                    last_error = f"HTTP {response.status_code}"
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        delay = max(delay, float(retry_after))
                    time.sleep(delay + random.random() * 0.25)
                    delay = min(delay * 1.8, 60.0)
                    continue
                raise ApiError(
                    f"{url} -> HTTP {response.status_code}: {response.text[:300]}"
                )
            except (httpx.HTTPError, ValueError) as exc:
                last_error = repr(exc)
                time.sleep(delay + random.random() * 0.25)
                delay = min(delay * 1.8, 60.0)
        raise ApiError(f"{url} failed after {self.retries} attempts: {last_error}")

    # ── profile / aggregates ────────────────────────────────────────────────
    def user_stats(self, address: str) -> dict[str, Any]:
        payload = self.get("user-stats", {"user": address})
        return payload.get("data") or {}

    def user_volume(self, address: str) -> dict[str, Any]:
        payload = self.get("user-volume", {"user": address})
        return payload.get("data") or {}

    def user_pnl(self, address: str, interval: str = "max", fidelity: str = "1h") -> dict[str, Any]:
        payload = self.get("user-pnl", {"user": address, "interval": interval, "fidelity": fidelity})
        return payload.get("data") or {}

    def leaderboard(
        self,
        time_period: str = "day",
        category: str = "overall",
        sort_by: str = "PNL",
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "time_period": time_period,
            "category": category,
            "sort_by": sort_by,
            "limit": limit,
        }
        if cursor:
            params["cursor"] = cursor
        return self.get("leaderboard", params)

    def leaderboard_user(self, address: str) -> dict[str, Any]:
        payload = self.get("leaderboard", {"user": address, "limit": 1})
        return payload.get("data") or {}

    # ── paged feeds ─────────────────────────────────────────────────────────
    def paged(
        self,
        path: str,
        params: dict[str, Any],
        max_rows: int | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield data pages, following the v2 cursor until it runs out."""
        cursor: str | None = None
        seen = 0
        while True:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor
            payload = self.get(path, page_params)
            rows = payload.get("data") or []
            if rows:
                yield rows
                seen += len(rows)
            pagination = payload.get("pagination") or {}
            cursor = pagination.get("next_cursor")
            if not cursor or not rows:
                break
            if max_rows is not None and seen >= max_rows:
                break

    def trades(
        self,
        address: str,
        start: int,
        end: int,
        taker_only: bool = False,
        max_rows: int | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        return self.paged(
            "trades",
            {
                "user": address,
                "limit": 1000,
                "start": int(start),
                "end": int(end),
                "taker_only": "true" if taker_only else "false",
            },
            max_rows=max_rows,
        )

    def activity(
        self,
        address: str,
        start: int,
        end: int,
        types: str | None = None,
        max_rows: int | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        params: dict[str, Any] = {
            "user": address,
            "limit": 1000,
            "start": int(start),
            "end": int(end),
            "exclude_deposits_withdrawals": "false",
        }
        if types:
            params["type"] = types
        return self.paged("activity", params, max_rows=max_rows)

    def positions(self, address: str, status: str = "OPEN") -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for page in self.paged(
            "positions",
            {"user": address, "limit": 500, "status": status},
            max_rows=5000,
        ):
            rows.extend(page)
        return rows

    def resolutions(self, conditions: list[str]) -> list[dict[str, Any]]:
        """Resolve up to 20 conditions per request; returns raw rows."""
        out: list[dict[str, Any]] = []
        for i in range(0, len(conditions), 20):
            chunk = conditions[i : i + 20]
            payload = self.get("resolutions", {"condition": ",".join(chunk)})
            out.extend(payload.get("data") or [])
        return out


class GammaClient:
    def __init__(self, base: str | None = None, timeout: float = 30.0) -> None:
        settings = get_settings()
        self.base = (base or settings.gamma_base).rstrip("/")
        self.client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )

    def close(self) -> None:
        self.client.close()

    def market_by_slug(self, slug: str) -> dict[str, Any] | None:
        response = self.client.get(f"{self.base}/markets", params={"slug": slug})
        if response.status_code != 200:
            return None
        try:
            data = response.json()
        except ValueError:
            return None
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            items = data.get("data") or []
            return items[0] if items else None
        return None

    def event_by_slug(self, slug: str) -> dict[str, Any] | None:
        response = self.client.get(f"{self.base}/events", params={"slug": slug})
        if response.status_code != 200:
            return None
        try:
            data = response.json()
        except ValueError:
            return None
        if isinstance(data, list) and data:
            return data[0]
        return None


class FutureNewsClient:
    """future.news public kline source used by the existing scraper.

    It serves 1-second UP/DOWN prices for 5-minute markets. The client keeps
    cookies in its httpx session, which mirrors the scraper's Cloudflare
    ``__cf_bm`` handling.
    """

    def __init__(
        self,
        base: str | None = None,
        rate: float | None = None,
        timeout: float = 45.0,
        retries: int = 5,
    ) -> None:
        settings = get_settings()
        self.base = (base or settings.future_news_base).rstrip("/")
        self.web = "https://future.news"
        self.rate = RateLimiter(rate if rate is not None else settings.kline_rate)
        self.timeout = timeout
        self.retries = retries
        self.client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/plain, */*",
                "Origin": self.web,
            },
        )
        self._device_id = f"lab-{random.randrange(10**12):012d}"

    def close(self) -> None:
        self.client.close()

    def _get(self, path: str, params: dict[str, Any] | None = None, referer: str | None = None) -> dict[str, Any]:
        query = dict(FUTURE_COMMON_QUERY)
        query["device_id"] = self._device_id
        if params:
            query.update(params)
        url = f"{self.base}/{path.lstrip('/')}"
        headers = {"Referer": referer} if referer else {}
        delay = 1.0
        last_error: str | None = None
        for _ in range(self.retries):
            self.rate.wait()
            try:
                response = self.client.get(url, params=query, headers=headers)
                if response.status_code == 200:
                    payload = response.json()
                    if payload.get("code") not in (0, None):
                        raise ApiError(f"future.news code={payload.get('code')}: {payload.get('message')}")
                    return payload
                if response.status_code in (408, 425, 429, 500, 502, 503, 504):
                    time.sleep(delay + random.random() * 0.25)
                    delay = min(delay * 1.8, 30.0)
                    continue
                last_error = f"HTTP {response.status_code}"
                break
            except (httpx.HTTPError, ValueError) as exc:
                last_error = repr(exc)
                time.sleep(delay)
                delay = min(delay * 1.8, 30.0)
        raise ApiError(f"future.news failed for {url}: {last_error}")

    def event(self, slug: str) -> dict[str, Any] | None:
        payload = self._get(
            f"event/polymarket/{slug}",
            referer=f"{self.web}/event/{slug}",
        )
        data = payload.get("data") or {}
        markets = data.get("marketInfo") or []
        return markets[0] if markets else None

    def token_kline(self, token: str, period: str = "5m", referer: str | None = None) -> list[dict[str, Any]]:
        payload = self._get(
            f"token_kline/polymarket/{token}",
            params={"period": period},
            referer=referer or f"{self.web}/",
        )
        raw = ((payload.get("data") or {}).get("list")) or []
        rows: list[dict[str, Any]] = []
        for item in raw:
            try:
                rows.append(
                    {
                        "ts": int(item["time"]) // 1000,
                        "price": float(item["price"]),
                        "volume": float(item.get("volume") or 0),
                    }
                )
            except (KeyError, TypeError, ValueError):
                continue
        return rows


def parse_slug(slug: str) -> tuple[str | None, str | None, int | None]:
    """Parse ``btc-updown-5m-1793670300`` into (asset, period, start_epoch)."""
    parts = slug.split("-")
    if len(parts) >= 4 and parts[-3] == "updown" and parts[-1].isdigit():
        return parts[0], parts[-2], int(parts[-1])
    # Be tolerant of event slugs such as btc-up-or-down-5m-...
    for index, part in enumerate(parts):
        if part in ("updown", "down") and index >= 1 and index + 2 < len(parts):
            asset = parts[index - 1]
            period = parts[index + 1]
            tail = parts[index + 2]
            if tail.isdigit():
                return asset, period, int(tail)
    return None, None, None


def market_slug(asset: str, period: str, start_epoch: int) -> str:
    return f"{asset}-updown-{period}-{int(start_epoch)}"


def parse_clob_tokens(market: dict[str, Any]) -> tuple[str | None, str | None]:
    """Extract UP/DOWN token ids from a Gamma market payload."""
    raw_ids = market.get("clobTokenIds")
    if isinstance(raw_ids, str):
        try:
            raw_ids = json.loads(raw_ids)
        except ValueError:
            raw_ids = []
    raw_ids = raw_ids or []
    outcomes = market.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except ValueError:
            outcomes = []
    outcomes = outcomes or []
    up = down = None
    for index, token in enumerate(raw_ids):
        label = str(outcomes[index]).strip().lower() if index < len(outcomes) else ""
        if label in ("up", "yes") and not up:
            up = str(token)
        elif label in ("down", "no") and not down:
            down = str(token)
    if not up and len(raw_ids) >= 1:
        up = str(raw_ids[0])
    if not down and len(raw_ids) >= 2:
        down = str(raw_ids[1])
    return up, down
