from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any

import websockets

from ..config import get_settings
from ..db import connect, now_ts, set_state, to_json
from .polymarket import (
    GammaClient,
    PolymarketClient,
    market_slug,
    parse_clob_tokens,
)

WINDOW_SECONDS = 300


def current_window(now: int | None = None) -> int:
    now = now or int(time.time())
    return (now // WINDOW_SECONDS) * WINDOW_SECONDS


def _iso(ts: int | float | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(float(ts), timezone.utc).isoformat()


class CollectorManager:
    """Live BTC 5m collector.

    Discovers the current ``btc-updown-5m-<epoch>`` market via Gamma,
    subscribes to both outcome tokens on the CLOB market channel and records
    book snapshots plus trade prints. It also polls the target wallet's fills
    so the dossier can show live activity.
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._status: dict[str, Any] = {
            "running": False,
            "connected": False,
            "slug": None,
            "window_start": None,
            "window_end": None,
            "up_token": None,
            "down_token": None,
            "messages": 0,
            "books": 0,
            "trades": 0,
            "last_event_ts": None,
            "last_error": None,
            "started_at": None,
            "updated_at": None,
        }
        self._enabled = self.settings.collector_enabled

    # ── lifecycle ───────────────────────────────────────────────────────────
    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="collector")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    def set_enabled(self, enabled: bool) -> None:
        self._enabled = enabled
        set_state("collector_enabled", "1" if enabled else "0")
        self._persist("collector_enabled", "1" if enabled else "0")

    def status(self) -> dict[str, Any]:
        return dict(self._status)

    # ── persistence ─────────────────────────────────────────────────────────
    def _persist(self, key: str, value: str) -> None:
        try:
            from ..db import set_state as _set

            _set(key, value)
        except Exception:
            pass

    def _persist_status(self) -> None:
        self._status["updated_at"] = now_ts()
        try:
            set_state("collector_status", to_json(self._status))
        except Exception:
            pass

    # ── main loop ───────────────────────────────────────────────────────────
    async def _run(self) -> None:
        self._status["running"] = True
        self._status["started_at"] = now_ts()
        self._persist_status()
        backoff = 2.0
        while not self._stop.is_set():
            if not self._enabled:
                self._status.update({"running": True, "connected": False, "slug": None})
                self._persist_status()
                await asyncio.sleep(5)
                continue
            try:
                await self._run_one_window()
                backoff = 2.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._status["last_error"] = str(exc)[:500]
                self._status["connected"] = False
                self._persist_status()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.8, 30.0)
        self._status["running"] = False
        self._status["connected"] = False
        self._persist_status()

    async def _discover(self, slug: str) -> dict[str, Any] | None:
        def _fetch() -> dict[str, Any] | None:
            client = GammaClient()
            try:
                return client.market_by_slug(slug)
            finally:
                client.close()

        return await asyncio.to_thread(_fetch)

    async def _run_one_window(self) -> None:
        start = current_window()
        slug = market_slug(self.settings.collector_asset, "5m", start)
        market = await self._discover(slug)
        while not market and not self._stop.is_set():
            # The market may not be indexed yet around the window boundary.
            if time.time() > start + WINDOW_SECONDS + 30:
                return
            await asyncio.sleep(3)
            market = await self._discover(slug)
        if not market:
            await asyncio.sleep(5)
            return
        up_token, down_token = parse_clob_tokens(market)
        if not up_token or not down_token:
            raise RuntimeError(f"market {slug} has no token ids")
        self._status.update(
            {
                "slug": slug,
                "window_start": start,
                "window_end": start + WINDOW_SECONDS,
                "up_token": up_token,
                "down_token": down_token,
                "last_error": None,
            }
        )
        self._persist_status()
        _upsert_market(slug, market, up_token, down_token)
        async with websockets.connect(
            self.settings.clob_ws,
            ping_interval=20,
            ping_timeout=20,
            max_size=None,
            open_timeout=20,
        ) as ws:
            await ws.send(json.dumps({"type": "market", "assets_ids": [up_token, down_token]}))
            self._status["connected"] = True
            self._persist_status()
            books: dict[str, dict[str, dict[float, float]]] = {
                up_token: {"bids": {}, "asks": {}},
                down_token: {"bids": {}, "asks": {}},
            }
            token_outcome = {up_token: "UP", down_token: "DOWN"}
            last_snapshot = 0.0
            last_poll = 0.0
            conn = connect()
            try:
                while not self._stop.is_set():
                    now = int(time.time())
                    if now >= start + WINDOW_SECONDS:
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    except asyncio.TimeoutError:
                        raw = None
                    if raw:
                        self._status["messages"] += 1
                        self._status["last_event_ts"] = now
                        self._handle_message(conn, raw, books, token_outcome, slug)
                    if time.monotonic() - last_snapshot >= self.settings.collector_interval:
                        last_snapshot = time.monotonic()
                        self._snapshot(conn, slug, books, token_outcome)
                    if time.monotonic() - last_poll >= self.settings.target_poll_seconds:
                        last_poll = time.monotonic()
                        await self._poll_target(conn)
                        self._persist_status()
            finally:
                conn.close()
        self._status["connected"] = False
        self._persist_status()
        # Move to the next window promptly.
        await asyncio.sleep(1)

    # ── message handling ────────────────────────────────────────────────────
    def _handle_message(
        self,
        conn: sqlite3.Connection,
        raw: str,
        books: dict[str, dict[str, dict[float, float]]],
        token_outcome: dict[str, str],
        slug: str,
    ) -> None:
        try:
            payload = json.loads(raw)
        except ValueError:
            return
        events = payload if isinstance(payload, list) else [payload]
        for event in events:
            if not isinstance(event, dict):
                continue
            event_type = event.get("event_type") or event.get("type")
            token = event.get("asset_id") or event.get("assetId")
            if event_type == "book" and token in books:
                self._apply_book(books[token], event)
            elif event_type == "price_change":
                # The market channel sends {"price_changes": [{asset_id, ...}]}
                # with one entry per outcome token.
                changes = event.get("price_changes") or event.get("changes") or []
                for change in changes:
                    change_token = change.get("asset_id") or token
                    if change_token in books:
                        self._apply_changes(books[change_token], [change])
            elif event_type == "last_trade_price":
                self._record_trade(conn, slug, token, token_outcome, event)
            elif event_type == "tick_size_change":
                continue

    def _apply_book(self, book: dict[str, dict[float, float]], event: dict[str, Any]) -> None:
        book["bids"] = _levels(event.get("bids") or event.get("buys") or [])
        book["asks"] = _levels(event.get("asks") or event.get("sells") or [])

    def _apply_changes(self, book: dict[str, dict[float, float]], changes: list[dict[str, Any]]) -> None:
        for change in changes:
            try:
                price = float(change.get("price"))
                size = float(change.get("size") or 0)
            except (TypeError, ValueError):
                continue
            side = str(change.get("side") or "").upper()
            target = book["bids"] if side == "BUY" else book["asks"]
            if size <= 0:
                target.pop(price, None)
            else:
                target[price] = size

    def _snapshot(
        self,
        conn: sqlite3.Connection,
        slug: str,
        books: dict[str, dict[str, dict[float, float]]],
        token_outcome: dict[str, str],
    ) -> None:
        rows = []
        for token, book in books.items():
            best_bid = max(book["bids"]) if book["bids"] else None
            best_ask = min(book["asks"]) if book["asks"] else None
            mid = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
            spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None
            bid_depth = sum(book["bids"].values())
            ask_depth = sum(book["asks"].values())
            rows.append(
                (
                    slug,
                    token,
                    token_outcome.get(token),
                    now_ts(),
                    best_bid,
                    best_ask,
                    mid,
                    spread,
                    bid_depth,
                    ask_depth,
                    None,
                )
            )
        if not rows:
            return
        conn.executemany(
            """INSERT INTO collector_books(
                 slug, token_id, outcome, ts, best_bid, best_ask, mid, spread,
                 bid_depth, ask_depth, raw_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        conn.commit()
        self._status["books"] += len(rows)

    def _record_trade(
        self,
        conn: sqlite3.Connection,
        slug: str,
        token: str | None,
        token_outcome: dict[str, str],
        event: dict[str, Any],
    ) -> None:
        try:
            price = float(event.get("price"))
            size = float(event.get("size") or 0)
        except (TypeError, ValueError):
            return
        ts = _parse_ts(event.get("timestamp"))
        conn.execute(
            """INSERT OR IGNORE INTO collector_trades(
                 slug, token_id, outcome, ts, price, size, side, raw_json)
               VALUES(?,?,?,?,?,?,?,?)""",
            (slug, token, token_outcome.get(token or ""), ts, price, size, event.get("side"), to_json(event)),
        )
        conn.commit()
        self._status["trades"] += 1

    # ── target polling ──────────────────────────────────────────────────────
    async def _poll_target(self, conn: sqlite3.Connection) -> None:
        address = self.settings.target_address.lower()

        def _fetch() -> list[dict[str, Any]]:
            client = PolymarketClient(rate=1.0)
            try:
                start = now_ts() - 180
                pages = client.trades(address, start, now_ts() + 60, taker_only=False)
                rows: list[dict[str, Any]] = []
                for page in pages:
                    rows.extend(page)
                return rows
            finally:
                client.close()

        try:
            rows = await asyncio.to_thread(_fetch)
        except Exception as exc:
            self._status["last_error"] = f"target poll: {exc}"[:500]
            return
        if not rows:
            return
        from .ingest import upsert_trades

        inserted = upsert_trades(conn, address, rows, 0)
        self._status["target_poll_rows"] = len(rows)
        self._status["target_poll_inserted"] = inserted
        self._persist_status()


def _parse_ts(value: Any) -> int:
    if value is None:
        return int(time.time())
    if isinstance(value, (int, float)):
        ts = int(value)
    else:
        text = str(value).strip()
        if text.isdigit():
            ts = int(text)
        else:
            try:
                ts = int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
            except ValueError:
                return int(time.time())
    if ts > 10_000_000_000:
        ts //= 1000
    return ts


def _levels(values: list[dict[str, Any]]) -> dict[float, float]:
    out: dict[float, float] = {}
    for level in values:
        if not isinstance(level, dict):
            continue
        try:
            price = float(level.get("price"))
            size = float(level.get("size") or 0)
        except (TypeError, ValueError):
            continue
        if size > 0:
            out[price] = size
    return out


def _upsert_market(slug: str, market: dict[str, Any], up_token: str, down_token: str) -> None:
    conn = connect()
    try:
        start = current_window()
        conn.execute(
            """INSERT OR REPLACE INTO collector_markets(
                 slug, asset, period, condition_id, start_epoch, end_epoch,
                 up_token, down_token, status, discovered_at, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                slug,
                "btc",
                "5m",
                market.get("conditionId") or market.get("condition_id"),
                start,
                start + WINDOW_SECONDS,
                up_token,
                down_token,
                market.get("closed") and "closed" or "open",
                now_ts(),
                now_ts(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def collector_status() -> dict[str, Any]:
    settings = get_settings()
    conn = connect()
    try:
        row = conn.execute("SELECT value FROM collector_state WHERE key='collector_status'").fetchone()
        enabled = conn.execute("SELECT value FROM collector_state WHERE key='collector_enabled'").fetchone()
        status = json.loads(row["value"]) if row else {}
        status["enabled"] = (enabled["value"] == "1") if enabled else settings.collector_enabled
        if not status["enabled"]:
            status["running"] = False
            status["connected"] = False
        return status
    finally:
        conn.close()


def recent_books(conn: sqlite3.Connection, limit: int = 40) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT * FROM collector_books ORDER BY id DESC LIMIT ?""", (limit,)
    ).fetchall()
    return [dict(row) for row in rows]


def recent_trades(conn: sqlite3.Connection, limit: int = 40) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT * FROM collector_trades ORDER BY id DESC LIMIT ?""", (limit,)
    ).fetchall()
    return [dict(row) for row in rows]


manager = CollectorManager()
