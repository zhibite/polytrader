from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from ..config import get_settings
from ..db import now_ts, to_json
from .polymarket import FutureNewsClient, GammaClient, parse_slug

STEP_SECONDS = {"5m": 300, "15m": 900}


def _day(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%d")


def _upsert_market(conn: sqlite3.Connection, market: dict[str, Any]) -> None:
    conn.execute(
        """INSERT INTO markets(slug, condition_id, title, asset, period, start_epoch,
               end_epoch, resolved, up_token, down_token, up_outcome, down_outcome,
               volume, liquidity, source, updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(slug) DO UPDATE SET
             condition_id=COALESCE(excluded.condition_id, markets.condition_id),
             title=COALESCE(excluded.title, markets.title),
             resolved=COALESCE(excluded.resolved, markets.resolved),
             up_token=COALESCE(excluded.up_token, markets.up_token),
             down_token=COALESCE(excluded.down_token, markets.down_token),
             volume=COALESCE(excluded.volume, markets.volume),
             liquidity=COALESCE(excluded.liquidity, markets.liquidity),
             source=COALESCE(excluded.source, markets.source),
             updated_at=excluded.updated_at""",
        (
            market.get("slug"),
            market.get("condition_id"),
            market.get("title"),
            market.get("asset"),
            market.get("period"),
            market.get("start_epoch"),
            market.get("end_epoch"),
            market.get("resolved"),
            market.get("up_token"),
            market.get("down_token"),
            market.get("up_outcome"),
            market.get("down_outcome"),
            market.get("volume"),
            market.get("liquidity"),
            market.get("source", "app"),
            now_ts(),
        ),
    )


def get_market(conn: sqlite3.Connection, slug: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM markets WHERE slug=?", (slug,)).fetchone()
    return dict(row) if row else None


def _load_from_dataset(conn: sqlite3.Connection, slug: str) -> dict[str, Any] | None:
    settings = get_settings()
    if not settings.klines_dataset_dir:
        return None
    asset, period, start = parse_slug(slug)
    if not asset or not period or start is None:
        return None
    base = Path(settings.klines_dataset_dir)
    day = _day(start)
    kline_path = base / f"asset={asset}" / f"period={period}" / f"date={day}" / "klines.csv.gz"
    market_path = base / f"asset={asset}" / f"period={period}" / f"date={day}" / "markets.csv.gz"
    if not kline_path.exists():
        return None
    market: dict[str, Any] = {
        "slug": slug,
        "asset": asset,
        "period": period,
        "start_epoch": start,
        "end_epoch": start + STEP_SECONDS.get(period, 300),
        "source": "dataset",
    }
    if market_path.exists():
        meta = _read_csv_cached(str(market_path), market_path.stat().st_mtime)
        row = meta[meta["slug"] == slug]
        if not row.empty:
            rec = row.iloc[0].to_dict()
            market.update(
                {
                    "condition_id": rec.get("condition_id") or None,
                    "title": rec.get("title") or None,
                    "resolved": (str(rec.get("resolved") or "").upper() or None),
                    "up_token": str(rec.get("up_token") or "") or None,
                    "down_token": str(rec.get("down_token") or "") or None,
                    "volume": _float(rec.get("volume")),
                    "liquidity": _float(rec.get("liquidity")),
                }
            )
            if rec.get("start_epoch"):
                market["start_epoch"] = int(rec["start_epoch"])
            if rec.get("end_epoch"):
                market["end_epoch"] = int(rec["end_epoch"])
    frame = _read_csv_cached(str(kline_path), kline_path.stat().st_mtime)
    frame = frame[frame["slug"] == slug]
    if frame.empty:
        return None
    rows = []
    for rec in frame.itertuples(index=False):
        ts = int(rec.time) // 1000
        rows.append(
            (
                slug,
                ts,
                _float(getattr(rec, "up_price", None)),
                _float(getattr(rec, "down_price", None)),
            )
        )
    conn.executemany(
        "INSERT OR IGNORE INTO samples(slug, ts, up_price, down_price) VALUES(?,?,?,?)",
        rows,
    )
    _upsert_market(conn, market)
    return market


def _load_from_future_news(
    conn: sqlite3.Connection, slug: str, client: FutureNewsClient | None = None
) -> dict[str, Any] | None:
    asset, period, start = parse_slug(slug)
    if not asset or not period or start is None:
        return None
    owns_client = client is None
    client = client or FutureNewsClient()
    try:
        info = client.event(slug)
        if not info:
            return None
        asset_ids = info.get("assetIds") or []
        outcomes = info.get("outcomes") or []
        up_token = down_token = None
        for index, token in enumerate(asset_ids):
            label = str(outcomes[index]).strip().lower() if index < len(outcomes) else ""
            if label == "up" and not up_token:
                up_token = str(token)
            elif label == "down" and not down_token:
                down_token = str(token)
        if not up_token and asset_ids:
            up_token = str(asset_ids[0])
        if not down_token and len(asset_ids) > 1:
            down_token = str(asset_ids[1])
        if not up_token or not down_token:
            return None
        referer = f"https://future.news/event/{slug}"
        up_rows = client.token_kline(up_token, period=period, referer=referer)
        down_rows = client.token_kline(down_token, period=period, referer=referer)
        by_ts: dict[int, dict[str, float | None]] = {}
        for row in up_rows:
            by_ts.setdefault(row["ts"], {})["up_price"] = row["price"]
        for row in down_rows:
            by_ts.setdefault(row["ts"], {})["down_price"] = row["price"]
        if not by_ts:
            return None
        payload = [
            (slug, ts, values.get("up_price"), values.get("down_price"))
            for ts, values in sorted(by_ts.items())
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO samples(slug, ts, up_price, down_price) VALUES(?,?,?,?)",
            payload,
        )
        market = {
            "slug": slug,
            "asset": asset,
            "period": period,
            "start_epoch": start,
            "end_epoch": start + STEP_SECONDS.get(period, 300),
            "up_token": up_token,
            "down_token": down_token,
            "title": info.get("title") or info.get("question"),
            "condition_id": info.get("conditionId") or info.get("condition_id"),
            "source": "future.news",
        }
        _upsert_market(conn, market)
        return market
    finally:
        if owns_client:
            client.close()


def _load_from_gamma(conn: sqlite3.Connection, slug: str) -> dict[str, Any] | None:
    asset, period, start = parse_slug(slug)
    if not asset or not period or start is None:
        return None
    client = GammaClient()
    try:
        market = client.market_by_slug(slug)
        if not market:
            return None
        from .polymarket import parse_clob_tokens

        up_token, down_token = parse_clob_tokens(market)
        record = {
            "slug": slug,
            "asset": asset,
            "period": period,
            "start_epoch": start,
            "end_epoch": start + STEP_SECONDS.get(period, 300),
            "up_token": up_token,
            "down_token": down_token,
            "title": market.get("question") or market.get("title"),
            "condition_id": market.get("conditionId") or market.get("condition_id"),
            "volume": _float(market.get("volume")),
            "liquidity": _float(market.get("liquidity")),
            "source": "gamma",
        }
        _upsert_market(conn, record)
        return record
    finally:
        client.close()


def ensure_samples(
    conn: sqlite3.Connection,
    slug: str,
    force: bool = False,
    news_client: FutureNewsClient | None = None,
) -> dict[str, Any] | None:
    """Ensure market metadata and 1s samples exist for a slug.

    Order: local dataset → future.news 1s API → Gamma metadata only. The last
    fallback still allows pair/volume analysis, just without markouts.
    """
    if not force:
        row = conn.execute("SELECT COUNT(*) AS n FROM samples WHERE slug=?", (slug,)).fetchone()
        if row and row["n"] > 0:
            market = get_market(conn, slug)
            if market:
                return market
    market = _load_from_dataset(conn, slug)
    if market:
        conn.commit()
        return market
    market = _load_from_future_news(conn, slug, news_client)
    if market:
        conn.commit()
        return market
    market = _load_from_gamma(conn, slug)
    if market:
        conn.commit()
        return market
    return get_market(conn, slug)


def fetch_resolutions(conn: sqlite3.Connection, conditions: list[str]) -> int:
    """Fetch Data API resolution rows and mark markets UP/DOWN."""
    conditions = [c for c in dict.fromkeys(conditions) if c]
    if not conditions:
        return 0
    from .polymarket import PolymarketClient

    client = PolymarketClient()
    updated = 0
    try:
        rows = client.resolutions(conditions)
    finally:
        client.close()
    for row in rows:
        condition = (row.get("condition_id") or "").lower()
        payouts = row.get("payouts")
        resolved = None
        if isinstance(payouts, list) and len(payouts) >= 2:
            if payouts[0] > payouts[1] and payouts[0] > 0:
                resolved = "UP"
            elif payouts[1] > payouts[0] and payouts[1] > 0:
                resolved = "DOWN"
        if not condition or not resolved:
            continue
        conn.execute(
            """UPDATE markets SET resolved=?, updated_at=?
               WHERE lower(condition_id)=?""",
            (resolved, now_ts(), condition),
        )
        updated += 1
    conn.commit()
    return updated


@lru_cache(maxsize=24)
def _read_csv_cached(path: str, mtime: float):
    """Cache one day-file at a time.

    The ingest worker walks many slugs from the same date; without this,
    every slug re-reads the same 80k-row gzipped CSV.
    """
    import pandas as pd

    return pd.read_csv(path)


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
