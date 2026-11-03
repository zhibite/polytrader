from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from bisect import bisect_left, bisect_right
from datetime import datetime, timezone
from typing import Any, Callable

from ..config import get_settings
from ..db import now_ts, to_json
from .klines import ensure_samples, fetch_resolutions
from .polymarket import FutureNewsClient, PolymarketClient, parse_slug

ProgressFn = Callable[[int, int, str], None]

MARKOUT_HORIZONS = (("markout_1s", "mid_1s", 1), ("markout_10s", "mid_10s", 10), ("markout_60s", "mid_60s", 60))
SAMPLE_TOLERANCE = 10
END_TOLERANCE = 15

# The activity feed duplicates every fill as a TRADE row. We already store
# fills from /v2/trades, so only pull the cash-flow and rebate event types.
ACTIVITY_TYPES = (
    "SPLIT,MERGE,REDEEM,CONVERSION,MIGRATION,REWARD,YIELD,"
    "MAKER_REBATE,TAKER_REBATE,REFERRAL_REWARD,DEPOSIT,WITHDRAWAL"
)


def trade_key(row: dict[str, Any]) -> str:
    raw = "|".join(
        str(row.get(key) or "")
        for key in (
            "transaction_hash",
            "token_id",
            "side",
            "size",
            "price",
            "timestamp",
            "outcome_index",
        )
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def activity_key(row: dict[str, Any]) -> str:
    raw = "|".join(
        str(row.get(key) or "")
        for key in (
            "type",
            "transaction_hash",
            "token_id",
            "side",
            "size",
            "usdc_size",
            "timestamp",
            "outcome_index",
        )
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _slug_times(slug: str | None) -> tuple[int | None, int | None]:
    if not slug:
        return None, None
    asset, period, start = parse_slug(slug)
    if start is None:
        return None, None
    step = {"5m": 300, "15m": 900}.get(period or "5m", 300)
    return start, start + step


def _window_meta(conn: sqlite3.Connection, address: str, days: int, start: int, end: int) -> tuple[int, int]:
    row = conn.execute(
        "SELECT * FROM ingest_checkpoints WHERE address=? AND stream='meta' AND days=?",
        (address, days),
    ).fetchone()
    if row and row["window_start"] and row["window_end"]:
        return int(row["window_start"]), int(row["window_end"])
    conn.execute(
        """INSERT OR REPLACE INTO ingest_checkpoints(
             address, stream, days, window_start, window_end, cursor, rows, signature, updated_at)
           VALUES(?, 'meta', ?, ?, ?, NULL, 0, NULL, ?)""",
        (address, days, start, end, now_ts()),
    )
    conn.commit()
    return start, end


def _paged_resumable(
    conn: sqlite3.Connection,
    client: PolymarketClient,
    path: str,
    params: dict[str, Any],
    address: str,
    stream: str,
    days: int,
    max_rows: int,
    on_rows: Callable[[list[dict[str, Any]]], Any],
    progress: ProgressFn | None,
    progress_base: float,
    progress_span: float,
) -> int:
    """Page a v2 feed with a cursor checkpoint after every page.

    A restart with the same ``(address, days, stream)`` continues from the
    last saved cursor; successful ingest clears the checkpoints so the next
    refresh starts from the newest page again.
    """
    signature = hashlib.sha1(
        json.dumps({"path": path, "params": params}, sort_keys=True).encode("utf-8")
    ).hexdigest()
    row = conn.execute(
        "SELECT * FROM ingest_checkpoints WHERE address=? AND stream=? AND days=?",
        (address, stream, days),
    ).fetchone()
    cursor = row["cursor"] if row and row["signature"] == signature else None
    seen = int(row["rows"]) if row and row["signature"] == signature else 0
    window_start = int(row["window_start"]) if row else int(params.get("start") or 1)
    window_end = int(row["window_end"]) if row else int(params.get("end") or now_ts())
    if row and row["signature"] == signature:
        if progress:
            progress(progress_base, 100, f"{stream}: resuming at {seen} rows")
    conn.execute(
        """INSERT OR REPLACE INTO ingest_checkpoints(
             address, stream, days, window_start, window_end, cursor, rows, signature, updated_at)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        (address, stream, days, window_start, window_end, cursor, seen, signature, now_ts()),
    )
    conn.commit()
    while True:
        page_params = dict(params)
        if cursor:
            page_params["cursor"] = cursor
        payload = client.get(path, page_params)
        rows = payload.get("data") or []
        if rows:
            on_rows(rows)
            seen += len(rows)
        cursor = (payload.get("pagination") or {}).get("next_cursor")
        conn.execute(
            """UPDATE ingest_checkpoints SET cursor=?, rows=?, updated_at=?
               WHERE address=? AND stream=? AND days=?""",
            (cursor, seen, now_ts(), address, stream, days),
        )
        conn.commit()
        if progress:
            fraction = min(0.98, seen / max_rows) if max_rows else 0.5
            progress(progress_base + fraction * progress_span, 100, f"{stream}: {seen:,} rows")
        if not cursor or not rows or (max_rows and seen >= max_rows):
            break
    return seen


def _clear_checkpoints(conn: sqlite3.Connection, address: str, days: int) -> None:
    conn.execute("DELETE FROM ingest_checkpoints WHERE address=? AND days=?", (address, days))
    conn.commit()


def upsert_trades(
    conn: sqlite3.Connection,
    address: str,
    rows: list[dict[str, Any]],
    is_taker: int,
) -> int:
    payload = []
    for row in rows:
        slug = row.get("slug") or ""
        start, end = _slug_times(slug)
        payload.append(
            (
                address,
                trade_key(row),
                row.get("transaction_hash"),
                row.get("token_id"),
                row.get("condition_id"),
                slug,
                row.get("outcome"),
                row.get("outcome_index"),
                row.get("side"),
                _float(row.get("price")),
                _float(row.get("size")),
                (_float(row.get("price")) or 0) * (_float(row.get("size")) or 0),
                int(row.get("timestamp") or 0),
                int(is_taker),
                row.get("title"),
                row.get("event_slug"),
                start,
                end,
            )
        )
    cursor = conn.executemany(
        """INSERT OR IGNORE INTO trades(
              address, trade_key, transaction_hash, token_id, condition_id, slug,
              outcome, outcome_index, side, price, size, usdc_size, ts, is_taker,
              title, event_slug, market_start, market_end)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        payload,
    )
    inserted = max(0, cursor.rowcount)
    if is_taker:
        keys = [p[1] for p in payload]
        conn.executemany(
            "UPDATE trades SET is_taker=1 WHERE address=? AND trade_key=?",
            [(address, key) for key in keys],
        )
    conn.commit()
    return inserted


def upsert_activity(conn: sqlite3.Connection, address: str, rows: list[dict[str, Any]]) -> int:
    payload = [
        (
            address,
            activity_key(row),
            int(row.get("timestamp") or 0),
            row.get("type"),
            row.get("condition_id"),
            row.get("token_id"),
            row.get("side"),
            _float(row.get("size")),
            _float(row.get("usdc_size")),
            _float(row.get("price")),
            row.get("outcome"),
            row.get("outcome_index"),
            row.get("title"),
            row.get("slug"),
            row.get("transaction_hash"),
        )
        for row in rows
    ]
    cursor = conn.executemany(
        """INSERT OR IGNORE INTO activity(
              address, activity_key, ts, type, condition_id, token_id, side, size,
              usdc_size, price, outcome, outcome_index, title, slug, transaction_hash)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        payload,
    )
    conn.commit()
    return max(0, cursor.rowcount)


def upsert_stats(
    conn: sqlite3.Connection,
    address: str,
    stats: dict[str, Any],
    volume: dict[str, Any] | None = None,
    pnl: dict[str, Any] | None = None,
) -> None:
    all_time = stats.get("all_time_pnl") or {}
    volume = volume or {}
    conn.execute(
        """INSERT INTO wallet_stats(address, fetched_at, join_date, markets, fills,
               volume_shares, volume_usdc, realized_pnl, unrealized_pnl, economic_pnl,
               settled_pnl, trade_pnl, maker_rebate, taker_rebate, reward_income,
               referral_income, fees, biggest_win, views, raw_json)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(address) DO UPDATE SET
             fetched_at=excluded.fetched_at, join_date=excluded.join_date,
             markets=excluded.markets, fills=excluded.fills,
             volume_shares=excluded.volume_shares, volume_usdc=excluded.volume_usdc,
             realized_pnl=excluded.realized_pnl, unrealized_pnl=excluded.unrealized_pnl,
             economic_pnl=excluded.economic_pnl, settled_pnl=excluded.settled_pnl,
             trade_pnl=excluded.trade_pnl, maker_rebate=excluded.maker_rebate,
             taker_rebate=excluded.taker_rebate, reward_income=excluded.reward_income,
             referral_income=excluded.referral_income, fees=excluded.fees,
             biggest_win=excluded.biggest_win, views=excluded.views,
             raw_json=excluded.raw_json""",
        (
            address,
            now_ts(),
            stats.get("join_date"),
            stats.get("trades"),
            volume.get("trade_count") or all_time.get("trade_count"),
            volume.get("volume") or all_time.get("volume"),
            volume.get("volume_usdc") or all_time.get("volume_usdc"),
            all_time.get("realized_pnl"),
            all_time.get("unrealized_pnl"),
            all_time.get("economic_pnl"),
            all_time.get("settled_pnl"),
            all_time.get("trade_pnl"),
            all_time.get("maker_rebate"),
            all_time.get("taker_rebate"),
            all_time.get("reward_income"),
            all_time.get("referral_income"),
            all_time.get("fees_paid"),
            stats.get("biggest_win"),
            stats.get("views"),
            to_json({"stats": stats, "volume": volume, "pnl": all_time}),
        ),
    )
    if pnl and pnl.get("points"):
        conn.executemany(
            """INSERT OR REPLACE INTO pnl_points(
                 address, ts, realized_pnl, unrealized_pnl, economic_pnl, position_pnl,
                 maker_rebate, taker_rebate, fees, volume_usdc, volume_shares, trade_count)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (
                    address,
                    int(point.get("timestamp") or 0),
                    point.get("realized_pnl"),
                    point.get("unrealized_pnl"),
                    point.get("economic_pnl"),
                    point.get("position_pnl"),
                    point.get("maker_rebate"),
                    point.get("taker_rebate"),
                    point.get("fees"),
                    point.get("volume_usdc"),
                    point.get("volume"),
                    point.get("trade_count"),
                )
                for point in pnl["points"]
                if point.get("timestamp")
            ],
        )
    conn.commit()


def upsert_positions(conn: sqlite3.Connection, address: str, rows: list[dict[str, Any]]) -> None:
    conn.executemany(
        """INSERT OR REPLACE INTO positions(
              address, token_id, condition_id, status, current_size, avg_price,
              current_price, current_value, realized_pnl, unrealized_pnl, total_pnl,
              title, slug, outcome, end_date, updated_at, raw_json)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (
                address,
                row.get("token_id"),
                row.get("condition_id"),
                row.get("status"),
                row.get("current_size"),
                row.get("avg_price"),
                row.get("current_price"),
                row.get("current_value"),
                row.get("realized_pnl"),
                row.get("unrealized_pnl"),
                row.get("total_pnl"),
                row.get("title"),
                row.get("slug"),
                row.get("outcome"),
                row.get("end_date"),
                now_ts(),
                to_json(row),
            )
            for row in rows
        ],
    )
    conn.commit()


def update_resolutions(conn: sqlite3.Connection, address: str) -> int:
    rows = conn.execute(
        """SELECT t.condition_id AS condition_id, MAX(t.slug) AS slug, MAX(t.title) AS title,
                  MAX(m.resolved) AS market_resolved
           FROM trades t
           LEFT JOIN markets m ON lower(m.condition_id)=lower(t.condition_id)
           WHERE t.address=? AND t.condition_id IS NOT NULL AND t.condition_id != ''
           GROUP BY t.condition_id""",
        (address,),
    ).fetchall()
    conditions = [
        row["condition_id"]
        for row in rows
        if not row["market_resolved"]
    ]
    if not rows:
        return 0
    # Make sure every condition has a markets row so the resolution update has
    # somewhere to land even when klines were not fetched for that slug.
    now = now_ts()
    for row in rows:
        slug = row["slug"] or ""
        asset, period, start = parse_slug(slug) if slug else (None, None, None)
        conn.execute(
            """INSERT OR IGNORE INTO markets(
                 slug, condition_id, title, asset, period, start_epoch, end_epoch, source, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                slug or f"condition-{row['condition_id'][:12]}",
                row["condition_id"],
                row["title"],
                asset,
                period,
                start,
                (start + 300) if start else None,
                "trades",
                now,
            ),
        )
    conn.commit()
    updated = fetch_resolutions(conn, conditions)
    conn.execute(
        """UPDATE trades SET resolved = (
               SELECT resolved FROM markets m WHERE lower(m.condition_id)=lower(trades.condition_id)
           )
           WHERE address=? AND resolved IS NULL""",
        (address,),
    )
    conn.commit()
    return updated


def compute_pair_markets(conn: sqlite3.Connection, address: str) -> int:
    """Aggregate fills into per-condition pairing / settlement economics."""
    conn.execute("DELETE FROM pair_markets WHERE address=?", (address,))
    rows = conn.execute(
        """SELECT condition_id,
                  COALESCE(slug, '') AS slug,
                  MAX(title) AS title,
                  MAX(market_start) AS market_start,
                  MAX(market_end) AS market_end,
                  MAX(resolved) AS resolved,
                  SUM(CASE WHEN side='BUY' AND outcome_index=0 THEN size ELSE 0 END) AS up_buy,
                  SUM(CASE WHEN side='BUY' AND outcome_index=1 THEN size ELSE 0 END) AS down_buy,
                  SUM(CASE WHEN side='SELL' AND outcome_index=0 THEN size ELSE 0 END) AS up_sell,
                  SUM(CASE WHEN side='SELL' AND outcome_index=1 THEN size ELSE 0 END) AS down_sell,
                  SUM(CASE WHEN side='BUY' AND outcome_index=0 THEN price*size ELSE 0 END) AS up_buy_cost,
                  SUM(CASE WHEN side='BUY' AND outcome_index=1 THEN price*size ELSE 0 END) AS down_buy_cost,
                  SUM(CASE WHEN side='SELL' AND outcome_index=0 THEN price*size ELSE 0 END) AS up_sell_proceeds,
                  SUM(CASE WHEN side='SELL' AND outcome_index=1 THEN price*size ELSE 0 END) AS down_sell_proceeds,
                  COUNT(*) AS n_fills,
                  SUM(CASE WHEN outcome_index=0 THEN 1 ELSE 0 END) AS up_fills,
                  SUM(CASE WHEN outcome_index=1 THEN 1 ELSE 0 END) AS down_fills,
                  MIN(ts) AS first_ts,
                  MAX(ts) AS last_ts
           FROM trades
           WHERE address=? AND condition_id IS NOT NULL AND condition_id != ''
           GROUP BY condition_id""",
        (address,),
    ).fetchall()
    payload = []
    for row in rows:
        up_buy = _num(row["up_buy"])
        down_buy = _num(row["down_buy"])
        up_sell = _num(row["up_sell"])
        down_sell = _num(row["down_sell"])
        up_net = max(0.0, up_buy - up_sell)
        down_net = max(0.0, down_buy - down_sell)
        up_cost = _num(row["up_buy_cost"]) - _num(row["up_sell_proceeds"])
        down_cost = _num(row["down_buy_cost"]) - _num(row["down_sell_proceeds"])
        up_avg = (_num(row["up_buy_cost"]) / up_buy) if up_buy > 0 else None
        down_avg = (_num(row["down_buy_cost"]) / down_buy) if down_buy > 0 else None
        matched = min(up_net, down_net)
        pair_cost = (up_avg + down_avg) if up_avg is not None and down_avg is not None else None
        locked = matched * max(0.0, 1.0 - pair_cost) if pair_cost is not None else 0.0
        residual = abs(up_net - down_net)
        residual_side = "UP" if up_net > down_net else ("DOWN" if down_net > up_net else None)
        resolved = (row["resolved"] or "").upper() or None
        payout = None
        winning = None
        settlement_pnl = None
        if resolved in ("UP", "DOWN"):
            winning = up_net if resolved == "UP" else down_net
            payout = winning
            settlement_pnl = payout - (up_cost + down_cost)
        payload.append(
            (
                address,
                row["condition_id"],
                row["slug"],
                row["title"],
                row["market_start"],
                row["market_end"],
                resolved,
                up_net,
                down_net,
                up_cost,
                down_cost,
                up_avg,
                down_avg,
                matched,
                pair_cost,
                locked,
                residual,
                residual_side,
                winning,
                payout,
                up_cost + down_cost,
                settlement_pnl,
                row["n_fills"],
                row["up_fills"],
                row["down_fills"],
                row["first_ts"],
                row["last_ts"],
                now_ts(),
            )
        )
    conn.executemany(
        """INSERT OR REPLACE INTO pair_markets(
              address, condition_id, slug, title, market_start, market_end, resolved,
              up_shares, down_shares, up_cost, down_cost, up_avg, down_avg, matched_shares,
              pair_cost, locked_profit, residual_shares, residual_side, winning_shares,
              payout, total_cost, settlement_pnl, n_fills, up_fills, down_fills,
              first_ts, last_ts, updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        payload,
    )
    conn.commit()
    return len(payload)


def _price_series(conn: sqlite3.Connection, slug: str, outcome: str) -> tuple[list[int], list[float]]:
    column = "down_price" if str(outcome).upper() == "DOWN" else "up_price"
    rows = conn.execute(
        f"SELECT ts, {column} AS price FROM samples WHERE slug=? AND {column} IS NOT NULL ORDER BY ts",
        (slug,),
    ).fetchall()
    ts_list = [int(row["ts"]) for row in rows]
    prices = [float(row["price"]) for row in rows]
    return ts_list, prices


def _at_or_before(ts_list: list[int], prices: list[float], target: int, tolerance: int) -> float | None:
    index = bisect_right(ts_list, target) - 1
    if index >= 0 and target - ts_list[index] <= tolerance:
        return prices[index]
    return None


def _at_or_after(ts_list: list[int], prices: list[float], target: int, tolerance: int) -> float | None:
    index = bisect_left(ts_list, target)
    if index < len(ts_list) and ts_list[index] - target <= tolerance:
        return prices[index]
    return None


def _nearest(ts_list: list[int], prices: list[float], target: int, tolerance: int) -> float | None:
    if not ts_list:
        return None
    right = bisect_left(ts_list, target)
    candidates: list[tuple[int, float]] = []
    if right < len(ts_list) and ts_list[right] - target <= tolerance:
        candidates.append((ts_list[right] - target, prices[right]))
    if right > 0 and target - ts_list[right - 1] <= tolerance:
        candidates.append((target - ts_list[right - 1], prices[right - 1]))
    if not candidates:
        return None
    return min(candidates, key=lambda item: item[0])[1]


def enrich_slug(conn: sqlite3.Connection, address: str, slug: str) -> int:
    market = conn.execute("SELECT * FROM markets WHERE slug=?", (slug,)).fetchone()
    market_end = int(market["end_epoch"]) if market and market["end_epoch"] else None
    market_start = int(market["start_epoch"]) if market and market["start_epoch"] else None
    resolved = market["resolved"] if market else None
    trades = conn.execute(
        "SELECT trade_key, ts, outcome, side, price FROM trades WHERE address=? AND slug=?",
        (address, slug),
    ).fetchall()
    if not trades:
        return 0
    series = {
        "UP": _price_series(conn, slug, "UP"),
        "DOWN": _price_series(conn, slug, "DOWN"),
    }
    updates = []
    for trade in trades:
        outcome = str(trade["outcome"] or "Up").upper()
        ts_list, prices = series["DOWN"] if outcome.startswith("D") else series["UP"]
        mid_at_fill = _nearest(ts_list, prices, int(trade["ts"]), SAMPLE_TOLERANCE) if ts_list else None
        mid_1s = _at_or_after(ts_list, prices, int(trade["ts"]) + 1, SAMPLE_TOLERANCE) if ts_list else None
        mid_10s = _at_or_after(ts_list, prices, int(trade["ts"]) + 10, SAMPLE_TOLERANCE) if ts_list else None
        mid_60s = _at_or_after(ts_list, prices, int(trade["ts"]) + 60, SAMPLE_TOLERANCE) if ts_list else None
        mid_end = None
        if ts_list and market_end:
            mid_end = _at_or_before(ts_list, prices, market_end, END_TOLERANCE)
        if mid_end is None and ts_list:
            mid_end = prices[-1]
        sign = -1.0 if str(trade["side"]).upper() == "SELL" else 1.0
        price = float(trade["price"] or 0)
        entry_edge = (mid_at_fill - price) * sign if mid_at_fill is not None else None
        markout_1s = (mid_1s - mid_at_fill) * sign if mid_1s is not None and mid_at_fill is not None else None
        markout_10s = (mid_10s - mid_at_fill) * sign if mid_10s is not None and mid_at_fill is not None else None
        markout_60s = (mid_60s - mid_at_fill) * sign if mid_60s is not None and mid_at_fill is not None else None
        markout_end = (mid_end - mid_at_fill) * sign if mid_end is not None and mid_at_fill is not None else None
        elapsed = (int(trade["ts"]) - market_start) if market_start else None
        window_label = _window_label(elapsed)
        price_bucket = _price_bucket(price)
        updates.append(
            (
                mid_at_fill,
                mid_1s,
                mid_10s,
                mid_60s,
                mid_end,
                entry_edge,
                markout_1s,
                markout_10s,
                markout_60s,
                markout_end,
                window_label,
                price_bucket,
                elapsed,
                resolved,
                now_ts(),
                address,
                trade["trade_key"],
            )
        )
    conn.executemany(
        """UPDATE trades SET
             mid_at_fill=?, mid_1s=?, mid_10s=?, mid_60s=?, mid_end=?,
             entry_edge=?, markout_1s=?, markout_10s=?, markout_60s=?, markout_end=?,
             window_label=?, price_bucket=?, elapsed=?, resolved=?,
             enriched_at=?
           WHERE address=? AND trade_key=?""",
        updates,
    )
    conn.commit()
    return len(updates)


def _window_label(elapsed: int | None) -> str | None:
    if elapsed is None:
        return None
    if elapsed < 0:
        return "pre-open"
    if elapsed < 30:
        return "0-30s"
    if elapsed < 60:
        return "30-60s"
    if elapsed < 120:
        return "1-2m"
    if elapsed < 180:
        return "2-3m"
    if elapsed < 240:
        return "3-4m"
    if elapsed < 300:
        return "4-5m"
    return "5m+"


def _price_bucket(price: float | None) -> str | None:
    if price is None:
        return None
    edges = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
    for edge in edges:
        if price < edge:
            return f"<{edge:.2f}"
    return ">=0.90"


def ingest_wallet(
    conn: sqlite3.Connection,
    address: str,
    days: int | None = None,
    max_trades: int | None = None,
    enrich: bool = True,
    enrich_max_trades: int | None = None,
    enrich_days: int | None = None,
    progress: ProgressFn | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    address = address.lower()
    now = now_ts()
    days = settings.default_window_days if days is None else days
    start = 1 if days <= 0 else now - days * 86400
    end = now
    max_trades = settings.ingest_max_trades if max_trades is None or max_trades <= 0 else max_trades
    enrich_max_trades = settings.enrich_max_trades if enrich_max_trades is None else enrich_max_trades
    enrich_days = settings.enrich_days if enrich_days is None else enrich_days
    start, end = _window_meta(conn, address, days, start, end)
    if progress:
        progress(0, 100, f"window {datetime.fromtimestamp(start, timezone.utc):%Y-%m-%d} → now")

    client = PolymarketClient()
    summary: dict[str, Any] = {"address": address, "start": start, "end": end, "days": days}
    try:
        if progress:
            progress(2, 100, "fetching profile aggregates")
        stats = client.user_stats(address)
        volume = client.user_volume(address)
        pnl = client.user_pnl(address, interval="max", fidelity="1h")
        upsert_stats(conn, address, stats, volume, pnl)

        if progress:
            progress(6, 100, "fetching taker fills")
        taker_rows = _paged_resumable(
            conn,
            client,
            "trades",
            {"user": address, "limit": 1000, "start": start, "end": end, "taker_only": "true"},
            address,
            "trades_taker",
            days,
            max_trades,
            lambda rows: upsert_trades(conn, address, rows, 1),
            progress,
            6,
            4,
        )
        summary["taker_fills"] = taker_rows

        if progress:
            progress(10, 100, "fetching all fills (maker + taker)")
        all_rows = _paged_resumable(
            conn,
            client,
            "trades",
            {"user": address, "limit": 1000, "start": start, "end": end, "taker_only": "false"},
            address,
            "trades_all",
            days,
            max_trades,
            lambda rows: upsert_trades(conn, address, rows, 0),
            progress,
            10,
            30,
        )
        summary["fills"] = all_rows

        if progress:
            progress(42, 100, "fetching activity feed")
        activity_rows = _paged_resumable(
            conn,
            client,
            "activity",
            {
                "user": address,
                "limit": 1000,
                "start": start,
                "end": end,
                "type": ACTIVITY_TYPES,
                "exclude_deposits_withdrawals": "false",
            },
            address,
            "activity",
            days,
            max_trades,
            lambda rows: upsert_activity(conn, address, rows),
            progress,
            42,
            8,
        )
        summary["activity"] = activity_rows

        if progress:
            progress(50, 100, "fetching open positions")
        for status in ("OPEN", "REDEEMABLE"):
            try:
                rows = client.positions(address, status=status)
                upsert_positions(conn, address, rows)
            except Exception:
                pass

        if progress:
            progress(52, 100, "resolving markets")
        summary["resolved_conditions"] = update_resolutions(conn, address)
    finally:
        client.close()

    if enrich and enrich_max_trades > 0:
        cutoff = now - enrich_days * 86400
        slugs = conn.execute(
            """SELECT slug, MAX(ts) AS last_ts, COUNT(*) AS n
               FROM trades
               WHERE address=? AND ts>=? AND slug IS NOT NULL AND slug!=''
               GROUP BY slug
               ORDER BY last_ts DESC""",
            (address, cutoff),
        ).fetchall()
        selected: list[str] = []
        budget = enrich_max_trades
        for row in slugs:
            if budget <= 0:
                break
            selected.append(row["slug"])
            budget -= int(row["n"])
        total = len(selected)
        news_client = FutureNewsClient()
        try:
            for index, slug in enumerate(selected, start=1):
                try:
                    ensure_samples(conn, slug, news_client=news_client)
                    enrich_slug(conn, address, slug)
                except Exception as exc:
                    if progress:
                        progress(52 + int(index / max(1, total) * 30), 100, f"enrich {slug}: {exc}")
                if progress and index % 10 == 0:
                    progress(52 + int(index / max(1, total) * 30), 100, f"enriched {index}/{total} markets")
        finally:
            news_client.close()
        summary["enriched_markets"] = total

    if progress:
        progress(84, 100, "computing pair economics")
    summary["pairs"] = compute_pair_markets(conn, address)

    if progress:
        progress(90, 100, "building strategy profile")
    from .profiles import generate_profile

    profile_days = days if days and days > 0 else 30
    summary["profile_days"] = profile_days
    summary["profile"] = generate_profile(conn, address, days=profile_days)

    _clear_checkpoints(conn, address, days)
    if progress:
        progress(100, 100, "done")
    summary["completed_at"] = now_ts()
    return summary


def _num(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
