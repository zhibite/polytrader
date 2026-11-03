from __future__ import annotations

import sqlite3
from typing import Any, Callable

from ..db import connect, from_json, now_ts, to_json
from .polymarket import ApiError, PolymarketClient

DEFAULT_PERIODS = ["day", "week", "month", "all"]
DEFAULT_CATEGORIES = ["overall", "crypto"]
DEFAULT_SORT = ["PNL", "VOLUME"]


def sync_leaderboards(
    time_periods: list[str] | None = None,
    categories: list[str] | None = None,
    sort_by: str | list[str] | None = None,
    limit: int = 100,
    progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    periods = time_periods or DEFAULT_PERIODS
    cats = categories or DEFAULT_CATEGORIES
    sorts = sort_by if isinstance(sort_by, list) else ([sort_by] if sort_by else DEFAULT_SORT)
    combos = [(period, category, sort) for period in periods for category in cats for sort in sorts]
    captured_at = now_ts()
    total_rows = 0
    errors: list[str] = []
    client = PolymarketClient()
    try:
        for index, (period, category, sort) in enumerate(combos, start=1):
            if progress:
                progress(index - 1, len(combos), f"{period}/{category}/{sort}")
            try:
                payload = client.leaderboard(
                    time_period=period,
                    category=category,
                    sort_by=sort,
                    limit=limit,
                )
            except ApiError as exc:
                errors.append(f"{period}/{category}/{sort}: {exc}")
                continue
            rows = payload.get("data") or []
            _store_rows(captured_at, period, category, sort, rows)
            total_rows += len(rows)
        if progress:
            progress(len(combos), len(combos), "leaderboards complete")
    finally:
        client.close()
    return {"rows": total_rows, "captured_at": captured_at, "errors": errors[:5]}


def _store_rows(
    captured_at: int,
    period: str,
    category: str,
    sort_by: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    conn = connect()
    try:
        payload = []
        for row in rows:
            address = (row.get("user_id") or row.get("proxy_wallet") or row.get("address") or "").lower()
            if not address:
                continue
            payload.append(
                (
                    captured_at,
                    period,
                    category,
                    sort_by,
                    row.get("rank"),
                    address,
                    row.get("pnl"),
                    row.get("volume"),
                    row.get("user_name") or row.get("name"),
                    row.get("profile_image"),
                    row.get("x_username"),
                    1 if row.get("verified") else 0,
                    to_json(row),
                )
            )
        conn.executemany(
            """INSERT INTO leaderboard_snapshots(
                 captured_at, time_period, category, sort_by, rank, address, pnl, volume,
                 user_name, profile_image, x_username, verified, raw_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            payload,
        )
        conn.commit()
    finally:
        conn.close()


def latest_capture(conn: sqlite3.Connection, period: str, category: str, sort_by: str) -> int | None:
    row = conn.execute(
        """SELECT MAX(captured_at) AS ts FROM leaderboard_snapshots
           WHERE time_period=? AND category=? AND sort_by=?""",
        (period, category, sort_by),
    ).fetchone()
    return int(row["ts"]) if row and row["ts"] else None


def leaderboard_rows(
    conn: sqlite3.Connection,
    period: str = "day",
    category: str = "overall",
    sort_by: str = "PNL",
    limit: int = 100,
) -> tuple[int | None, list[dict[str, Any]]]:
    captured = latest_capture(conn, period, category, sort_by)
    if not captured:
        return None, []
    rows = conn.execute(
        """SELECT * FROM leaderboard_snapshots
           WHERE captured_at=? AND time_period=? AND category=? AND sort_by=?
           ORDER BY rank ASC LIMIT ?""",
        (captured, period, category, sort_by, limit),
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["raw"] = from_json(item.get("raw_json"), {}) or {}
        out.append(item)
    return captured, out


def available_combos(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT time_period, category, sort_by, MAX(captured_at) AS captured_at,
                  COUNT(*) AS n
           FROM leaderboard_snapshots
           GROUP BY time_period, category, sort_by
           ORDER BY time_period, category, sort_by"""
    ).fetchall()
    return [dict(row) for row in rows]
