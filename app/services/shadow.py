from __future__ import annotations

import sqlite3
from bisect import bisect_left
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from ..db import connect, from_json, now_ts, to_json
from .metrics import _mean, _median, window_start


def _mid_at(
    conn: sqlite3.Connection,
    cache: dict[tuple[str, str], tuple[list[int], list[float]]],
    slug: str,
    outcome: str,
    ts: int,
    tolerance: int = 10,
) -> float | None:
    key = (slug, "DOWN" if str(outcome).upper().startswith("D") else "UP")
    if key not in cache:
        column = "down_price" if key[1] == "DOWN" else "up_price"
        rows = conn.execute(
            f"SELECT ts, {column} AS p FROM samples WHERE slug=? AND {column} IS NOT NULL ORDER BY ts",
            (slug,),
        ).fetchall()
        cache[key] = ([int(row["ts"]) for row in rows], [float(row["p"]) for row in rows])
    ts_list, prices = cache[key]
    if not ts_list:
        return None
    index = bisect_left(ts_list, ts)
    candidates = []
    if index < len(ts_list) and ts_list[index] - ts <= tolerance:
        candidates.append((ts_list[index] - ts, prices[index]))
    if index > 0 and ts - ts_list[index - 1] <= tolerance:
        candidates.append((ts - ts_list[index - 1], prices[index - 1]))
    if not candidates:
        return None
    return min(candidates, key=lambda item: item[0])[1]


def _ensure_run(run_id: str, address: str, params: dict[str, Any]) -> None:
    conn = connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO shadow_runs(id, address, status, params_json, created_at)
               VALUES(?,?, 'RUNNING', ?, ?)""",
            (run_id, address, to_json(params), now_ts()),
        )
        conn.commit()
    finally:
        conn.close()


def _finish_run(run_id: str, status: str, summary: dict[str, Any] | None = None, error: str | None = None) -> None:
    conn = connect()
    try:
        conn.execute(
            "UPDATE shadow_runs SET status=?, summary_json=?, error=?, finished_at=? WHERE id=?",
            (status, to_json(summary or {}), error, now_ts(), run_id),
        )
        conn.commit()
    finally:
        conn.close()


def run_shadow(
    address: str,
    days: int = 7,
    delay_s: float = 0.0,
    size_scale: float = 1.0,
    copy_sells: bool = False,
    run_id: str | None = None,
    progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    address = address.lower()
    run_id = run_id or f"shadow_{now_ts()}"
    params = {
        "days": days,
        "delay_s": delay_s,
        "size_scale": size_scale,
        "copy_sells": copy_sells,
    }
    _ensure_run(run_id, address, params)
    try:
        conn = connect()
        try:
            start = window_start(days)
            rows = conn.execute(
                """SELECT trade_key, ts, slug, condition_id, outcome, side, price, size,
                          resolved, mid_end, entry_edge, markout_end, window_label, price_bucket
                   FROM trades WHERE address=? AND ts>=?
                   ORDER BY ts ASC""",
                (address, start),
            ).fetchall()
            if not rows:
                summary = {"empty": True, "copied": 0}
                _finish_run(run_id, "DONE", summary)
                return {"summary": summary}
            conn.execute("DELETE FROM shadow_trades WHERE run_id=?", (run_id,))
            # End the write transaction before the long copy loop; progress
            # callbacks open their own connections and would otherwise wait on
            # this connection's write lock for busy_timeout seconds.
            conn.commit()
            cache: dict[tuple[str, str], tuple[list[int], list[float]]] = {}
            trades = []
            total_cost = 0.0
            total_pnl = 0.0
            target_pnl = 0.0
            wins = 0
            skipped = 0
            by_day: dict[str, dict[str, float]] = defaultdict(lambda: {"pnl": 0.0, "n": 0})
            by_bucket: dict[str, dict[str, float]] = defaultdict(lambda: {"pnl": 0.0, "n": 0})
            by_window: dict[str, dict[str, float]] = defaultdict(lambda: {"pnl": 0.0, "n": 0})
            edges = []
            markouts = []
            for index, row in enumerate(rows, start=1):
                side = str(row["side"]).upper()
                if side == "SELL" and not copy_sells:
                    skipped += 1
                    continue
                target_price = float(row["price"] or 0)
                copy_price = target_price
                if delay_s > 0:
                    delayed = _mid_at(conn, cache, row["slug"], row["outcome"], int(row["ts"]) + int(delay_s))
                    if delayed is None:
                        copy_price = target_price
                    else:
                        copy_price = delayed
                size = float(row["size"] or 0) * size_scale
                if size <= 0:
                    continue
                resolved = (row["resolved"] or "").upper()
                outcome = str(row["outcome"] or "").upper()
                mid_end = row["mid_end"]
                if side == "BUY":
                    if resolved in ("UP", "DOWN"):
                        payout = size if outcome.startswith(resolved[0]) else 0.0
                        pnl = payout - copy_price * size
                        target_value = (size if outcome.startswith(resolved[0]) else 0.0) - target_price * size
                    else:
                        mark = float(mid_end) if mid_end is not None else copy_price
                        pnl = (mark - copy_price) * size
                        target_value = (mark - target_price) * size
                    cost = copy_price * size
                    total_cost += cost
                else:
                    # Short / exit copy: proceeds now, liability at settlement.
                    if resolved in ("UP", "DOWN"):
                        liability = size if outcome.startswith(resolved[0]) else 0.0
                        pnl = copy_price * size - liability
                        target_value = target_price * size - liability
                    else:
                        mark = float(mid_end) if mid_end is not None else copy_price
                        pnl = (copy_price - mark) * size
                        target_value = (target_price - mark) * size
                    cost = 0.0
                total_pnl += pnl
                target_pnl += target_value
                if pnl > 0:
                    wins += 1
                day = datetime.fromtimestamp(int(row["ts"]), timezone.utc).strftime("%Y-%m-%d")
                by_day[day]["pnl"] += pnl
                by_day[day]["n"] += 1
                bucket = row["price_bucket"] or "unknown"
                by_bucket[bucket]["pnl"] += pnl
                by_bucket[bucket]["n"] += 1
                window = row["window_label"] or "unknown"
                by_window[window]["pnl"] += pnl
                by_window[window]["n"] += 1
                if row["entry_edge"] is not None:
                    edges.append(float(row["entry_edge"]))
                if row["markout_end"] is not None:
                    markouts.append(float(row["markout_end"]))
                trades.append(
                    (
                        run_id,
                        row["trade_key"],
                        int(row["ts"]),
                        row["slug"],
                        row["condition_id"],
                        row["outcome"],
                        side,
                        target_price,
                        copy_price,
                        size,
                        float(delay_s),
                        resolved or None,
                        pnl + copy_price * size if side == "BUY" else None,
                        pnl,
                        mid_end,
                        row["entry_edge"],
                        row["markout_end"],
                    )
                )
                if progress and index % 500 == 0:
                    progress(index, len(rows), f"copied {len(trades)} fills")
            conn.executemany(
                """INSERT OR REPLACE INTO shadow_trades(
                     run_id, trade_key, ts, slug, condition_id, outcome, side, target_price,
                     copy_price, size, delay_s, resolved, payout, pnl, mid_end, entry_edge,
                     markout_end)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                trades,
            )
            conn.commit()
        finally:
            conn.close()
        copied = len(trades)
        summary = {
            "empty": copied == 0,
            "copied": copied,
            "skipped_sells": skipped,
            "size_scale": size_scale,
            "delay_s": delay_s,
            "cost": total_cost,
            "pnl": total_pnl,
            "target_pnl": target_pnl,
            "roi": (total_pnl / total_cost) if total_cost else None,
            "win_rate": (wins / copied) if copied else None,
            "avg_entry_edge": _mean(edges),
            "avg_markout_end": _mean(markouts),
            "median_markout_end": _median(markouts),
            "by_day": [{"day": day, **values} for day, values in sorted(by_day.items())],
            "by_bucket": [{"bucket": bucket, **values} for bucket, values in sorted(by_bucket.items())],
            "by_window": [{"window": window, **values} for window, values in sorted(by_window.items())],
            "params": params,
        }
        _finish_run(run_id, "DONE", summary)
        if progress:
            progress(len(rows), len(rows), "shadow complete")
        return {"summary": summary}
    except Exception as exc:
        _finish_run(run_id, "ERROR", error=str(exc)[:1000])
        raise


def list_runs(conn: sqlite3.Connection, limit: int = 30) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM shadow_runs ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["summary"] = from_json(item.get("summary_json"), {}) or {}
        item["params"] = from_json(item.get("params_json"), {}) or {}
        out.append(item)
    return out


def get_run(conn: sqlite3.Connection, run_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM shadow_runs WHERE id=?", (run_id,)).fetchone()
    if not row:
        return None
    item = dict(row)
    item["summary"] = from_json(item.get("summary_json"), {}) or {}
    item["params"] = from_json(item.get("params_json"), {}) or {}
    trades = conn.execute(
        "SELECT * FROM shadow_trades WHERE run_id=? ORDER BY ts DESC LIMIT 500", (run_id,)
    ).fetchall()
    item["trades"] = [dict(trade) for trade in trades]
    return item
