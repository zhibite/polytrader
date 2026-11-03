from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from ..db import connect, from_json, now_ts, to_json
from .metrics import _mean, _median, window_start

WINDOW_ORDER = ["pre-open", "0-30s", "30-60s", "1-2m", "2-3m", "3-4m", "4-5m", "5m+"]


def _ensure_run(run_id: str, strategy: str, params: dict[str, Any]) -> None:
    conn = connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO replica_runs(id, strategy, status, params_json, created_at)
               VALUES(?,?, 'RUNNING', ?, ?)""",
            (run_id, strategy, to_json(params), now_ts()),
        )
        conn.commit()
    finally:
        conn.close()


def _finish_run(
    run_id: str,
    status: str,
    summary: dict[str, Any] | None = None,
    equity: list[dict[str, Any]] | None = None,
    error: str | None = None,
) -> None:
    conn = connect()
    try:
        conn.execute(
            """UPDATE replica_runs SET status=?, summary_json=?, equity_json=?, error=?,
                                       finished_at=? WHERE id=?""",
            (status, to_json(summary or {}), to_json(equity or []), error, now_ts(), run_id),
        )
        conn.commit()
    finally:
        conn.close()


def _window_label(elapsed: int) -> str:
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


def _markets_in_range(conn: sqlite3.Connection, start: int, end: int, asset: str, period: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT m.*, COUNT(s.ts) AS n_samples
           FROM markets m JOIN samples s ON s.slug=m.slug
           WHERE m.start_epoch>=? AND m.start_epoch<=? AND m.asset=? AND m.period=?
           GROUP BY m.slug HAVING n_samples>0
           ORDER BY m.start_epoch ASC""",
        (start, end, asset, period),
    ).fetchall()
    return [dict(row) for row in rows]


def _series(conn: sqlite3.Connection, slug: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT ts, up_price, down_price FROM samples WHERE slug=? ORDER BY ts ASC",
        (slug,),
    ).fetchall()
    return [dict(row) for row in rows]


def _resolved_value(resolved: str | None, outcome: str) -> float:
    resolved = (resolved or "").upper()
    outcome = outcome.upper()
    if resolved == "UP" and outcome == "UP":
        return 1.0
    if resolved == "DOWN" and outcome == "DOWN":
        return 1.0
    return 0.0


def _simulate_mid_maker(
    conn: sqlite3.Connection,
    market: dict[str, Any],
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    samples = _series(conn, market["slug"])
    if not samples:
        return []
    start = int(market["start_epoch"] or samples[0]["ts"])
    end = int(market["end_epoch"] or samples[-1]["ts"])
    offset = float(params.get("offset", 0.005))
    size = float(params.get("size", 10))
    min_price = float(params.get("min_price", 0.30))
    max_price = float(params.get("max_price", 0.90))
    max_cost = float(params.get("max_cost_per_market", 50))
    allowed = set(params.get("allowed_windows") or WINDOW_ORDER)
    both_sides = bool(params.get("both_sides", True))
    fee_bps = float(params.get("fee_bps", 0))
    rebate_bps = float(params.get("rebate_bps", 0))
    resolved = market.get("resolved")
    trades: list[dict[str, Any]] = []
    sides = ["UP", "DOWN"] if both_sides else ["UP"]
    for side in sides:
        price_key = "up_price" if side == "UP" else "down_price"
        for index in range(len(samples) - 1):
            sample = samples[index]
            price = sample.get(price_key)
            nxt = samples[index + 1].get(price_key)
            if price is None or nxt is None:
                continue
            elapsed = int(sample["ts"]) - start
            label = _window_label(elapsed)
            if label not in allowed:
                continue
            if not (min_price <= price <= max_price):
                continue
            quote = price - offset
            if quote <= 0:
                continue
            # Maker fill model: the quote is resting at `quote`; the next
            # 1-second sample trades through it. No look-ahead beyond t+1.
            if nxt > quote:
                continue
            carried = min(size, max_cost / quote)
            if carried <= 0:
                continue
            cost = carried * quote
            fee = cost * fee_bps / 10000.0
            rebate = cost * rebate_bps / 10000.0
            payout = carried * _resolved_value(resolved, side)
            if resolved not in ("UP", "DOWN"):
                payout = carried * nxt
            pnl = payout - cost - fee + rebate
            trades.append(
                {
                    "slug": market["slug"],
                    "condition_id": market.get("condition_id"),
                    "ts": int(samples[index + 1]["ts"]),
                    "outcome": side,
                    "price": quote,
                    "size": carried,
                    "mid_at_fill": price,
                    "resolved": resolved,
                    "payout": payout,
                    "pnl": pnl,
                    "elapsed": int(samples[index + 1]["ts"]) - start,
                }
            )
            break
    return trades


def _simulate_pair_lock(
    conn: sqlite3.Connection,
    market: dict[str, Any],
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    samples = _series(conn, market["slug"])
    if not samples:
        return []
    start = int(market["start_epoch"] or samples[0]["ts"])
    size = float(params.get("size", 10))
    min_edge = float(params.get("min_edge", 0.01))
    min_price = float(params.get("min_price", 0.05))
    max_price = float(params.get("max_price", 0.95))
    max_cost = float(params.get("max_cost_per_market", 500))
    fee_bps = float(params.get("fee_bps", 0))
    rebate_bps = float(params.get("rebate_bps", 0))
    resolved = market.get("resolved")
    for sample in samples:
        up = sample.get("up_price")
        down = sample.get("down_price")
        if up is None or down is None:
            continue
        if not (min_price <= up <= max_price and min_price <= down <= max_price):
            continue
        pair_cost = up + down
        fee = pair_cost * fee_bps / 10000.0
        if pair_cost + fee > 1.0 - min_edge:
            continue
        shares = min(size, max_cost / max(pair_cost, 1e-9))
        if shares <= 0:
            continue
        cost = shares * pair_cost
        fees = cost * fee_bps / 10000.0
        rebate = cost * rebate_bps / 10000.0
        payout = shares if resolved in ("UP", "DOWN") else shares * max(up, down)
        pnl = payout - cost - fees + rebate
        return [
            {
                "slug": market["slug"],
                "condition_id": market.get("condition_id"),
                "ts": int(sample["ts"]),
                "outcome": "PAIR",
                "price": pair_cost,
                "size": shares,
                "mid_at_fill": pair_cost,
                "resolved": resolved,
                "payout": payout,
                "pnl": pnl,
                "elapsed": int(sample["ts"]) - start,
            }
        ]
    return []


def run_replica(
    strategy: str = "mid_maker",
    days: int = 3,
    params: dict[str, Any] | None = None,
    run_id: str | None = None,
    progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    params = dict(params or {})
    params.setdefault("strategy", strategy)
    run_id = run_id or f"replica_{now_ts()}"
    _ensure_run(run_id, strategy, params)
    try:
        conn = connect()
        try:
            end = now_ts()
            start = window_start(days, end)
            asset = params.get("asset", "btc")
            period = params.get("period", "5m")
            markets = _markets_in_range(conn, start, end, asset, period)
            if not markets:
                summary = {
                    "empty": True,
                    "message": "没有可用样本。请先运行目标钱包的 ingest 以载入 klines，或挂载 LAB_KLINES_DATASET_DIR。",
                }
                _finish_run(run_id, "DONE", summary, [])
                return {"summary": summary}
            conn.execute("DELETE FROM replica_trades WHERE run_id=?", (run_id,))
            conn.commit()
            all_trades: list[dict[str, Any]] = []
            for index, market in enumerate(markets, start=1):
                if strategy == "pair_lock":
                    all_trades.extend(_simulate_pair_lock(conn, market, params))
                else:
                    all_trades.extend(_simulate_mid_maker(conn, market, params))
                if progress and index % 25 == 0:
                    progress(index, len(markets), f"simulated {index}/{len(markets)} markets")
            conn.executemany(
                """INSERT INTO replica_trades(
                     run_id, slug, condition_id, ts, outcome, price, size, mid_at_fill,
                     resolved, payout, pnl, elapsed)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (
                        run_id,
                        trade["slug"],
                        trade["condition_id"],
                        trade["ts"],
                        trade["outcome"],
                        trade["price"],
                        trade["size"],
                        trade["mid_at_fill"],
                        trade["resolved"],
                        trade["payout"],
                        trade["pnl"],
                        trade["elapsed"],
                    )
                    for trade in all_trades
                ],
            )
            conn.commit()
        finally:
            conn.close()
        pnls = [trade["pnl"] for trade in all_trades]
        by_day: dict[str, dict[str, float]] = defaultdict(lambda: {"pnl": 0.0, "n": 0, "cost": 0.0})
        for trade in all_trades:
            day = datetime.fromtimestamp(int(trade["ts"]), timezone.utc).strftime("%Y-%m-%d")
            by_day[day]["pnl"] += trade["pnl"]
            by_day[day]["n"] += 1
            by_day[day]["cost"] += trade["price"] * trade["size"]
        equity = []
        cumulative = 0.0
        for day in sorted(by_day):
            cumulative += by_day[day]["pnl"]
            equity.append({"day": day, "pnl": cumulative, "daily": by_day[day]["pnl"], "n": by_day[day]["n"]})
        total_cost = sum(trade["price"] * trade["size"] for trade in all_trades)
        summary = {
            "empty": not all_trades,
            "strategy": strategy,
            "markets": len(markets),
            "trades": len(all_trades),
            "cost": total_cost,
            "pnl": sum(pnls),
            "roi": (sum(pnls) / total_cost) if total_cost else None,
            "win_rate": (sum(1 for p in pnls if p > 0) / len(pnls)) if pnls else None,
            "avg_pnl": _mean(pnls),
            "median_pnl": _median(pnls),
            "params": params,
            "by_day": [{"day": day, **values} for day, values in sorted(by_day.items())],
        }
        _finish_run(run_id, "DONE", summary, equity)
        if progress:
            progress(len(markets), len(markets), "replica complete")
        return {"summary": summary, "equity": equity}
    except Exception as exc:
        _finish_run(run_id, "ERROR", error=str(exc)[:1000])
        raise


def list_runs(conn: sqlite3.Connection, limit: int = 30) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM replica_runs ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["summary"] = from_json(item.get("summary_json"), {}) or {}
        item["params"] = from_json(item.get("params_json"), {}) or {}
        item["equity"] = from_json(item.get("equity_json"), []) or []
        out.append(item)
    return out


def get_run(conn: sqlite3.Connection, run_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM replica_runs WHERE id=?", (run_id,)).fetchone()
    if not row:
        return None
    item = dict(row)
    item["summary"] = from_json(item.get("summary_json"), {}) or {}
    item["params"] = from_json(item.get("params_json"), {}) or {}
    item["equity"] = from_json(item.get("equity_json"), []) or []
    trades = conn.execute(
        "SELECT * FROM replica_trades WHERE run_id=? ORDER BY ts DESC LIMIT 500", (run_id,)
    ).fetchall()
    item["trades"] = [dict(trade) for trade in trades]
    return item
