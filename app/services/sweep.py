from __future__ import annotations

import sqlite3
import secrets
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from ..db import connect, from_json, now_ts, to_json
from .metrics import window_start

WINDOW_ORDER = ["0-30s", "30-60s", "1-2m", "2-3m", "3-4m", "4-5m"]
WINDOW_SETS: dict[str, list[str]] = {
    "all": WINDOW_ORDER,
    "target": ["0-30s", "1-2m", "2-3m", "4-5m"],
    "early": ["0-30s", "30-60s"],
    "mid": ["1-2m", "2-3m"],
    "late": ["3-4m", "4-5m"],
}
DEFAULT_OFFSETS = [0.002, 0.003, 0.005, 0.008]
DEFAULT_BANDS = [(0.30, 0.90), (0.50, 0.90)]
DEFAULT_SIZES = [5.0, 10.0, 20.0]
DEFAULT_REBATES = [0.0, 45.0]

ProgressFn = Callable[[int, int, str], None]


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


def _day(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%d")


def default_params() -> dict[str, Any]:
    return {
        "days": 7,
        "asset": "btc",
        "period": "5m",
        "offsets": DEFAULT_OFFSETS,
        "window_sets": list(WINDOW_SETS),
        "bands": DEFAULT_BANDS,
        "sizes": DEFAULT_SIZES,
        "rebates": DEFAULT_REBATES,
        "fee_bps": 0.0,
        "max_cost_per_market": 25.0,
        "max_open_cost": 500.0,
        "min_val_fills": 10,
    }


def load_grid(params: dict[str, Any] | None = None) -> dict[str, Any]:
    base = default_params()
    if params:
        for key, value in params.items():
            if value is not None and value != "":
                base[key] = value
    base["offsets"] = [float(value) for value in base["offsets"] if float(value) > 0]
    base["sizes"] = [float(value) for value in base["sizes"] if float(value) > 0]
    base["rebates"] = [float(value) for value in base["rebates"]]
    base["bands"] = [(float(pair[0]), float(pair[1])) for pair in base["bands"]]
    base["window_sets"] = [name for name in base["window_sets"] if name in WINDOW_SETS]
    if not base["offsets"]:
        base["offsets"] = DEFAULT_OFFSETS
    if not base["window_sets"]:
        base["window_sets"] = list(WINDOW_SETS)
    if not base["sizes"]:
        base["sizes"] = DEFAULT_SIZES
    if not base["rebates"]:
        base["rebates"] = DEFAULT_REBATES
    if not base["bands"]:
        base["bands"] = DEFAULT_BANDS
    base["days"] = int(base["days"])
    base["fee_bps"] = float(base["fee_bps"])
    base["max_cost_per_market"] = float(base["max_cost_per_market"])
    base["max_open_cost"] = float(base["max_open_cost"])
    base["min_val_fills"] = int(base["min_val_fills"])
    return base


def _load_data(conn: sqlite3.Connection, params: dict[str, Any]) -> list[dict[str, Any]]:
    start = window_start(int(params["days"]))
    end = now_ts()
    market_rows = conn.execute(
        """SELECT m.slug, m.condition_id, m.title, m.start_epoch, m.end_epoch, m.resolved
           FROM markets m
           WHERE m.start_epoch>=? AND m.start_epoch<=? AND m.asset=? AND m.period=?
             AND EXISTS(SELECT 1 FROM samples s WHERE s.slug=m.slug)
           ORDER BY m.start_epoch ASC""",
        (start, end, params["asset"], params["period"]),
    ).fetchall()
    if not market_rows:
        return []
    markets: dict[str, dict[str, Any]] = {}
    for row in market_rows:
        markets[row["slug"]] = {
            "slug": row["slug"],
            "condition_id": row["condition_id"],
            "title": row["title"],
            "start": int(row["start_epoch"]),
            "end": int(row["end_epoch"] or (int(row["start_epoch"]) + 300)),
            "resolved": (row["resolved"] or "").upper() or None,
            "samples": [],
        }
    sample_rows = conn.execute(
        """SELECT s.slug, s.ts, s.up_price, s.down_price
           FROM samples s JOIN markets m ON m.slug=s.slug
           WHERE m.start_epoch>=? AND m.start_epoch<=? AND m.asset=? AND m.period=?
           ORDER BY s.slug, s.ts ASC""",
        (start, end, params["asset"], params["period"]),
    ).fetchall()
    for row in sample_rows:
        market = markets.get(row["slug"])
        if market is not None:
            market["samples"].append(
                (int(row["ts"]), row["up_price"], row["down_price"])
            )
    return [market for market in markets.values() if len(market["samples"]) > 1]


def _crossings(market: dict[str, Any], offsets: list[float]) -> dict[float, list[dict[str, Any]]]:
    """For each offset, record price-drop crossings for both outcome tokens.

    A quote is placed at sample i (mid - offset) and fills when the next
    sample trades at or below it. We keep every crossing; the grid evaluation
    later selects the earliest crossing that matches a window/price band.
    """
    result: dict[float, list[dict[str, Any]]] = {offset: [] for offset in offsets}
    for side, key in (("UP", 1), ("DOWN", 2)):
        series = [
            (int(item[0]), item[key])
            for item in market["samples"]
            if item[key] is not None
        ]
        if len(series) < 2:
            continue
        last_price = float(series[-1][1])
        for index in range(len(series) - 1):
            ts, price = series[index]
            next_ts, next_price = series[index + 1]
            if next_ts > market["end"]:
                continue
            if price is None or next_price is None:
                continue
            price = float(price)
            next_price = float(next_price)
            for offset in offsets:
                quote = price - offset
                if quote <= 0 or next_price > quote:
                    continue
                result[offset].append(
                    {
                        "slug": market["slug"],
                        "condition_id": market["condition_id"],
                        "outcome": side,
                        "ts": next_ts,
                        "market_end": market["end"],
                        "mid": price,
                        "quote": quote,
                        "window": _window_label(next_ts - market["start"]),
                        "resolved": market["resolved"],
                        "last_price": last_price,
                    }
                )
    return result


def _variant_fills(
    crossings: list[dict[str, Any]],
    allowed_windows: set[str],
    band: tuple[float, float],
) -> list[dict[str, Any]]:
    best: dict[tuple[str, str], dict[str, Any]] = {}
    low, high = band
    for fill in crossings:
        if fill["window"] not in allowed_windows:
            continue
        if not (low <= fill["mid"] <= high):
            continue
        key = (fill["slug"], fill["outcome"])
        previous = best.get(key)
        if previous is None or fill["ts"] < previous["ts"]:
            best[key] = fill
    return list(best.values())


def _metrics(
    fills: list[dict[str, Any]],
    size: float,
    rebate_bps: float,
    fee_bps: float,
    max_cost_per_market: float,
    max_open_cost: float,
    min_val_fills: int,
) -> dict[str, Any]:
    if not fills:
        return {
            "pnl": 0.0, "roi": None, "fills": 0, "win_rate": None, "cost": 0.0,
            "max_drawdown": None, "train_pnl": 0.0, "train_roi": None, "train_fills": 0,
            "train_win_rate": None, "val_pnl": 0.0, "val_roi": None, "val_fills": 0,
            "val_win_rate": None, "profit_factor": None, "train_profit_factor": None,
            "val_profit_factor": None, "score": -999.0, "skipped_portfolio": 0,
        }
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fill in fills:
        grouped[fill["slug"]].append(fill)

    positions: list[dict[str, Any]] = []
    for slug, group in grouped.items():
        raw = []
        for fill in group:
            shares = min(size, max_cost_per_market / fill["quote"])
            raw.append({**fill, "shares": shares, "cost": shares * fill["quote"]})
        total_cost = sum(item["cost"] for item in raw)
        scale = min(1.0, max_cost_per_market / total_cost) if total_cost > 0 else 1.0
        for item in raw:
            shares = item["shares"] * scale
            cost = item["cost"] * scale
            resolved = item["resolved"]
            if resolved in ("UP", "DOWN"):
                payout = shares if resolved == item["outcome"] else 0.0
            else:
                payout = shares * item["last_price"]
            fee = cost * fee_bps / 10000.0
            rebate = cost * rebate_bps / 10000.0
            pnl = payout - cost - fee + rebate
            positions.append(
                {
                    "ts": item["ts"],
                    "end": item["market_end"],
                    "cost": cost,
                    "pnl": pnl,
                    "day": _day(item["ts"]),
                }
            )

    positions.sort(key=lambda item: item["ts"])
    accepted: list[dict[str, Any]] = []
    open_positions: list[tuple[int, float]] = []
    skipped_portfolio = 0
    for position in positions:
        now = position["ts"]
        open_positions = [(end, cost) for end, cost in open_positions if end > now]
        open_cost = sum(cost for _, cost in open_positions)
        if open_cost + position["cost"] > max_open_cost + 1e-9:
            skipped_portfolio += 1
            continue
        accepted.append(position)
        open_positions.append((position["end"], position["cost"]))

    if not accepted:
        return _metrics([], size, rebate_bps, fee_bps, max_cost_per_market, max_open_cost, min_val_fills)

    total_cost = sum(item["cost"] for item in accepted)
    total_pnl = sum(item["pnl"] for item in accepted)
    wins = sum(1 for item in accepted if item["pnl"] > 0)
    daily: dict[str, float] = defaultdict(float)
    for item in accepted:
        daily[item["day"]] += item["pnl"]
    days = sorted(daily)
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for day in days:
        cumulative += daily[day]
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)

    if len(days) >= 2:
        split = max(1, min(len(days) - 1, int(len(days) * 0.7 + 0.5)))
        train_days = set(days[:split])
        val_days = set(days[split:])
    else:
        train_days = set(days)
        val_days = set()
    train = [item for item in accepted if item["day"] in train_days]
    val = [item for item in accepted if item["day"] in val_days]

    def profit_factor(items: list[dict[str, Any]]) -> float | None:
        gross_win = sum(item["pnl"] for item in items if item["pnl"] > 0)
        gross_loss = abs(sum(item["pnl"] for item in items if item["pnl"] < 0))
        return (gross_win / gross_loss) if gross_loss > 0 else None

    def aggregate(items: list[dict[str, Any]]) -> tuple[float, float | None, int, float | None, float | None]:
        if not items:
            return 0.0, None, 0, None, None
        cost = sum(item["cost"] for item in items)
        pnl = sum(item["pnl"] for item in items)
        win_rate = sum(1 for item in items if item["pnl"] > 0) / len(items)
        return pnl, (pnl / cost if cost else None), len(items), win_rate, profit_factor(items)

    train_pnl, train_roi, train_fills, train_win_rate, train_pf = aggregate(train)
    val_pnl, val_roi, val_fills, val_win_rate, val_pf = aggregate(val)
    score = val_roi if val_fills >= min_val_fills and val_roi is not None else -999.0
    return {
        "pnl": total_pnl,
        "roi": (total_pnl / total_cost) if total_cost else None,
        "fills": len(accepted),
        "win_rate": wins / len(accepted),
        "cost": total_cost,
        "max_drawdown": max_drawdown,
        "train_pnl": train_pnl,
        "train_roi": train_roi,
        "train_fills": train_fills,
        "train_win_rate": train_win_rate,
        "val_pnl": val_pnl,
        "val_roi": val_roi,
        "val_fills": val_fills,
        "val_win_rate": val_win_rate,
        "profit_factor": profit_factor(accepted),
        "train_profit_factor": train_pf,
        "val_profit_factor": val_pf,
        "score": score,
        "skipped_portfolio": skipped_portfolio,
    }


def run_sweep(
    run_id: str,
    params: dict[str, Any] | None = None,
    progress: ProgressFn | None = None,
) -> dict[str, Any]:
    params = load_grid(params)
    conn = connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO sweep_runs(id, status, params_json, created_at)
               VALUES(?, 'RUNNING', ?, ?)""",
            (run_id, to_json(params), now_ts()),
        )
        conn.commit()
    finally:
        conn.close()

    try:
        conn = connect()
        try:
            if progress:
                progress(3, 100, "loading markets and 1s samples")
            markets = _load_data(conn, params)
            if not markets:
                summary = {"empty": True, "message": "没有可用的 samples，请先运行 ingest 或挂载 kline 数据集"}
                _finish(run_id, summary, [])
                return {"summary": summary, "results": []}
            if progress:
                progress(10, 100, f"precomputing crossings for {len(markets)} markets")
            offset_list = params["offsets"]
            variants: list[dict[str, Any]] = []
            for offset in offset_list:
                crossing_by_market: list[dict[str, Any]] = []
                for market in markets:
                    crossing_by_market.extend(_crossings(market, [offset])[offset])
                for window_name in params["window_sets"]:
                    allowed = set(WINDOW_SETS[window_name])
                    for band in params["bands"]:
                        selected = _variant_fills(crossing_by_market, allowed, band)
                        variants.append(
                            {
                                "offset": offset,
                                "window_set": window_name,
                                "band": band,
                                "fills": selected,
                            }
                        )
            combos = len(offset_list) * len(params["window_sets"]) * len(params["bands"]) * len(params["sizes"]) * len(params["rebates"])
            if progress:
                progress(25, 100, f"evaluating {combos} parameter combinations")
            results = []
            done = 0
            for variant in variants:
                for size in params["sizes"]:
                    for rebate in params["rebates"]:
                        metrics = _metrics(
                            variant["fills"],
                            size=size,
                            rebate_bps=rebate,
                            fee_bps=params["fee_bps"],
                            max_cost_per_market=params["max_cost_per_market"],
                            max_open_cost=params["max_open_cost"],
                            min_val_fills=params["min_val_fills"],
                        )
                        combo_params = {
                            "family": "mid_maker",
                            "offset": variant["offset"],
                            "window_set": variant["window_set"],
                            "windows": WINDOW_SETS[variant["window_set"]],
                            "band": list(variant["band"]),
                            "size": size,
                            "rebate_bps": rebate,
                            "fee_bps": params["fee_bps"],
                            "max_cost_per_market": params["max_cost_per_market"],
                            "max_open_cost": params["max_open_cost"],
                        }
                        combo_id = (
                            f"{variant['offset']}|{variant['window_set']}|"
                            f"{variant['band'][0]:.2f}-{variant['band'][1]:.2f}|{size}|{rebate}"
                        )
                        results.append(
                            {
                                "combo_id": combo_id,
                                "params": combo_params,
                                **metrics,
                            }
                        )
                        done += 1
                        if progress and done % max(1, combos // 20) == 0:
                            progress(25 + int(done / max(1, combos) * 65), 100, f"{done}/{combos} combos")
            results.sort(key=lambda item: (item["score"], item["val_pnl"], item["pnl"]), reverse=True)
            for index, result in enumerate(results, start=1):
                result["rank"] = index
            _store_results(conn, run_id, results)
            best = results[0] if results else None
            summary = {
                "empty": not results,
                "markets": len(markets),
                "combos": len(results),
                "days": params["days"],
                "window_sets": params["window_sets"],
                "offsets": offset_list,
                "bands": [list(band) for band in params["bands"]],
                "sizes": params["sizes"],
                "rebates": params["rebates"],
                "max_cost_per_market": params["max_cost_per_market"],
                "max_open_cost": params["max_open_cost"],
                "best": best,
                "top": results[:12],
                "generated_at": now_ts(),
            }
            _finish(run_id, summary, results, conn=conn)
            if progress:
                progress(100, 100, "sweep complete")
            return {"summary": summary, "results": results}
        finally:
            conn.close()
    except Exception as exc:
        conn = connect()
        try:
            conn.execute(
                "UPDATE sweep_runs SET status='ERROR', error=?, finished_at=? WHERE id=?",
                (str(exc)[:1000], now_ts(), run_id),
            )
            conn.commit()
        finally:
            conn.close()
        raise


def refine_params(
    params: dict[str, Any],
    results: list[dict[str, Any]],
    top_k: int = 8,
) -> dict[str, Any]:
    """Narrow the grid around the best validation parameter sets.

    This is a local refinement, not a new strategy family. It intentionally
    keeps the same train/validation split, so round 2/3 numbers become
    progressively more in-sample; validate the finalists forward.
    """
    if not results:
        return dict(params)
    eligible = [
        item
        for item in results
        if item.get("val_fills") is not None
        and item["val_fills"] >= int(params.get("min_val_fills", 10))
        and item.get("val_roi") is not None
        and item.get("score", -999) > -900
    ]
    top = (eligible or results)[: max(1, int(top_k))]
    offsets: set[float] = set()
    window_sets: list[str] = []
    bands: list[tuple[float, float]] = []
    sizes: set[float] = set()
    rebates: set[float] = set()
    for item in top:
        combo = item.get("params") or {}
        try:
            base = float(combo.get("offset", 0))
        except (TypeError, ValueError):
            base = 0
        for delta in (-0.002, -0.001, 0.0, 0.001, 0.002):
            value = round(base + delta, 4)
            if value > 0:
                offsets.add(value)
        window = combo.get("window_set")
        if window in WINDOW_SETS and window not in window_sets:
            window_sets.append(window)
        try:
            low, high = float(combo["band"][0]), float(combo["band"][1])
        except (KeyError, TypeError, ValueError):
            continue
        candidates = [(low, high)]
        if high - low >= 0.10:
            candidates.extend([(round(low + 0.05, 2), high), (low, round(high - 0.05, 2))])
        if low >= 0.05:
            candidates.append((round(low - 0.05, 2), high))
        for band in candidates:
            if 0 < band[0] < band[1] <= 1 and band not in bands:
                bands.append(band)
        try:
            size = float(combo.get("size", 10))
        except (TypeError, ValueError):
            size = 10.0
        sizes.update({size, round(size * 2, 2)})
        try:
            rebates.add(float(combo.get("rebate_bps", 0)))
        except (TypeError, ValueError):
            rebates.add(0.0)
    refined = dict(params)
    refined["offsets"] = sorted(offsets)[:4] or params["offsets"]
    refined["window_sets"] = (window_sets[:2] or params["window_sets"])
    refined["bands"] = bands[:3] or params["bands"]
    refined["sizes"] = sorted(sizes)[:2] or params["sizes"]
    refined["rebates"] = sorted(rebates | {0.0})[:3] or params["rebates"]
    return refined


def run_auto_sweep(
    job_id: str,
    params: dict[str, Any] | None = None,
    progress: ProgressFn | None = None,
) -> dict[str, Any]:
    params = dict(params or {})
    rounds = max(1, int(params.get("auto_rounds", 3)))
    top_k = max(1, int(params.get("top_k", 8)))
    base = {key: value for key, value in params.items() if key not in ("auto_rounds", "top_k")}
    current = dict(base)
    history: list[dict[str, Any]] = []
    overall_best: dict[str, Any] | None = None
    for round_number in range(1, rounds + 1):
        run_id = f"{job_id}-r{round_number}"

        def round_progress(done: int, total: int, message: str, _round: int = round_number) -> None:
            if progress:
                percent = int(((_round - 1) + max(0, min(100, done)) / 100.0) / rounds * 100)
                progress(percent, 100, f"round {_round}/{rounds}: {message}")

        result = run_sweep(run_id=run_id, params=current, progress=round_progress)
        summary = result.get("summary") or {}
        best = summary.get("best") or {}
        history.append(
            {
                "round": round_number,
                "run_id": run_id,
                "combos": summary.get("combos"),
                "best": best,
            }
        )
        if best and (
            overall_best is None
            or (best.get("score") or -999) > (overall_best.get("score") or -999)
            or (
                (best.get("score") or -999) == (overall_best.get("score") or -999)
                and (best.get("val_pnl") or 0) > (overall_best.get("val_pnl") or 0)
            )
        ):
            overall_best = {**best, "run_id": run_id, "round": round_number}
        if round_number >= rounds or summary.get("empty") or not best:
            break
        next_params = refine_params(current, result.get("results") or [], top_k)
        if next_params == current:
            break
        current = next_params
    summary = {
        "rounds": len(history),
        "history": history,
        "best": overall_best,
        "base_params": base,
        "generated_at": now_ts(),
    }
    if progress:
        progress(100, 100, "auto sweep complete")
    return {"summary": summary}


def _store_results(conn: sqlite3.Connection, run_id: str, results: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM sweep_results WHERE run_id=?", (run_id,))
    conn.executemany(
        """INSERT INTO sweep_results(
             run_id, combo_id, params_json, family, train_pnl, train_roi, train_fills,
             train_win_rate, val_pnl, val_roi, val_fills, val_win_rate, pnl, roi, fills,
             win_rate, cost, max_drawdown, score, rank,
             train_profit_factor, val_profit_factor, profit_factor)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [
            (
                run_id,
                item["combo_id"],
                to_json(item["params"]),
                item["params"]["family"],
                item["train_pnl"],
                item["train_roi"],
                item["train_fills"],
                item["train_win_rate"],
                item["val_pnl"],
                item["val_roi"],
                item["val_fills"],
                item["val_win_rate"],
                item["pnl"],
                item["roi"],
                item["fills"],
                item["win_rate"],
                item["cost"],
                item["max_drawdown"],
                item["score"],
                item["rank"],
                item.get("train_profit_factor"),
                item.get("val_profit_factor"),
                item.get("profit_factor"),
            )
            for item in results
        ],
    )
    conn.commit()


def _finish(
    run_id: str,
    summary: dict[str, Any],
    results: list[dict[str, Any]],
    conn: sqlite3.Connection | None = None,
) -> None:
    own = conn is None
    conn = conn or connect()
    try:
        conn.execute(
            "UPDATE sweep_runs SET status='DONE', summary_json=?, finished_at=? WHERE id=?",
            (to_json(summary), now_ts(), run_id),
        )
        conn.commit()
    finally:
        if own:
            conn.close()


def list_runs(conn: sqlite3.Connection, limit: int = 20) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM sweep_runs ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["summary"] = from_json(item.get("summary_json"), {}) or {}
        item["params"] = from_json(item.get("params_json"), {}) or {}
        out.append(item)
    return out


def get_run(conn: sqlite3.Connection, run_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM sweep_runs WHERE id=?", (run_id,)).fetchone()
    if not row:
        return None
    item = dict(row)
    item["summary"] = from_json(item.get("summary_json"), {}) or {}
    item["params"] = from_json(item.get("params_json"), {}) or {}
    item["results"] = [
        {
            **dict(result),
            "params": from_json(result["params_json"], {}) or {},
        }
        for result in conn.execute(
            "SELECT * FROM sweep_results WHERE run_id=? ORDER BY rank ASC LIMIT 60", (run_id,)
        ).fetchall()
    ]
    return item


def save_strategy(
    name: str,
    family: str,
    params: dict[str, Any],
    metrics: dict[str, Any],
    source_run: str | None,
) -> str:
    strategy_id = f"stg_{secrets.token_hex(6)}"
    conn = connect()
    try:
        conn.execute(
            """INSERT INTO strategies(id, name, family, params_json, metrics_json, source_run, created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (strategy_id, name, family, to_json(params), to_json(metrics), source_run, now_ts()),
        )
        conn.commit()
    finally:
        conn.close()
    return strategy_id


def list_strategies(conn: sqlite3.Connection, limit: int = 30) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM strategies ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["params"] = from_json(item.get("params_json"), {}) or {}
        item["metrics"] = from_json(item.get("metrics_json"), {}) or {}
        out.append(item)
    return out
