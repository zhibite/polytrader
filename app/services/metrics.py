from __future__ import annotations

import math
import sqlite3
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from ..db import from_json, now_ts

WINDOWS = ["pre-open", "0-30s", "30-60s", "1-2m", "2-3m", "3-4m", "4-5m", "5m+"]
BUCKETS = ["<0.10", "<0.20", "<0.30", "<0.40", "<0.50", "<0.60", "<0.70", "<0.80", "<0.90", ">=0.90"]
HORIZONS = [
    ("entry_edge", "Entry edge", "mid − fill"),
    ("markout_1s", "+1s", "mid t+1s"),
    ("markout_10s", "+10s", "mid t+10s"),
    ("markout_60s", "+60s", "mid t+60s"),
    ("markout_end", "Settlement", "mid at close"),
]


def window_start(days: int | None, now: int | None = None) -> int:
    now = now or now_ts()
    if days is None or days <= 0:
        return 1
    return now - int(days) * 86400


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _pct(value: float | None) -> float | None:
    return None if value is None else value * 100.0


def _quantiles(values: list[float], points: tuple[float, ...] = (0.5, 0.9, 0.99)) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(values)

    def pick(q: float) -> float:
        index = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
        return ordered[index]

    return {f"p{int(q * 100)}": pick(q) for q in points}


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def _histogram(values: list[float], bins: int = 24, lo: float | None = None, hi: float | None = None) -> dict[str, Any]:
    if not values:
        return {"labels": [], "counts": []}
    lo = min(values) if lo is None else lo
    hi = max(values) if hi is None else hi
    if hi <= lo:
        hi = lo + 1e-9
    width = (hi - lo) / bins
    counts = [0] * bins
    for value in values:
        index = min(bins - 1, max(0, int((value - lo) / width)))
        counts[index] += 1
    labels = [f"{lo + width * i:.3f}" for i in range(bins)]
    return {"labels": labels, "counts": counts, "lo": lo, "hi": hi, "width": width}


def overview(conn: sqlite3.Connection, address: str, days: int | None) -> dict[str, Any]:
    start = window_start(days)
    stats = conn.execute(
        """SELECT side, is_taker, price, size, usdc_size, condition_id, slug,
                  outcome, token_id, ts
           FROM trades WHERE address=? AND ts>=?""",
        (address, start),
    ).fetchall()
    if not stats:
        return {"empty": True, "fills": 0, "days": days}
    rows = [dict(row) for row in stats]
    fills = len(rows)
    maker = sum(1 for row in rows if not row["is_taker"])
    taker = fills - maker
    buys = sum(1 for row in rows if str(row["side"]).upper() == "BUY")
    sells = fills - buys
    conditions = {row["condition_id"] for row in rows if row["condition_id"]}
    slugs = {row["slug"] for row in rows if row["slug"]}
    prices = [float(row["price"] or 0) for row in rows]
    sizes = [float(row["size"] or 0) for row in rows]
    volume = sum(float(row["usdc_size"] or 0) for row in rows)
    shares = sum(sizes)
    ts_values = sorted(int(row["ts"]) for row in rows)
    gaps = [b - a for a, b in zip(ts_values, ts_values[1:]) if b >= a]
    assets = Counter((row["slug"] or "").split("-")[0] for row in rows if row["slug"])
    outcomes = Counter(row["outcome"] for row in rows)
    taker_fills = [row for row in rows if row["is_taker"]]
    maker_rows = [row for row in rows if not row["is_taker"]]
    per_market = Counter(row["condition_id"] for row in rows)
    top_market_counts = per_market.most_common(10)
    total_volume_by_market = Counter()
    for row in rows:
        total_volume_by_market[row["condition_id"]] += float(row["usdc_size"] or 0)
    total_volume = sum(total_volume_by_market.values()) or 1.0
    hhi = sum((value / total_volume) ** 2 for value in total_volume_by_market.values())
    top_volume = total_volume_by_market.most_common(5)
    price_bands = Counter()
    edges = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.01]
    for price in prices:
        for index in range(len(edges) - 1):
            if edges[index] <= price < edges[index + 1]:
                price_bands[f"{edges[index]:.2f}-{edges[index + 1]:.2f}"] += 1
                break
    return {
        "empty": False,
        "days": days,
        "start": start,
        "first_ts": ts_values[0],
        "last_ts": ts_values[-1],
        "fills": fills,
        "maker": maker,
        "taker": taker,
        "maker_share": maker / fills if fills else None,
        "taker_share": taker / fills if fills else None,
        "buys": buys,
        "sells": sells,
        "buy_share": buys / fills if fills else None,
        "markets": len(conditions),
        "slugs": len(slugs),
        "volume_usdc": volume,
        "shares": shares,
        "avg_price": _mean(prices),
        "median_price": _median(prices),
        "avg_size": _mean(sizes),
        "median_size": _median(sizes),
        "size_quantiles": _quantiles(sizes),
        "gap_quantiles": _quantiles([float(g) for g in gaps]),
        "median_gap": _median([float(g) for g in gaps]),
        "assets": dict(assets.most_common(8)),
        "outcomes": dict(outcomes),
        "taker_volume": sum(float(row["usdc_size"] or 0) for row in taker_fills),
        "maker_volume": sum(float(row["usdc_size"] or 0) for row in maker_rows),
        "fills_per_market": _mean([float(v) for v in per_market.values()]),
        "max_fills_market": max(per_market.values()) if per_market else 0,
        "hhi": hhi,
        "top_markets": [
            {"condition_id": cid, "count": count, "volume": total_volume_by_market[cid]}
            for cid, count in top_market_counts
        ],
        "price_bands": dict(price_bands),
        "unique_tokens": len({row["token_id"] for row in rows if row["token_id"]}),
    }


def execution(conn: sqlite3.Connection, address: str, days: int | None) -> dict[str, Any]:
    start = window_start(days)
    rows = _rows(
        conn,
        """SELECT side, price, size, is_taker, ts, entry_edge, markout_1s, markout_10s,
                  markout_60s, markout_end, window_label, price_bucket, elapsed
           FROM trades WHERE address=? AND ts>=? AND enriched_at IS NOT NULL""",
        (address, start),
    )
    if not rows:
        return {"empty": True, "enriched": 0}
    horizons: list[dict[str, Any]] = []
    for key, label, desc in HORIZONS:
        values = [float(row[key]) for row in rows if row[key] is not None]
        horizons.append(
            {
                "key": key,
                "label": label,
                "desc": desc,
                "n": len(values),
                "mean": _mean(values),
                "median": _median(values),
                "positive_share": (sum(1 for v in values if v > 0) / len(values)) if values else None,
                "mean_cents": _mean(values) * 100 if values else None,
            }
        )
    by_window: dict[str, dict[str, Any]] = {}
    for label in WINDOWS:
        subset = [row for row in rows if row["window_label"] == label]
        if not subset:
            continue
        by_window[label] = {
            "n": len(subset),
            "entry_edge": _mean([float(r["entry_edge"]) for r in subset if r["entry_edge"] is not None]),
            "markout_60s": _mean([float(r["markout_60s"]) for r in subset if r["markout_60s"] is not None]),
            "markout_end": _mean([float(r["markout_end"]) for r in subset if r["markout_end"] is not None]),
            "positive_end_share": (
                sum(1 for r in subset if r["markout_end"] is not None and float(r["markout_end"]) > 0)
                / max(1, sum(1 for r in subset if r["markout_end"] is not None))
            ),
        }
    by_bucket: dict[str, dict[str, Any]] = {}
    for label in BUCKETS:
        subset = [row for row in rows if row["price_bucket"] == label]
        if not subset:
            continue
        by_bucket[label] = {
            "n": len(subset),
            "entry_edge": _mean([float(r["entry_edge"]) for r in subset if r["entry_edge"] is not None]),
            "markout_60s": _mean([float(r["markout_60s"]) for r in subset if r["markout_60s"] is not None]),
            "markout_end": _mean([float(r["markout_end"]) for r in subset if r["markout_end"] is not None]),
        }
    edges = [float(row["entry_edge"]) for row in rows if row["entry_edge"] is not None]
    ends = [float(row["markout_end"]) for row in rows if row["markout_end"] is not None]
    maker_edges = [
        float(row["entry_edge"]) for row in rows if not row["is_taker"] and row["entry_edge"] is not None
    ]
    taker_edges = [
        float(row["entry_edge"]) for row in rows if row["is_taker"] and row["entry_edge"] is not None
    ]
    return {
        "empty": False,
        "enriched": len(rows),
        "horizons": horizons,
        "by_window": by_window,
        "by_bucket": by_bucket,
        "entry_edge_hist": _histogram(edges, bins=30),
        "markout_end_hist": _histogram(ends, bins=30),
        "entry_edge_mean": _mean(edges),
        "entry_edge_median": _median(edges),
        "entry_edge_positive": (sum(1 for v in edges if v > 0) / len(edges)) if edges else None,
        "markout_end_mean": _mean(ends),
        "markout_end_median": _median(ends),
        "maker_entry_edge": _mean(maker_edges),
        "taker_entry_edge": _mean(taker_edges),
        "maker_n": len(maker_edges),
        "taker_n": len(taker_edges),
    }


def pairing(conn: sqlite3.Connection, address: str, days: int | None) -> dict[str, Any]:
    start = window_start(days)
    rows = _rows(
        conn,
        """SELECT * FROM pair_markets WHERE address=? AND (first_ts IS NULL OR first_ts>=?)""",
        (address, start),
    )
    if not rows:
        return {"empty": True, "markets": 0}
    two_sided = [row for row in rows if _num(row["up_shares"]) > 0 and _num(row["down_shares"]) > 0]
    matched = [_num(row["matched_shares"]) for row in rows if row["pair_cost"] is not None]
    pair_costs = [float(row["pair_cost"]) for row in rows if row["pair_cost"] is not None]
    weighted_den = sum(matched)
    weighted_pair_cost = (
        sum(float(row["pair_cost"]) * _num(row["matched_shares"]) for row in rows if row["pair_cost"] is not None)
        / weighted_den
        if weighted_den
        else None
    )
    locked = sum(_num(row["locked_profit"]) for row in rows)
    matched_share = sum(matched)
    below_one = sum(
        _num(row["matched_shares"]) for row in rows if row["pair_cost"] is not None and float(row["pair_cost"]) < 1.0
    )
    resolved = [row for row in rows if row["resolved"] in ("UP", "DOWN")]
    settlement_pnl = sum(_num(row["settlement_pnl"]) for row in resolved)
    total_cost = sum(_num(row["total_cost"]) for row in resolved)
    payouts = sum(_num(row["payout"]) for row in resolved)
    winners = sum(1 for row in resolved if _num(row["settlement_pnl"]) > 0)
    top = sorted(
        [row for row in rows if row["settlement_pnl"] is not None],
        key=lambda row: _num(row["settlement_pnl"]),
        reverse=True,
    )[:12]
    return {
        "empty": False,
        "markets": len(rows),
        "two_sided": len(two_sided),
        "two_sided_ratio": len(two_sided) / len(rows) if rows else None,
        "both_side_markets": len(two_sided),
        "matched_shares": matched_share,
        "unmatched_shares": sum(
            abs(_num(row["up_shares"]) - _num(row["down_shares"])) for row in rows
        ),
        "weighted_pair_cost": weighted_pair_cost,
        "median_pair_cost": _median(pair_costs),
        "pair_cost_p25": _quantiles(pair_costs, (0.25, 0.5, 0.75)).get("p25"),
        "pair_cost_p75": _quantiles(pair_costs, (0.25, 0.5, 0.75)).get("p75"),
        "pair_cost_hist": _histogram(pair_costs, bins=24, lo=0.85, hi=1.20),
        "share_matched_below_one": (below_one / matched_share) if matched_share else None,
        "locked_profit": locked,
        "resolved_markets": len(resolved),
        "settlement_pnl": settlement_pnl,
        "settlement_roi": (settlement_pnl / total_cost) if total_cost else None,
        "payout": payouts,
        "total_cost": total_cost,
        "win_markets": winners,
        "win_ratio": (winners / len(resolved)) if resolved else None,
        "top_markets": [
            {
                "slug": row["slug"],
                "title": row["title"],
                "up_shares": _num(row["up_shares"]),
                "down_shares": _num(row["down_shares"]),
                "pair_cost": row["pair_cost"],
                "settlement_pnl": row["settlement_pnl"],
                "resolved": row["resolved"],
            }
            for row in top
        ],
    }


def timing(conn: sqlite3.Connection, address: str, days: int | None) -> dict[str, Any]:
    start = window_start(days)
    rows = _rows(
        conn,
        "SELECT ts, window_label, elapsed, size, usdc_size FROM trades WHERE address=? AND ts>=?",
        (address, start),
    )
    if not rows:
        return {"empty": True}
    windows = Counter(row["window_label"] or "unknown" for row in rows)
    hours = Counter(datetime.fromtimestamp(int(row["ts"]), timezone.utc).hour for row in rows)
    weekdays = Counter(datetime.fromtimestamp(int(row["ts"]), timezone.utc).weekday() for row in rows)
    elapsed = [int(row["elapsed"]) for row in rows if row["elapsed"] is not None]
    daily = Counter(
        datetime.fromtimestamp(int(row["ts"]), timezone.utc).strftime("%Y-%m-%d") for row in rows
    )
    return {
        "empty": False,
        "windows": {label: windows[label] for label in WINDOWS if windows[label]},
        "hours": {str(hour): hours[hour] for hour in range(24)},
        "weekdays": {str(day): weekdays[day] for day in range(7)},
        "elapsed_median": _median([float(v) for v in elapsed]),
        "elapsed_quantiles": _quantiles([float(v) for v in elapsed]),
        "daily": dict(sorted(daily.items())),
    }


def rebates(conn: sqlite3.Connection, address: str, days: int | None) -> dict[str, Any]:
    start = window_start(days)
    rows = _rows(
        conn,
        "SELECT type, COUNT(*) AS n, SUM(usdc_size) AS usdc, SUM(size) AS size FROM activity "
        "WHERE address=? AND ts>=? GROUP BY type ORDER BY n DESC",
        (address, start),
    )
    by_type = {
        row["type"]: {"n": row["n"], "usdc": _num(row["usdc"]), "size": _num(row["size"])}
        for row in rows
    }
    stats = conn.execute("SELECT * FROM wallet_stats WHERE address=?", (address,)).fetchone()
    fills = conn.execute(
        "SELECT COUNT(*) AS n, SUM(usdc_size) AS v FROM trades WHERE address=? AND ts>=?",
        (address, start),
    ).fetchone()
    maker_rebate = by_type.get("MAKER_REBATE", {}).get("usdc", 0.0)
    taker_rebate = by_type.get("TAKER_REBATE", {}).get("usdc", 0.0)
    reward = by_type.get("REWARD", {}).get("usdc", 0.0)
    volume = _num(fills["v"]) if fills else 0.0
    fill_count = int(fills["n"]) if fills else 0
    lifetime_economic = _num(stats["economic_pnl"]) if stats else None
    # Window PnL from the cumulative user-pnl series. The baseline is the last
    # point at or before the window start so the delta is a true window value.
    baseline = conn.execute(
        "SELECT economic_pnl, realized_pnl, fees FROM pnl_points "
        "WHERE address=? AND ts<=? ORDER BY ts DESC LIMIT 1",
        (address, start),
    ).fetchone()
    latest = conn.execute(
        "SELECT economic_pnl, realized_pnl, fees FROM pnl_points "
        "WHERE address=? ORDER BY ts DESC LIMIT 1",
        (address,),
    ).fetchone()
    first_in_window = conn.execute(
        "SELECT economic_pnl, realized_pnl, fees FROM pnl_points "
        "WHERE address=? AND ts>=? ORDER BY ts ASC LIMIT 1",
        (address, start),
    ).fetchone()
    window_economic = None
    window_realized = None
    window_fees = None
    if latest and baseline:
        window_economic = _num(latest["economic_pnl"]) - _num(baseline["economic_pnl"])
        window_realized = _num(latest["realized_pnl"]) - _num(baseline["realized_pnl"])
        window_fees = _num(latest["fees"]) - _num(baseline["fees"])
    elif latest and first_in_window and latest is not first_in_window:
        window_economic = _num(latest["economic_pnl"]) - _num(first_in_window["economic_pnl"])
        window_realized = _num(latest["realized_pnl"]) - _num(first_in_window["realized_pnl"])
        window_fees = _num(latest["fees"]) - _num(first_in_window["fees"])
    economic_for_ratio = window_economic if window_economic and window_economic > 0 else lifetime_economic
    return {
        "by_type": by_type,
        "maker_rebate": maker_rebate,
        "taker_rebate": taker_rebate,
        "reward": reward,
        "total_income": maker_rebate + taker_rebate + reward,
        "volume": volume,
        "fills": fill_count,
        "rebate_per_fill": (maker_rebate + taker_rebate) / fill_count if fill_count else None,
        "rebate_bps": ((maker_rebate + taker_rebate) / volume * 10000) if volume else None,
        "rebate_share_of_economic": (
            (maker_rebate + taker_rebate) / economic_for_ratio
            if economic_for_ratio and economic_for_ratio > 0
            else None
        ),
        "lifetime_economic_pnl": lifetime_economic,
        "lifetime_rebate_share": (
            ((_num(stats["maker_rebate"]) + _num(stats["taker_rebate"])) / lifetime_economic)
            if stats and lifetime_economic and lifetime_economic > 0
            else None
        ),
        "window_economic_pnl": window_economic,
        "window_realized_pnl": window_realized,
        "window_fees": window_fees,
        "redeem_count": by_type.get("REDEEM", {}).get("n", 0),
        "redeem_usdc": by_type.get("REDEEM", {}).get("usdc", 0.0),
        "split_count": by_type.get("SPLIT", {}).get("n", 0),
        "merge_count": by_type.get("MERGE", {}).get("n", 0),
        "conversion_count": by_type.get("CONVERSION", {}).get("n", 0),
    }


def pnl_series(conn: sqlite3.Connection, address: str, days: int | None, max_points: int = 400) -> dict[str, Any]:
    start = window_start(days)
    rows = _rows(
        conn,
        """SELECT ts, realized_pnl, unrealized_pnl, economic_pnl, maker_rebate, taker_rebate,
                  fees, volume_usdc, volume_shares, trade_count
           FROM pnl_points WHERE address=? AND ts>=? ORDER BY ts""",
        (address, start),
    )
    if not rows:
        return {"points": [], "daily": []}
    step = max(1, math.ceil(len(rows) / max_points))
    sampled = rows[::step]
    if sampled[-1] is not rows[-1]:
        sampled.append(rows[-1])
    points = [
        {
            "ts": row["ts"],
            "realized": _num(row["realized_pnl"]),
            "unrealized": _num(row["unrealized_pnl"]),
            "economic": _num(row["economic_pnl"]),
            "rebate": _num(row["maker_rebate"]) + _num(row["taker_rebate"]),
            "fees": _num(row["fees"]),
            "volume": _num(row["volume_usdc"]),
            "fills": int(row["trade_count"] or 0),
        }
        for row in sampled
    ]
    # daily deltas from the last point of each UTC day
    by_day: dict[str, dict[str, Any]] = {}
    for row in rows:
        day = datetime.fromtimestamp(int(row["ts"]), timezone.utc).strftime("%Y-%m-%d")
        by_day[day] = row
    daily = []
    previous = None
    for day in sorted(by_day):
        row = by_day[day]
        delta = {
            "day": day,
            "realized": _num(row["realized_pnl"]) - (_num(previous["realized_pnl"]) if previous else 0),
            "economic": _num(row["economic_pnl"]) - (_num(previous["economic_pnl"]) if previous else 0),
            "rebate": (_num(row["maker_rebate"]) + _num(row["taker_rebate"]))
            - ((_num(previous["maker_rebate"]) + _num(previous["taker_rebate"])) if previous else 0),
            "fees": _num(row["fees"]) - (_num(previous["fees"]) if previous else 0),
            "volume": _num(row["volume_usdc"]) - (_num(previous["volume_usdc"]) if previous else 0),
            "fills": int(row["trade_count"] or 0) - (int(previous["trade_count"] or 0) if previous else 0),
        }
        daily.append(delta)
        previous = row
    return {"points": points, "daily": daily}


def pnl_decomposition(conn: sqlite3.Connection, address: str, days: int | None) -> dict[str, Any]:
    """Answer 'where does the money come from' with an explicit reconciliation."""
    start = window_start(days)
    stats = conn.execute("SELECT * FROM wallet_stats WHERE address=?", (address,)).fetchone()
    rebate_rows = conn.execute(
        "SELECT type, COALESCE(SUM(usdc_size),0) AS usdc FROM activity "
        "WHERE address=? AND ts>=? AND type IN ('MAKER_REBATE','TAKER_REBATE','REWARD') GROUP BY type",
        (address, start),
    ).fetchall()
    window_rebates = sum(_num(row["usdc"]) for row in rebate_rows)
    baseline = conn.execute(
        "SELECT realized_pnl, economic_pnl, fees FROM pnl_points WHERE address=? AND ts<=? "
        "ORDER BY ts DESC LIMIT 1",
        (address, start),
    ).fetchone()
    latest = conn.execute(
        "SELECT realized_pnl, economic_pnl, fees FROM pnl_points WHERE address=? ORDER BY ts DESC LIMIT 1",
        (address,),
    ).fetchone()
    if latest and baseline:
        window_realized = _num(latest["realized_pnl"]) - _num(baseline["realized_pnl"])
        window_economic = _num(latest["economic_pnl"]) - _num(baseline["economic_pnl"])
        window_fees = _num(latest["fees"]) - _num(baseline["fees"])
    else:
        window_realized = window_economic = window_fees = None
    lifetime_realized = _num(stats["realized_pnl"]) if stats else None
    lifetime_rebates = (
        _num(stats["maker_rebate"]) + _num(stats["taker_rebate"]) + _num(stats["reward_income"])
        if stats
        else None
    )
    lifetime_fees = _num(stats["fees"]) if stats else None
    lifetime_economic = _num(stats["economic_pnl"]) if stats else None
    return {
        "lifetime": {
            "realized_pnl": lifetime_realized,
            "maker_rebate": _num(stats["maker_rebate"]) if stats else None,
            "taker_rebate": _num(stats["taker_rebate"]) if stats else None,
            "reward_income": _num(stats["reward_income"]) if stats else None,
            "fees": lifetime_fees,
            "economic_pnl": lifetime_economic,
            "residual": (
                lifetime_economic - (lifetime_realized + lifetime_rebates + lifetime_fees)
                if lifetime_economic is not None
                else None
            ),
        },
        "window": {
            "realized_pnl": window_realized,
            "rebates": window_rebates,
            "fees": window_fees,
            "economic_pnl": window_economic,
            "residual": (
                window_economic - (window_realized + window_rebates + window_fees)
                if window_economic is not None
                else None
            ),
        },
    }


def stats_row(conn: sqlite3.Connection, address: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM wallet_stats WHERE address=?", (address,)).fetchone()
    return dict(row) if row else None


def profile_row(conn: sqlite3.Connection, address: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM profiles WHERE address=?", (address,)).fetchone()
    if not row:
        return None
    data = dict(row)
    data["tags"] = from_json(data.get("tags_json"), [])
    data["evidence"] = from_json(data.get("evidence_json"), [])
    data["features"] = from_json(data.get("features_json"), {})
    return data


def build_features(conn: sqlite3.Connection, address: str, days: int | None = None) -> dict[str, Any]:
    days = 7 if days is None else days
    ov = overview(conn, address, days)
    ex = execution(conn, address, days)
    pair = pairing(conn, address, days)
    tm = timing(conn, address, days)
    rb = rebates(conn, address, days)
    stats = stats_row(conn, address) or {}
    return {
        "generated_at": now_ts(),
        "window_days": days,
        "overview": ov,
        "execution": ex,
        "pairing": pair,
        "timing": tm,
        "rebates": rb,
        "lifetime": {
            "markets": stats.get("markets"),
            "fills": stats.get("fills"),
            "volume_usdc": stats.get("volume_usdc"),
            "realized_pnl": stats.get("realized_pnl"),
            "economic_pnl": stats.get("economic_pnl"),
            "maker_rebate": stats.get("maker_rebate"),
            "taker_rebate": stats.get("taker_rebate"),
            "fees": stats.get("fees"),
            "join_date": stats.get("join_date"),
        },
    }


def dossier(conn: sqlite3.Connection, address: str, days: int | None = None) -> dict[str, Any]:
    days = 7 if days is None else days
    features = build_features(conn, address, days)
    return {
        "address": address,
        "days": days,
        "stats": stats_row(conn, address),
        "profile": profile_row(conn, address),
        "pnl": pnl_series(conn, address, days),
        "decomposition": pnl_decomposition(conn, address, days),
        "features": features,
        "generated_at": now_ts(),
    }
