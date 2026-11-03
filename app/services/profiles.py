from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

from ..db import from_json, now_ts, to_json
from .insights import build_insights
from .metrics import (
    build_features,
    execution,
    overview,
    pairing,
    pnl_decomposition,
    rebates,
    timing,
)


def _safe(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _holding_stats(conn: sqlite3.Connection, address: str, days: int) -> dict[str, Any]:
    rows = conn.execute(
        """SELECT ts, market_end, side FROM trades
           WHERE address=? AND ts>=? AND market_end IS NOT NULL""",
        (address, now_ts() - days * 86400 if days > 0 else 1),
    ).fetchall()
    holds = [
        max(0, int(row["market_end"]) - int(row["ts"]))
        for row in rows
        if row["market_end"] and row["ts"]
    ]
    if not holds:
        return {"median_hold_s": None, "median_hold_min": None}
    holds.sort()
    median = holds[len(holds) // 2]
    return {"median_hold_s": median, "median_hold_min": median / 60.0}


def _long_tail_share(conn: sqlite3.Connection, address: str, days: int) -> float | None:
    start = now_ts() - days * 86400 if days > 0 else 1
    row = conn.execute(
        """SELECT COUNT(*) AS n,
                  SUM(CASE WHEN price < 0.20 THEN 1 ELSE 0 END) AS tail
           FROM trades WHERE address=? AND ts>=?""",
        (address, start),
    ).fetchone()
    if not row or not row["n"]:
        return None
    return _safe(row["tail"]) / _safe(row["n"])


def _outcome_imbalance(conn: sqlite3.Connection, address: str, days: int) -> dict[str, Any]:
    start = now_ts() - days * 86400 if days > 0 else 1
    row = conn.execute(
        """SELECT SUM(CASE WHEN outcome_index=0 THEN size ELSE 0 END) AS up,
                  SUM(CASE WHEN outcome_index=1 THEN size ELSE 0 END) AS down
           FROM trades WHERE address=? AND ts>=?""",
        (address, start),
    ).fetchone()
    up = _safe(row["up"]) if row else 0.0
    down = _safe(row["down"]) if row else 0.0
    total = up + down
    return {
        "up_shares": up,
        "down_shares": down,
        "imbalance": (abs(up - down) / total) if total else None,
    }


def compute_features(conn: sqlite3.Connection, address: str, days: int = 7) -> dict[str, Any]:
    ov = overview(conn, address, days)
    ex = execution(conn, address, days)
    pair = pairing(conn, address, days)
    rb = rebates(conn, address, days)
    tm = timing(conn, address, days)
    hold = _holding_stats(conn, address, days)
    tail = _long_tail_share(conn, address, days)
    imbalance = _outcome_imbalance(conn, address, days)
    decomposition = pnl_decomposition(conn, address, days)
    fills = _safe(ov.get("fills"))
    if days and days > 0:
        days_effective = float(days)
    elif ov.get("first_ts") and ov.get("last_ts"):
        days_effective = max(1.0, (float(ov["last_ts"]) - float(ov["first_ts"])) / 86400.0)
    else:
        days_effective = 1.0
    horizons = {h["key"]: h for h in (ex.get("horizons") or [])}
    return {
        "window_days": days,
        "fills": fills,
        "fills_per_day": fills / days_effective,
        "maker_share": ov.get("maker_share"),
        "taker_share": ov.get("taker_share"),
        "buy_share": ov.get("buy_share"),
        "median_gap_s": ov.get("median_gap"),
        "markets": ov.get("markets"),
        "volume_usdc": ov.get("volume_usdc"),
        "avg_size": ov.get("avg_size"),
        "median_size": ov.get("median_size"),
        "two_sided_ratio": pair.get("two_sided_ratio"),
        "median_pair_cost": pair.get("median_pair_cost"),
        "weighted_pair_cost": pair.get("weighted_pair_cost"),
        "share_matched_below_one": pair.get("share_matched_below_one"),
        "locked_profit": pair.get("locked_profit"),
        "settlement_pnl": pair.get("settlement_pnl"),
        "entry_edge_mean": ex.get("entry_edge_mean"),
        "entry_edge_positive": ex.get("entry_edge_positive"),
        "markout_1s": (horizons.get("markout_1s") or {}).get("mean"),
        "markout_10s": (horizons.get("markout_10s") or {}).get("mean"),
        "markout_60s": (horizons.get("markout_60s") or {}).get("mean"),
        "markout_end": (horizons.get("markout_end") or {}).get("mean"),
        "maker_rebate": rb.get("maker_rebate"),
        "taker_rebate": rb.get("taker_rebate"),
        "rebate_share_of_economic": rb.get("rebate_share_of_economic"),
        "rebate_share_lifetime": rb.get("lifetime_rebate_share"),
        "window_economic_pnl": rb.get("window_economic_pnl"),
        "rebate_bps": rb.get("rebate_bps"),
        "rebate_per_fill": rb.get("rebate_per_fill"),
        "redeem_count": rb.get("redeem_count"),
        "split_count": rb.get("split_count"),
        "merge_count": rb.get("merge_count"),
        "median_hold_s": hold.get("median_hold_s"),
        "median_hold_min": hold.get("median_hold_min"),
        "long_tail_share": tail,
        "outcome_imbalance": imbalance.get("imbalance"),
        "market_participation": (
            (_safe(ov.get("markets")) / (days_effective * 288.0))
            if _safe(((ov.get("assets") or {}).get("btc", 0))) / max(1.0, fills) >= 0.8
            else None
        ),
        "hhi": ov.get("hhi"),
        "avg_price": ov.get("avg_price"),
        "size_p90": (ov.get("size_quantiles") or {}).get("p90"),
        "decomposition": decomposition,
        # Nested context used by the insight writer and kept in the stored
        # profile so the dossier can render evidence without recomputation.
        "overview": ov,
        "execution": ex,
        "pairing": pair,
        "rebates": rb,
        "timing": tm,
    }


def classify(features: dict[str, Any]) -> dict[str, Any]:
    maker = _safe(features.get("maker_share"))
    taker = _safe(features.get("taker_share"))
    fpd = _safe(features.get("fills_per_day"))
    gap = _safe(features.get("median_gap_s"), 1e9)
    two_sided = _safe(features.get("two_sided_ratio"))
    pair_below = _safe(features.get("share_matched_below_one"))
    rebate_share = max(
        _safe(features.get("rebate_share_of_economic")),
        _safe(features.get("rebate_share_lifetime")),
    )
    rebate_bps = _safe(features.get("rebate_bps"))
    edge = _safe(features.get("entry_edge_mean"))
    markout_end = _safe(features.get("markout_end"))
    hold_min = _safe(features.get("median_hold_min"))
    tail = _safe(features.get("long_tail_share"))
    imbalance = _safe(features.get("outcome_imbalance"))
    markets = _safe(features.get("markets"))

    scores: dict[str, float] = {}
    reasons: dict[str, list[str]] = {}

    def add(name: str, score: float, reason: str) -> None:
        if score <= 0:
            return
        scores[name] = scores.get(name, 0.0) + score
        reasons.setdefault(name, []).append(reason)

    if maker >= 0.90:
        add("hft_maker", 2.5, f"maker 占比 {maker:.1%}")
    elif maker >= 0.75:
        add("hft_maker", 1.5, f"maker 占比 {maker:.1%}")
    if fpd >= 1000:
        add("hft_maker", 2.0, f"每日 {fpd:,.0f} 笔成交")
    elif fpd >= 300:
        add("hft_maker", 1.2, f"每日 {fpd:,.0f} 笔成交")
    if gap <= 15:
        add("hft_maker", 1.5, f"成交间隔中位数 {gap:.0f}s")
    if two_sided >= 0.5:
        add("hft_maker", 0.8, f"{two_sided:.0%} 市场双边成交")

    if rebate_share >= 0.20:
        add("rebate_farmer", 3.0, f"返佣占经济 PnL {rebate_share:.1%}")
    elif rebate_share >= 0.10:
        add("rebate_farmer", 1.5, f"返佣占经济 PnL {rebate_share:.1%}")
    if rebate_bps >= 30:
        add("rebate_farmer", 1.5, f"有效返佣 {rebate_bps:.1f} bps")
    if maker >= 0.85 and fpd >= 300:
        add("rebate_farmer", 1.0, "高周转 maker 结构")

    if two_sided >= 0.80:
        add("pair_arb", 2.0, f"{two_sided:.0%} 市场同时买 Up/Down")
    if pair_below >= 0.60:
        add("pair_arb", 3.0, f"{pair_below:.0%} 配对份额成本 < $1")
    elif pair_below >= 0.35:
        add("pair_arb", 1.0, f"{pair_below:.0%} 配对份额成本 < $1")
    if _safe(features.get("locked_profit")) > 0:
        add("pair_arb", 0.5, "存在锁定利润")

    if taker >= 0.50:
        add("directional_taker", 3.0, f"taker 占比 {taker:.1%}")
    if imbalance >= 0.60:
        add("directional_taker", 1.5, f"结果方向不平衡 {imbalance:.0%}")
    if two_sided < 0.30:
        add("directional_taker", 1.0, "很少双边建仓")

    if tail >= 0.55:
        add("long_tail_buyer", 5.0, f"{tail:.0%} 成交在 0.20 以下")
    elif tail >= 0.40:
        add("long_tail_buyer", 3.0, f"{tail:.0%} 成交在 0.20 以下")
    if hold_min >= 60 * 6:
        add("long_tail_buyer", 1.0, f"持仓中位 {hold_min / 60:.1f} 小时")
    if fpd < 100:
        add("long_tail_buyer", 1.0, "低频成交")

    if hold_min >= 360:
        add("swing", 2.0, f"持仓中位 {hold_min / 60:.1f} 小时")
    if 0 < fpd < 100:
        add("swing", 1.5, "低频、较长持有")
    if 0 < markets < 200:
        add("swing", 0.8, f"仅覆盖 {markets:.0f} 个市场")

    if not scores:
        scores["unclassified"] = 1.0
        reasons["unclassified"] = ["样本不足，暂未形成稳定画像"]

    ordered = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    primary, top_score = ordered[0]
    total = sum(scores.values()) or 1.0
    tags = [name for name, score in ordered if score >= 1.5]
    labels = {
        "hft_maker": "HFT 做市",
        "rebate_farmer": "返佣型",
        "pair_arb": "配对套利",
        "directional_taker": "方向性 taker",
        "long_tail_buyer": "长尾买入",
        "swing": "波段",
        "unclassified": "未分类",
    }
    evidence = []
    for name, score in ordered:
        if score < 1.0:
            continue
        evidence.append(
            {
                "label": labels.get(name, name),
                "score": round(score, 2),
                "reasons": reasons.get(name, []),
            }
        )
    return {
        "classification": primary,
        "classification_label": labels.get(primary, primary),
        "confidence": min(0.95, top_score / total + 0.15),
        "scores": {name: round(score, 2) for name, score in ordered},
        "tags": tags,
        "tag_labels": [labels.get(name, name) for name in tags],
        "evidence": evidence,
        "verdict": _verdict(primary, features),
    }


def _verdict(primary: str, features: dict[str, Any]) -> str:
    maker = _safe(features.get("maker_share"))
    edge = _safe(features.get("entry_edge_mean")) * 100
    end = _safe(features.get("markout_end")) * 100
    rebate = max(
        _safe(features.get("rebate_share_of_economic")),
        _safe(features.get("rebate_share_lifetime")),
    )
    pair = _safe(features.get("weighted_pair_cost"))
    two_sided = _safe(features.get("two_sided_ratio"))
    if primary in ("hft_maker", "rebate_farmer"):
        return (
            f"以 maker 挂单为主（{maker:.1%}），成交价相对 mid 平均改善 {edge:+.2f}¢，"
            f"持有到结算的 markout {end:+.2f}¢，返佣占经济 PnL {rebate:.0%}。"
            f"双边成交市场占比 {two_sided:.0%}，加权配对成本 {pair:.3f}。"
            "核心不是方向预测，而是「低成交价 + 结算赔率差 + 返佣」的组合。"
        )
    if primary == "pair_arb":
        return (
            f"双边成交占比 {two_sided:.0%}，配对成本 {pair:.3f}；"
            "收益主要来自配对锁定与结算，而不是单边方向。"
        )
    if primary == "directional_taker":
        return "以主动吃单为主，需重点看方向暴露、止损与短期 markout。"
    if primary == "long_tail_buyer":
        return "集中在低价尾部合约，收益依赖少数大赔率事件，需控制容量与尾部风险。"
    if primary == "swing":
        return "低频、较长持有，收益来自跨窗口的赔率变化。"
    return "样本不足，建议扩大时间窗后重新生成画像。"


def generate_profile(conn: sqlite3.Connection, address: str, days: int = 7) -> dict[str, Any]:
    features = compute_features(conn, address, days)
    result = classify(features)
    insights = build_insights(features)
    conn.execute(
        """INSERT OR REPLACE INTO profiles(
              address, generated_at, classification, confidence, tags_json,
              evidence_json, features_json, cluster_id, cluster_label)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        (
            address,
            now_ts(),
            result["classification_label"],
            result["confidence"],
            to_json(result["tag_labels"]),
            to_json([{"label": e["label"], "score": e["score"], "reasons": e["reasons"]} for e in result["evidence"]]),
            to_json(
                {
                    "features": features,
                    "scores": result["scores"],
                    "verdict": result["verdict"],
                    "insights": insights,
                }
            ),
            None,
            None,
        ),
    )
    conn.commit()
    return {"features": features, "insights": insights, **result}


def cluster_wallets(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Cluster tracked wallets on their behaviour features.

    Uses KMeans when scikit-learn is available and there are enough wallets,
    otherwise falls back to a deterministic rule label. Cluster labels are the
    most common primary classification inside the cluster.
    """
    rows = conn.execute(
        """SELECT p.address, p.classification, p.features_json, w.label
           FROM profiles p JOIN wallets w ON w.address=p.address
           WHERE w.tracked=1"""
    ).fetchall()
    if not rows:
        return []
    keys = [
        "maker_share",
        "taker_share",
        "fills_per_day",
        "two_sided_ratio",
        "weighted_pair_cost",
        "entry_edge_mean",
        "markout_end",
        "rebate_share_of_economic",
        "median_hold_min",
        "long_tail_share",
    ]
    vectors: list[list[float]] = []
    metas: list[dict[str, Any]] = []
    for row in rows:
        payload = from_json(row["features_json"], {}) or {}
        features = payload.get("features") or {}
        vector = []
        for key in keys:
            value = features.get(key)
            if value is None or (isinstance(value, float) and (value != value)):
                value = 0.0
            vector.append(float(value))
        vectors.append(vector)
        metas.append(
            {
                "address": row["address"],
                "label": row["label"],
                "classification": row["classification"],
            }
        )
    labels: list[int] = [0] * len(vectors)
    if len(vectors) >= 3:
        try:
            import numpy as np
            from sklearn.cluster import KMeans

            matrix = np.asarray(vectors, dtype=float)
            std = matrix.std(axis=0)
            std[std == 0] = 1.0
            normalized = (matrix - matrix.mean(axis=0)) / std
            n_clusters = min(3, len(vectors))
            model = KMeans(n_clusters=n_clusters, n_init=10, random_state=7)
            labels = model.fit_predict(normalized).tolist()
        except Exception:
            labels = [0] * len(vectors)
    clusters: dict[int, list[int]] = {}
    for index, label in enumerate(labels):
        clusters.setdefault(int(label), []).append(index)
    out = []
    for label, indexes in clusters.items():
        counts = {}
        for index in indexes:
            counts[metas[index]["classification"]] = counts.get(metas[index]["classification"], 0) + 1
        cluster_label = max(counts.items(), key=lambda item: item[1])[0] if counts else "Unclassified"
        for index in indexes:
            conn.execute(
                "UPDATE profiles SET cluster_id=?, cluster_label=? WHERE address=?",
                (int(label), cluster_label, metas[index]["address"]),
            )
            out.append(
                {
                    **metas[index],
                    "cluster_id": int(label),
                    "cluster_label": cluster_label,
                }
            )
    conn.commit()
    return out
