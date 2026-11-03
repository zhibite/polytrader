from __future__ import annotations

import sqlite3
from typing import Any

from ..db import connect, from_json, now_ts, to_json
from .metrics import build_features


def capture_monitor(address: str, days: int = 7) -> dict[str, Any]:
    address = address.lower()
    conn = connect()
    try:
        features = build_features(conn, address, days)
        conn.execute(
            """INSERT INTO monitor_snapshots(address, captured_at, window_days, metrics_json)
               VALUES(?,?,?,?)""",
            (address, now_ts(), days, to_json(features)),
        )
        conn.commit()
    finally:
        conn.close()
    return {
        "address": address,
        "captured_at": now_ts(),
        "fills": (features.get("overview") or {}).get("fills"),
        "entry_edge": (features.get("execution") or {}).get("entry_edge_mean"),
        "markout_end": (features.get("execution") or {}).get("markout_end_mean"),
        "rebate": (features.get("rebates") or {}).get("maker_rebate"),
    }


def monitor_all_tracked(days: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        addresses = [
            row["address"]
            for row in conn.execute("SELECT address FROM wallets WHERE tracked=1").fetchall()
        ]
    finally:
        conn.close()
    results = []
    for address in addresses:
        try:
            results.append(capture_monitor(address, days or 7))
        except Exception as exc:
            results.append({"address": address, "error": str(exc)})
    return results


def snapshot_rows(conn: sqlite3.Connection, address: str, limit: int = 60) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT * FROM monitor_snapshots WHERE address=?
           ORDER BY captured_at DESC LIMIT ?""",
        (address, limit),
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["metrics"] = from_json(item.get("metrics_json"), {}) or {}
        out.append(item)
    return out


def _metric_value(metrics: dict[str, Any], path: str, default: float | None = None) -> float | None:
    current: Any = metrics
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    try:
        return float(current)
    except (TypeError, ValueError):
        return default


def drift(conn: sqlite3.Connection, address: str) -> dict[str, Any]:
    rows = snapshot_rows(conn, address, limit=2)
    if len(rows) < 2:
        return {"ok": False, "message": "至少需要两次快照才能比较", "alerts": []}
    latest, previous = rows[0], rows[1]
    latest_metrics = latest["metrics"]
    previous_metrics = previous["metrics"]
    keys = [
        ("overview.fills", "成交笔数"),
        ("overview.volume_usdc", "成交额"),
        ("execution.entry_edge_mean", "平均 entry edge"),
        ("execution.markout_60s_mean", "+60s markout"),
        ("execution.markout_end_mean", "结算 markout"),
        ("pairing.weighted_pair_cost", "加权配对成本"),
        ("pairing.two_sided_ratio", "双边市场占比"),
        ("rebates.maker_rebate", "maker 返佣"),
        ("rebates.rebate_share_of_economic", "返佣/PnL"),
    ]
    comparisons = []
    alerts = []
    for path, label in keys:
        new = _metric_value(latest_metrics, path)
        old = _metric_value(previous_metrics, path)
        delta = (new - old) if new is not None and old is not None else None
        pct = (delta / abs(old)) if delta is not None and old not in (None, 0) else None
        comparisons.append(
            {
                "path": path,
                "label": label,
                "latest": new,
                "previous": old,
                "delta": delta,
                "pct": pct,
            }
        )
        if pct is None:
            continue
        if path in ("execution.entry_edge_mean", "execution.markout_end_mean", "execution.markout_60s_mean") and pct < -0.25:
            alerts.append(f"{label} 下降 {abs(pct):.0%}")
        if path == "pairing.weighted_pair_cost" and pct > 0.02:
            alerts.append(f"{label} 上升 {pct:.1%}")
        if path == "rebates.rebate_share_of_economic" and pct < -0.20:
            alerts.append(f"{label} 下降 {abs(pct):.0%}")
        if path == "overview.fills" and pct < -0.50:
            alerts.append(f"{label} 下降 {abs(pct):.0%}")
    return {
        "ok": True,
        "latest": latest,
        "previous": previous,
        "comparisons": comparisons,
        "alerts": alerts,
    }
