"""Automated research pipeline.

ingest latest 7 days -> run 3 auto-refined sweep rounds -> save the best
configuration -> activate it for forward paper trading.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from ..config import get_settings
from ..db import connect, now_ts
from .ingest import ingest_wallet
from .paper import activate_paper_strategy
from .sweep import run_auto_sweep, save_strategy

ProgressFn = Callable[[int, int, str], None]


def run_pipeline(
    job_id: str,
    address: str,
    params: dict[str, Any] | None = None,
    progress: ProgressFn | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    params = dict(params or {})
    days = int(params.get("days", settings.auto_pipeline_days) or settings.auto_pipeline_days)
    rounds = int(params.get("auto_rounds", settings.auto_pipeline_rounds) or settings.auto_pipeline_rounds)
    top_k = int(params.get("top_k", settings.auto_pipeline_top_k) or settings.auto_pipeline_top_k)
    max_cost = float(
        params.get("max_cost_per_market", settings.auto_pipeline_max_cost_per_market)
        or settings.auto_pipeline_max_cost_per_market
    )
    max_open = float(
        params.get("max_open_cost", settings.auto_pipeline_max_open_cost)
        or settings.auto_pipeline_max_open_cost
    )
    starting_balance = float(params.get("starting_balance", settings.paper_starting_balance))

    # 0-45: ingest
    conn = connect()
    try:
        if progress:
            progress(1, 100, "pipeline: ingest")
        ingest_summary = ingest_wallet(
            conn,
            address,
            days=days,
            max_trades=params.get("max_trades") or settings.ingest_max_trades,
            enrich=True,
            enrich_max_trades=params.get("enrich_max_trades") or settings.enrich_max_trades,
            enrich_days=settings.enrich_days,
            progress=(lambda done, total, msg: progress(int(done * 0.45), 100, f"ingest: {msg}")) if progress else None,
        )
    finally:
        conn.close()

    # 45-90: auto sweep
    sweep_params = {
        "days": days,
        "asset": "btc",
        "period": "5m",
        "offsets": params.get("offsets") or [0.002, 0.003, 0.005, 0.008],
        "window_sets": params.get("window_sets") or ["all", "target", "early", "mid", "late"],
        "bands": params.get("bands") or [[0.30, 0.90], [0.50, 0.90]],
        "sizes": params.get("sizes") or [5.0, 10.0, 20.0],
        "rebates": params.get("rebates") or [0.0, 45.0],
        "fee_bps": params.get("fee_bps", 0.0),
        "max_cost_per_market": max_cost,
        "max_open_cost": max_open,
        "min_val_fills": params.get("min_val_fills", 10),
        "auto_rounds": rounds,
        "top_k": top_k,
    }
    sweep_result = run_auto_sweep(
        job_id=f"{job_id}-s",
        params=sweep_params,
        progress=(
            (lambda done, total, msg: progress(45 + int(done * 0.45), 100, f"sweep: {msg}"))
            if progress
            else None
        ),
    )
    sweep_summary = sweep_result.get("summary") or {}
    best = sweep_summary.get("best") or {}
    saved_strategy_id = None
    paper_strategy_id = None
    if best and best.get("params") and (best.get("score") or -999) > -900:
        name = f"auto-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M')}"
        metrics = {
            key: best.get(key)
            for key in (
                "pnl",
                "roi",
                "fills",
                "win_rate",
                "max_drawdown",
                "train_pnl",
                "train_roi",
                "train_fills",
                "val_pnl",
                "val_roi",
                "val_fills",
                "val_win_rate",
            )
        }
        saved_strategy_id = save_strategy(
            name,
            best["params"].get("family", "mid_maker"),
            best["params"],
            metrics,
            best.get("run_id"),
        )
        paper_params = dict(best["params"])
        paper_params.update(
            {
                "max_cost_per_market": max_cost,
                "max_open_cost": max_open,
                "starting_balance": starting_balance,
            }
        )
        paper_strategy_id = activate_paper_strategy(
            f"{name} · paper",
            paper_params,
            source_run=best.get("run_id"),
            starting_balance=starting_balance,
        )
    if progress:
        progress(100, 100, "pipeline complete")
    return {
        "address": address,
        "days": days,
        "rounds": sweep_summary.get("rounds"),
        "best": best,
        "ingest": {
            "fills": ingest_summary.get("fills"),
            "pairs": ingest_summary.get("pairs"),
            "profile": (ingest_summary.get("profile") or {}).get("classification_label"),
        },
        "saved_strategy_id": saved_strategy_id,
        "paper_strategy_id": paper_strategy_id,
        "completed_at": now_ts(),
    }
