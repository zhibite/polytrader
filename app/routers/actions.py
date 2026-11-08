from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

from ..db import connect, now_ts, set_state
from ..services.collector import manager
from ..services.jobs import create_job

router = APIRouter()


def _redirect(request: Request, path: str | None = None, job: str | None = None) -> RedirectResponse:
    target = path or request.headers.get("referer") or "/"
    if job:
        separator = "&" if "?" in target else "?"
        target = f"{target}{separator}job={job}"
    return RedirectResponse(target, status_code=303)


@router.post("/actions/wallet")
def add_wallet(
    request: Request,
    address: Annotated[str, Form()],
    label: Annotated[str, Form()] = "",
    days: Annotated[int, Form()] = 7,
):
    address = address.strip().lower()
    if not address.startswith("0x") or len(address) != 42:
        return _redirect(request, "/wallets")
    conn = connect()
    try:
        conn.execute(
            """INSERT INTO wallets(address, label, source, notes, tracked, created_at, updated_at)
               VALUES(?,?, 'manual', '', 1, ?, ?)
               ON CONFLICT(address) DO UPDATE SET label=excluded.label, tracked=1,
                                                  updated_at=excluded.updated_at""",
            (address, label.strip() or address[:10], now_ts(), now_ts()),
        )
        conn.commit()
    finally:
        conn.close()
    job = create_job("ingest", address, {"days": days, "enrich": True})
    return _redirect(request, f"/wallets/{address}", job)


@router.post("/actions/ingest/{address}")
def ingest_wallet(
    request: Request,
    address: str,
    days: Annotated[int, Form()] = 7,
    max_trades: Annotated[int | None, Form()] = None,
    enrich_max_trades: Annotated[int | None, Form()] = None,
    enrich: Annotated[str | None, Form()] = "on",
):
    params = {
        "days": days,
        "enrich": enrich in ("on", "1", "true", "yes"),
    }
    if max_trades:
        params["max_trades"] = max_trades
    if enrich_max_trades:
        params["enrich_max_trades"] = enrich_max_trades
    job = create_job("ingest", address.lower(), params)
    return _redirect(request, f"/wallets/{address.lower()}", job)


@router.post("/actions/ingest-all")
def ingest_all(request: Request, days: Annotated[int, Form()] = 7):
    conn = connect()
    try:
        addresses = [
            row["address"] for row in conn.execute("SELECT address FROM wallets WHERE tracked=1").fetchall()
        ]
    finally:
        conn.close()
    jobs = [create_job("ingest", address, {"days": days, "enrich": True}) for address in addresses]
    return _redirect(request, "/wallets", jobs[0] if jobs else None)


@router.post("/actions/discover")
def refresh_leaderboard(
    request: Request,
    period: Annotated[str, Form()] = "day",
    category: Annotated[str, Form()] = "overall",
    sort_by: Annotated[str, Form()] = "PNL",
):
    job = create_job(
        "discover",
        None,
        {"time_periods": [period], "categories": [category], "sort_by": sort_by, "limit": 100},
    )
    return _redirect(request, f"/discover?period={period}&category={category}&sort_by={sort_by}", job)


@router.post("/actions/discover-all")
def refresh_all_leaderboards(request: Request):
    job = create_job("discover", None, {"limit": 100})
    return _redirect(request, "/discover", job)


@router.post("/actions/profile/{address}")
def regenerate_profile(request: Request, address: str, days: Annotated[int, Form()] = 7):
    job = create_job("profile", address.lower(), {"days": days})
    return _redirect(request, f"/wallets/{address.lower()}?days={days}", job)


@router.post("/actions/sweep")
def create_sweep(
    request: Request,
    days: Annotated[int, Form()] = 7,
    offsets: Annotated[list[str] | None, Form()] = None,
    window_sets: Annotated[list[str] | None, Form()] = None,
    bands: Annotated[list[str] | None, Form()] = None,
    sizes: Annotated[list[str] | None, Form()] = None,
    rebates: Annotated[list[str] | None, Form()] = None,
    fee_bps: Annotated[float, Form()] = 0.0,
    max_cost_per_market: Annotated[float, Form()] = 25.0,
    max_open_cost: Annotated[float, Form()] = 500.0,
    min_val_fills: Annotated[int, Form()] = 10,
    auto_rounds: Annotated[int, Form()] = 3,
    top_k: Annotated[int, Form()] = 8,
):
    def _floats(values: list[str] | None, default: list[float]) -> list[float]:
        if not values:
            return default
        out = []
        for value in values:
            try:
                out.append(float(value))
            except (TypeError, ValueError):
                continue
        return out or default

    band_values = []
    for raw in bands or []:
        try:
            low, high = str(raw).split("-", 1)
            band_values.append([float(low), float(high)])
        except (TypeError, ValueError):
            continue
    params = {
        "days": days,
        "offsets": _floats(offsets, [0.002, 0.003, 0.005, 0.008]),
        "window_sets": window_sets or ["all", "target", "early", "mid", "late"],
        "bands": band_values or [[0.30, 0.90], [0.50, 0.90]],
        "sizes": _floats(sizes, [5.0, 10.0, 20.0]),
        "rebates": _floats(rebates, [0.0, 45.0]),
        "fee_bps": fee_bps,
        "max_cost_per_market": max_cost_per_market,
        "max_open_cost": max_open_cost,
        "min_val_fills": min_val_fills,
        "auto_rounds": max(1, auto_rounds),
        "top_k": max(1, top_k),
    }
    job = create_job("sweep", None, params)
    return _redirect(request, "/lab", job)


@router.post("/actions/pipeline")
def run_pipeline_now(request: Request):
    from ..config import get_settings

    settings = get_settings()
    job = create_job(
        "pipeline",
        settings.target_address,
        {
            "days": settings.auto_pipeline_days,
            "auto_rounds": settings.auto_pipeline_rounds,
            "top_k": settings.auto_pipeline_top_k,
            "max_cost_per_market": settings.auto_pipeline_max_cost_per_market,
            "max_open_cost": settings.auto_pipeline_max_open_cost,
            "starting_balance": settings.paper_starting_balance,
        },
    )
    set_state("last_auto_pipeline_ts", str(now_ts()))
    return _redirect(request, "/lab", job)


@router.post("/actions/strategies")
def save_strategy_action(
    request: Request,
    name: Annotated[str, Form()] = "",
    run_id: Annotated[str, Form()] = "",
    combo_id: Annotated[str, Form()] = "",
):
    from ..services.sweep import save_strategy

    conn = connect()
    try:
        row = conn.execute(
            "SELECT * FROM sweep_results WHERE run_id=? AND combo_id=?",
            (run_id, combo_id),
        ).fetchone()
        if not row:
            return _redirect(request, f"/lab/{run_id}")
        import json

        params = json.loads(row["params_json"] or "{}")
        metrics = {
            "pnl": row["pnl"],
            "roi": row["roi"],
            "fills": row["fills"],
            "win_rate": row["win_rate"],
            "max_drawdown": row["max_drawdown"],
            "train_pnl": row["train_pnl"],
            "train_roi": row["train_roi"],
            "train_fills": row["train_fills"],
            "val_pnl": row["val_pnl"],
            "val_roi": row["val_roi"],
            "val_fills": row["val_fills"],
            "val_win_rate": row["val_win_rate"],
        }
        save_strategy(
            name.strip() or f"strategy-{combo_id[:24]}",
            row["family"] or "mid_maker",
            params,
            metrics,
            run_id,
        )
    finally:
        conn.close()
    return _redirect(request, f"/lab/{run_id}")


@router.post("/actions/live/reconcile")
async def live_reconcile(request: Request):
    from ..services.live import LiveError, executor

    try:
        await executor.reconcile()
        return _redirect(request, "/execution?live_msg=reconciled")
    except LiveError as exc:
        from urllib.parse import quote

        return _redirect(request, f"/execution?live_error={quote(str(exc))}")


@router.post("/actions/live/cancel-all")
async def live_cancel_all(request: Request):
    from ..services.live import LiveError, executor

    try:
        await executor.cancel_all(reason="manual")
        return _redirect(request, "/execution?live_msg=cancelled")
    except LiveError as exc:
        from urllib.parse import quote

        return _redirect(request, f"/execution?live_error={quote(str(exc))}")


@router.post("/actions/live/kill")
async def live_kill(request: Request, reason: Annotated[str, Form()] = "manual"):
    from ..services.live import executor

    await executor.engage_kill(reason=reason or "manual")
    return _redirect(request, "/execution?live_msg=kill_switch_engaged")


@router.post("/actions/live/release")
def live_release(request: Request, confirm: Annotated[str, Form()] = ""):
    from ..services.live import KILL_SWITCH_RELEASE, LiveError, executor

    try:
        executor.release_kill(confirm)
        return _redirect(request, "/execution?live_msg=kill_switch_released")
    except LiveError as exc:
        from urllib.parse import quote

        return _redirect(request, f"/execution?live_error={quote(str(exc))}")


@router.post("/actions/monitor")
def monitor_now(request: Request, address: Annotated[str | None, Form()] = None):
    targets = [address.lower()] if address else []
    if targets:
        jobs = [create_job("monitor", target, {"days": 7}) for target in targets]
        return _redirect(request, f"/monitor?job={jobs[0]}")
    conn = connect()
    try:
        addresses = [
            row["address"] for row in conn.execute("SELECT address FROM wallets WHERE tracked=1").fetchall()
        ]
    finally:
        conn.close()
    jobs = [create_job("monitor", address, {"days": 7}) for address in addresses]
    return _redirect(request, "/monitor", jobs[0] if jobs else None)


@router.post("/actions/shadow")
def create_shadow(
    request: Request,
    address: Annotated[str, Form()],
    days: Annotated[int, Form()] = 7,
    delay_s: Annotated[float, Form()] = 0.0,
    size_scale: Annotated[float, Form()] = 1.0,
    copy_sells: Annotated[str | None, Form()] = None,
):
    job = create_job(
        "shadow",
        address.lower(),
        {
            "days": days,
            "delay_s": delay_s,
            "size_scale": size_scale,
            "copy_sells": copy_sells in ("on", "1", "true", "yes"),
        },
    )
    return _redirect(request, "/shadow", job)


@router.post("/actions/replica")
def create_replica(
    request: Request,
    strategy: Annotated[str, Form()] = "mid_maker",
    days: Annotated[int, Form()] = 3,
    offset: Annotated[float, Form()] = 0.005,
    size: Annotated[float, Form()] = 10.0,
    min_price: Annotated[float, Form()] = 0.30,
    max_price: Annotated[float, Form()] = 0.90,
    fee_bps: Annotated[float, Form()] = 0.0,
    rebate_bps: Annotated[float, Form()] = 45.0,
    both_sides: Annotated[str | None, Form()] = "on",
    allowed_windows: Annotated[list[str] | None, Form()] = None,
):
    params = {
        "days": days,
        "offset": offset,
        "size": size,
        "min_price": min_price,
        "max_price": max_price,
        "fee_bps": fee_bps,
        "rebate_bps": rebate_bps,
        "both_sides": both_sides in ("on", "1", "true", "yes"),
        "allowed_windows": allowed_windows or [],
    }
    job = create_job("replica", None, params)
    return _redirect(request, "/replica", job)


@router.post("/actions/collector/toggle")
def toggle_collector(request: Request, action: Annotated[str, Form()] = "toggle"):
    current = manager.enabled
    if action == "start":
        manager.set_enabled(True)
    elif action == "stop":
        manager.set_enabled(False)
    else:
        manager.set_enabled(not current)
    return _redirect(request, "/collector")


@router.post("/actions/collector/restart")
def restart_collector(request: Request):
    return _redirect(request, "/collector")
