from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ..config import get_settings
from ..db import connect
from ..services import collector as collector_service
from ..services import discovery as discovery_service
from ..services import metrics
from ..services.jobs import get_job, list_jobs
from ..templating import templates

router = APIRouter(prefix="/api")
settings = get_settings()


@router.get("/health")
def health() -> dict[str, Any]:
    db_ok = True
    counts: dict[str, int] = {}
    try:
        conn = connect()
        try:
            for table in ("wallets", "trades", "jobs", "samples"):
                counts[table] = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
        finally:
            conn.close()
    except Exception:
        db_ok = False
    return {
        "ok": db_ok,
        "app": settings.app_name,
        "version": settings.app_version,
        "counts": counts,
        "collector": collector_service.collector_status(),
    }


@router.get("/jobs/{job_id}")
def job_status(job_id: str) -> JSONResponse:
    job = get_job(job_id)
    if not job:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    return JSONResponse({"ok": True, "job": job})


@router.get("/jobs")
def jobs(limit: int = 30, address: str | None = None) -> dict[str, Any]:
    return {"jobs": list_jobs(limit=limit, address=address)}


@router.get("/collector/status")
def collector_status() -> dict[str, Any]:
    return collector_service.collector_status()


@router.get("/collector/partial", response_class=HTMLResponse)
def collector_partial(request: Request):
    status = collector_service.collector_status()
    conn = connect()
    try:
        books = collector_service.recent_books(conn, 20)
        trades = collector_service.recent_trades(conn, 20)
    finally:
        conn.close()
    return templates.TemplateResponse(
        request=request,
        name="partials/collector_status.html",
        context={"request": request, "status": status, "books": books, "trades": trades},
    )


@router.get("/wallets/{address}/dossier")
def wallet_dossier(address: str, days: int = Query(7)) -> dict[str, Any]:
    conn = connect()
    try:
        dossier = metrics.dossier(conn, address.lower(), days)
    finally:
        conn.close()
    return dossier


@router.get("/leaderboard")
def leaderboard(
    period: str = Query("day"),
    category: str = Query("overall"),
    sort_by: str = Query("PNL"),
    limit: int = Query(100),
) -> dict[str, Any]:
    conn = connect()
    try:
        captured, rows = discovery_service.leaderboard_rows(conn, period, category, sort_by, limit)
    finally:
        conn.close()
    return {"captured_at": captured, "rows": rows}
