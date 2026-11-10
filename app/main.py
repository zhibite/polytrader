from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from .auth import COOKIE_NAME, credentials_ok, parse_basic, read_session
from .config import get_settings
from .db import connect, ensure_target_wallet, get_state, init_db, now_ts, set_state
from .routers import actions, api, pages
from .routers import auth as auth_router
from .services.collector import manager as collector_manager
from .services.demo import seed_demo
from .services.jobs import JobWorker, create_job, reset_stale_jobs
from .services.monitor import monitor_all_tracked
from .services.paper import PaperWorker
from .services.champion import live_strategy_loop
from .services.live import LiveError, executor as live_executor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("polytrader")

settings = get_settings()
worker = JobWorker()
paper_worker = PaperWorker()
_monitor_task: asyncio.Task | None = None
_pipeline_task: asyncio.Task | None = None
_live_task: asyncio.Task | None = None
_champion_task: asyncio.Task | None = None


async def _live_loop() -> None:
    """Reconcile live orders/balances and enforce the kill switch.

    This loop never places orders. It only reconciles and cancels. It is
    started only when live execution is explicitly enabled.
    """
    while True:
        try:
            status = live_executor.configured_status()
            if status.get("credentials_ready"):
                await live_executor.reconcile()
                if status.get("kill_switch"):
                    await live_executor.cancel_all("kill_switch")
        except (LiveError, Exception) as exc:
            log.warning("live reconcile loop: %s", exc)
        await asyncio.sleep(20)


async def _pipeline_loop() -> None:
    """Queue ingest -> auto sweep -> paper activation every N hours."""
    interval = max(0.25, settings.auto_pipeline_hours) * 3600
    while True:
        try:
            now = now_ts()
            last = get_state("last_auto_pipeline_ts")
            last_ts = int(last) if last else 0
            if now - last_ts >= interval:
                conn = connect()
                try:
                    running = conn.execute(
                        "SELECT 1 FROM jobs WHERE kind='pipeline' AND status IN ('QUEUED','RUNNING') LIMIT 1"
                    ).fetchone()
                finally:
                    conn.close()
                if not running:
                    create_job(
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
                    set_state("last_auto_pipeline_ts", str(now))
                    log.info("auto pipeline queued")
        except Exception as exc:
            log.warning("auto pipeline scheduler failed: %s", exc)
        await asyncio.sleep(300)


async def _monitor_loop() -> None:
    interval = max(0.25, settings.monitor_interval_hours) * 3600
    while True:
        await asyncio.sleep(interval)
        try:
            await asyncio.to_thread(monitor_all_tracked, settings.monitor_window_days)
            log.info("monitor snapshots captured")
        except Exception as exc:  # keep the loop alive
            log.warning("monitor snapshot failed: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if settings.demo_seed:
        try:
            conn = connect()
            try:
                has_trades = conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"] > 0
            finally:
                conn.close()
            if not has_trades:
                log.info("seeding demo dataset")
                seeded = seed_demo(force=False)
                log.info("demo seed: %s", "created" if seeded else "skipped")
        except Exception as exc:
            log.warning("demo seed failed: %s", exc)
    ensure_target_wallet()
    reset_stale_jobs()
    worker.start()
    paper_worker.start()
    # A previously toggled state wins over the environment default.
    state = get_state("collector_enabled")
    if state is not None:
        collector_manager._enabled = state == "1"
    else:
        collector_manager._enabled = settings.collector_enabled
    collector_manager.start()
    global _monitor_task, _pipeline_task, _live_task, _champion_task
    if settings.monitor_interval_hours > 0:
        _monitor_task = asyncio.create_task(_monitor_loop(), name="monitor-loop")
    if settings.auto_pipeline_hours > 0:
        _pipeline_task = asyncio.create_task(_pipeline_loop(), name="pipeline-loop")
    if settings.live_enabled:
        _live_task = asyncio.create_task(_live_loop(), name="live-loop")
    if settings.live_enabled and settings.live_strategy_enabled:
        _champion_task = asyncio.create_task(live_strategy_loop(), name="champion-loop")
    log.info("%s %s ready", settings.app_name, settings.app_version)
    try:
        yield
    finally:
        if _monitor_task:
            _monitor_task.cancel()
            try:
                await _monitor_task
            except (asyncio.CancelledError, Exception):
                pass
        if _pipeline_task:
            _pipeline_task.cancel()
            try:
                await _pipeline_task
            except (asyncio.CancelledError, Exception):
                pass
        if _live_task:
            _live_task.cancel()
            try:
                await _live_task
            except (asyncio.CancelledError, Exception):
                pass
        if _champion_task:
            _champion_task.cancel()
            try:
                await _champion_task
            except (asyncio.CancelledError, Exception):
                pass
        await collector_manager.stop()
        paper_worker.stop()
        worker.stop()


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url=None,
    openapi_url="/api/openapi.json",
)


PUBLIC_PREFIXES = ("/static/",)
PUBLIC_PATHS = {"/login", "/logout", "/api/health", "/favicon.ico"}


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Session login for browsers, Basic Auth for scripts/API.

    ``/api/health`` stays open so Coolify/Docker health checks keep working.
    """
    if not settings.auth_enabled:
        return await call_next(request)
    path = request.url.path
    if path in PUBLIC_PATHS or any(path.startswith(prefix) for prefix in PUBLIC_PREFIXES):
        return await call_next(request)

    # Programmatic access keeps working with the same credentials.
    username, password = parse_basic(request.headers.get("authorization"))
    if credentials_ok(username, password):
        return await call_next(request)

    if read_session(request.cookies.get(COOKIE_NAME)):
        return await call_next(request)

    if path.startswith("/api/"):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    next_path = path
    if request.url.query:
        next_path = f"{next_path}?{request.url.query}"
    return RedirectResponse(f"/login?next={quote(next_path, safe='')}", status_code=303)


app.mount("/static", StaticFiles(directory=str(settings.static_dir)), name="static")
app.include_router(auth_router.router)
app.include_router(pages.router)
app.include_router(actions.router)
app.include_router(api.router)
