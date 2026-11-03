from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

from .config import get_settings
from .db import connect, ensure_target_wallet, get_state, init_db
from .routers import actions, api, pages
from .services.collector import manager as collector_manager
from .services.demo import seed_demo
from .services.jobs import JobWorker, reset_stale_jobs
from .services.monitor import monitor_all_tracked

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("polytrader")

settings = get_settings()
worker = JobWorker()
_monitor_task: asyncio.Task | None = None


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
    # A previously toggled state wins over the environment default.
    state = get_state("collector_enabled")
    if state is not None:
        collector_manager._enabled = state == "1"
    else:
        collector_manager._enabled = settings.collector_enabled
    collector_manager.start()
    global _monitor_task
    if settings.monitor_interval_hours > 0:
        _monitor_task = asyncio.create_task(_monitor_loop(), name="monitor-loop")
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
        await collector_manager.stop()
        worker.stop()


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url=None,
    openapi_url="/api/openapi.json",
)


@app.middleware("http")
async def basic_auth(request: Request, call_next):
    """Optional HTTP Basic Auth for the public deployment.

    ``/api/health`` stays open so Coolify/Docker health checks keep working.
    """
    if (
        settings.basic_auth_user
        and settings.basic_auth_password
        and request.url.path != "/api/health"
    ):
        header = request.headers.get("authorization", "")
        if not header.startswith("Basic "):
            return Response(
                status_code=401,
                content="Authentication required",
                headers={"WWW-Authenticate": 'Basic realm="Trader Strategy Lab"'},
            )
        try:
            decoded = base64.b64decode(header.removeprefix("Basic ")).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            decoded = ""
        expected = f"{settings.basic_auth_user}:{settings.basic_auth_password}"
        if not hmac.compare_digest(decoded, expected):
            return Response(
                status_code=401,
                content="Invalid credentials",
                headers={"WWW-Authenticate": 'Basic realm="Trader Strategy Lab"'},
            )
    return await call_next(request)


app.mount("/static", StaticFiles(directory=str(settings.static_dir)), name="static")
app.include_router(pages.router)
app.include_router(actions.router)
app.include_router(api.router)
