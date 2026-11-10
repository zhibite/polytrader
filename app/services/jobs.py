from __future__ import annotations

import json
import secrets
import sqlite3
import threading
import time
from typing import Any, Callable

from ..db import connect, from_json, now_ts, to_json


class JobWorker:
    """Single-process SQLite-backed job worker.

    The app is deployed with one uvicorn worker on purpose: Polymarket rate
    limits are shared and the ingest jobs are I/O bound, so a single serial
    worker keeps the crawler polite and the progress state consistent.
    """

    def __init__(self, poll_seconds: float = 2.0) -> None:
        self.poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._current_job: str | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="job-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    @property
    def current_job(self) -> str | None:
        return self._current_job

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                job = claim_next_job()
                if job:
                    self._current_job = job["id"]
                    run_job(job)
                    self._current_job = None
                else:
                    self._stop.wait(self.poll_seconds)
            except Exception:
                # Never let the worker thread die; the job error is recorded
                # in run_job, this is only a last-resort guard.
                self._current_job = None
                self._stop.wait(max(2.0, self.poll_seconds))


def create_job(kind: str, address: str | None, params: dict[str, Any] | None = None) -> str:
    job_id = f"job_{secrets.token_hex(8)}"
    conn = connect()
    try:
        conn.execute(
            """INSERT INTO jobs(id, kind, address, status, params_json, progress, total, done,
                                message, created_at)
               VALUES(?,?,?, 'QUEUED', ?, 0, 0, 0, ?, ?)""",
            (job_id, kind, (address or "").lower() or None, to_json(params or {}), "queued", now_ts()),
        )
        conn.commit()
    finally:
        conn.close()
    return job_id


def claim_next_job() -> dict[str, Any] | None:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT * FROM jobs WHERE status='QUEUED' ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE jobs SET status='RUNNING', started_at=?, message='starting' WHERE id=?",
            (now_ts(), row["id"]),
        )
        conn.commit()
        job = dict(row)
        job["status"] = "RUNNING"
        return job
    finally:
        conn.close()


def update_job(job_id: str, **fields: Any) -> None:
    if not fields:
        return
    allowed = {
        "status",
        "progress",
        "total",
        "done",
        "message",
        "error",
        "started_at",
        "finished_at",
    }
    pairs = [(key, value) for key, value in fields.items() if key in allowed]
    if not pairs:
        return
    conn = connect()
    try:
        assignments = ", ".join(f"{key}=?" for key, _ in pairs)
        conn.execute(f"UPDATE jobs SET {assignments} WHERE id=?", [value for _, value in pairs] + [job_id])
        conn.commit()
    finally:
        conn.close()


def get_job(job_id: str) -> dict[str, Any] | None:
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_jobs(limit: int = 30, address: str | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        if address:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE address=? ORDER BY created_at DESC LIMIT ?",
                (address.lower(), limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def reset_stale_jobs() -> int:
    """Mark jobs left RUNNING by a previous process as ERROR."""
    conn = connect()
    try:
        cursor = conn.execute(
            """UPDATE jobs SET status='ERROR', error='interrupted by restart',
                               finished_at=?, message='interrupted'
               WHERE status IN ('RUNNING','QUEUED')""",
            (now_ts(),),
        )
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()


def _progress_callback(job_id: str) -> Callable[[int, int, str], None]:
    state = {"last": 0.0}

    def callback(done: int, total: int, message: str) -> None:
        now = time.monotonic()
        if now - state["last"] < 0.8 and done < total:
            return
        state["last"] = now
        progress = (done / total * 100.0) if total else 0.0
        update_job(
            job_id,
            progress=round(progress, 2),
            total=int(total),
            done=int(done),
            message=str(message)[:500],
        )

    return callback


def run_job(job: dict[str, Any]) -> None:
    job_id = job["id"]
    kind = job["kind"]
    address = job.get("address") or ""
    params = from_json(job.get("params_json"), {}) or {}
    update_job(job_id, status="RUNNING", message=f"running {kind}", error=None)
    try:
        if kind == "ingest":
            from .ingest import ingest_wallet

            conn = connect()
            try:
                raw_days = params.get("days")
                raw_max = params.get("max_trades")
                raw_enrich = params.get("enrich_max_trades")
                summary = ingest_wallet(
                    conn,
                    address,
                    days=int(raw_days) if raw_days is not None else None,
                    max_trades=int(raw_max) if raw_max is not None else None,
                    enrich=bool(params.get("enrich", True)),
                    enrich_max_trades=int(raw_enrich) if raw_enrich is not None else None,
                    enrich_days=int(params.get("enrich_days", 14)),
                    progress=_progress_callback(job_id),
                )
            finally:
                conn.close()
            profile = summary.get("profile") or {}
            update_job(
                job_id,
                status="DONE",
                progress=100,
                message=(
                    f"ingest 完成：fills={summary.get('fills', 0):,}, "
                    f"pairs={summary.get('pairs', 0):,}, "
                    f"profile={profile.get('classification_label', '—')}"
                ),
                finished_at=now_ts(),
            )
            return
        if kind == "discover":
            from .discovery import sync_leaderboards

            result = sync_leaderboards(
                time_periods=params.get("time_periods"),
                categories=params.get("categories"),
                sort_by=params.get("sort_by"),
                limit=int(params.get("limit", 100)),
                progress=_progress_callback(job_id),
            )
            update_job(
                job_id,
                status="DONE",
                progress=100,
                message=f"leaderboard rows: {result.get('rows', 0)}",
                finished_at=now_ts(),
            )
            return
        if kind == "shadow":
            from .shadow import run_shadow

            result = run_shadow(
                address,
                days=int(params.get("days", 7)),
                delay_s=float(params.get("delay_s", 0)),
                size_scale=float(params.get("size_scale", 1.0)),
                copy_sells=bool(params.get("copy_sells", False)),
                run_id=job_id,
                progress=_progress_callback(job_id),
            )
            summary = result.get("summary") or {}
            update_job(
                job_id,
                status="DONE",
                progress=100,
                message=(
                    f"Shadow 完成：copied={summary.get('copied', 0):,}, "
                    f"PnL=${summary.get('pnl', 0):,.2f}, "
                    f"ROI={(summary.get('roi') or 0) * 100:.2f}%; run=/shadow/{job_id}"
                ),
                finished_at=now_ts(),
            )
            return
        if kind == "replica":
            from .replica import run_replica

            result = run_replica(
                strategy=params.get("strategy", "mid_maker"),
                days=int(params.get("days", 3)),
                params=params,
                run_id=job_id,
                progress=_progress_callback(job_id),
            )
            summary = result.get("summary") or {}
            update_job(
                job_id,
                status="DONE",
                progress=100,
                message=(
                    f"Replica 完成：trades={summary.get('trades', 0):,}, "
                    f"PnL=${summary.get('pnl', 0):,.2f}, "
                    f"ROI={(summary.get('roi') or 0) * 100:.2f}%; run=/replica/{job_id}"
                ),
                finished_at=now_ts(),
            )
            return
        if kind == "pipeline":
            from .pipeline import run_pipeline

            result = run_pipeline(
                job_id=job_id,
                address=address,
                params=params,
                progress=_progress_callback(job_id),
            )
            best = result.get("best") or {}
            raw_best = result.get("raw_best") or {}
            decision_reason = ((result.get("paper_decision") or {}).get("reason") or "")[:60]
            if result.get("gate_rejected"):
                message = (
                    f"Pipeline 完成：fills={((result.get('ingest') or {}).get('fills') or 0):,}, "
                    f"no candidate passed gates "
                    f"(raw val ROI={(raw_best.get('val_roi') or 0) * 100:.2f}%); "
                    f"paper={result.get('paper_strategy_id') or '—'} "
                    f"({result.get('paper_action') or 'flat'}: {decision_reason}); "
                    f"run=/lab/{raw_best.get('run_id') or '—'}"
                )
            else:
                message = (
                    f"Pipeline 完成：fills={((result.get('ingest') or {}).get('fills') or 0):,}, "
                    f"best val ROI={(best.get('val_roi') or 0) * 100:.2f}%, "
                    f"paper={result.get('paper_strategy_id') or '—'} "
                    f"({result.get('paper_action') or '—'}: {decision_reason}); "
                    f"run=/lab/{best.get('run_id') or '—'}"
                )
            update_job(
                job_id,
                status="DONE",
                progress=100,
                message=message,
                finished_at=now_ts(),
            )
            return
        if kind == "sweep":
            auto_rounds = int(params.get("auto_rounds", 1) or 1)
            if auto_rounds > 1:
                from .sweep import run_auto_sweep

                result = run_auto_sweep(job_id=job_id, params=params, progress=_progress_callback(job_id))
                summary = result.get("summary") or {}
                best = summary.get("best") or {}
                best_run = best.get("run_id") or job_id
                update_job(
                    job_id,
                    status="DONE",
                    progress=100,
                    message=(
                        f"AutoSweep 完成：rounds={summary.get('rounds', 0)}, "
                        f"best round={best.get('round', '—')}, "
                        f"val ROI={(best.get('val_roi') or 0) * 100:.2f}%, "
                        f"val PnL=${best.get('val_pnl', 0):,.2f}; run=/lab/{best_run}"
                    ),
                    finished_at=now_ts(),
                )
                return
            from .sweep import run_sweep

            result = run_sweep(run_id=job_id, params=params, progress=_progress_callback(job_id))
            summary = result.get("summary") or {}
            best = summary.get("best") or {}
            update_job(
                job_id,
                status="DONE",
                progress=100,
                message=(
                    f"Sweep 完成：combos={summary.get('combos', 0):,}, "
                    f"best val ROI={(best.get('val_roi') or 0) * 100:.2f}%, "
                    f"val PnL=${best.get('val_pnl', 0):,.2f}; run=/lab/{job_id}"
                ),
                finished_at=now_ts(),
            )
            return
        if kind == "monitor":
            from .monitor import capture_monitor

            result = capture_monitor(address, days=int(params.get("days", 7)))
            update_job(
                job_id,
                status="DONE",
                progress=100,
                message=(
                    f"monitor 完成：fills={result.get('fills', 0):,}, "
                    f"entry_edge={(result.get('entry_edge') or 0) * 100:.2f}¢, "
                    f"markout_end={(result.get('markout_end') or 0) * 100:.2f}¢"
                ),
                finished_at=now_ts(),
            )
            return
        if kind == "profile":
            from .profiles import generate_profile

            conn = connect()
            try:
                result = generate_profile(conn, address, days=int(params.get("days", 7)))
            finally:
                conn.close()
            update_job(
                job_id,
                status="DONE",
                progress=100,
                message=result.get("classification_label", "done"),
                finished_at=now_ts(),
            )
            return
        raise ValueError(f"unknown job kind: {kind}")
    except Exception as exc:
        update_job(
            job_id,
            status="ERROR",
            error=str(exc)[:1500],
            message=f"{kind} failed",
            finished_at=now_ts(),
        )
