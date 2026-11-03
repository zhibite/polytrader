from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from ..config import get_settings
from ..db import connect, from_json
from ..services import discovery as discovery_service
from ..services import metrics
from ..services import collector as collector_service
from ..services import sweep as sweep_service
from ..services.jobs import get_job, list_jobs
from ..services.monitor import drift, snapshot_rows
from ..services.profiles import cluster_wallets
from ..services.replica import get_run as get_replica_run
from ..services.replica import list_runs as list_replica_runs
from ..services.shadow import get_run as get_shadow_run
from ..services.shadow import list_runs as list_shadow_runs
from ..templating import templates

router = APIRouter()
settings = get_settings()

NAV = [
    {"href": "/", "label": "总览", "icon": "grid"},
    {"href": "/discover", "label": "发现交易者", "icon": "compass"},
    {"href": "/wallets", "label": "钱包画像", "icon": "wallet"},
    {"href": "/compare", "label": "对比与聚类", "icon": "compare"},
    {"href": "/lab", "label": "策略实验室", "icon": "target"},
    {"href": "/shadow", "label": "Shadow 复刻", "icon": "layers"},
    {"href": "/replica", "label": "Replica 纸盘", "icon": "flask"},
    {"href": "/monitor", "label": "持续监控", "icon": "pulse"},
    {"href": "/collector", "label": "行情采集", "icon": "signal"},
]


def _ctx(request: Request, **extra: Any) -> dict[str, Any]:
    job_id = request.query_params.get("job")
    job = get_job(job_id) if job_id else None
    return {
        "request": request,
        "nav": NAV,
        "current_path": request.url.path,
        "job": job,
        "now": datetime.now(timezone.utc),
        **extra,
    }


def _render(request: Request, name: str, **extra: Any) -> HTMLResponse:
    return templates.TemplateResponse(request=request, name=name, context=_ctx(request, **extra))


def _wallet_rows(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT w.address, w.label, w.source, w.tracked, w.notes, w.created_at,
                  s.markets, s.fills, s.volume_usdc, s.realized_pnl, s.economic_pnl,
                  s.maker_rebate, s.fees, s.join_date,
                  p.classification, p.confidence, p.tags_json, p.cluster_label
           FROM wallets w
           LEFT JOIN wallet_stats s ON s.address=w.address
           LEFT JOIN profiles p ON p.address=w.address
           WHERE w.tracked=1
           ORDER BY COALESCE(s.volume_usdc,0) DESC, w.created_at ASC"""
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["tags"] = from_json(item.get("tags_json"), [])
        out.append(item)
    return out


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    conn = connect()
    try:
        wallets = _wallet_rows(conn)
        target = settings.target_address.lower()
        target_row = next((row for row in wallets if row["address"] == target), None)
        dossier = metrics.dossier(conn, target, 7) if target_row else None
        jobs = list_jobs(limit=8)
        _, leaderboard = discovery_service.leaderboard_rows(conn, "day", "overall", "PNL", 8)
        total_trades = conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
        total_wallets = conn.execute("SELECT COUNT(*) AS n FROM wallets WHERE tracked=1").fetchone()["n"]
        total_volume = conn.execute("SELECT COALESCE(SUM(volume_usdc),0) AS v FROM wallet_stats").fetchone()["v"]
        collector = collector_service.collector_status()
    finally:
        conn.close()
    chart = _chart_payload(dossier) if dossier else {}
    return _render(
        request,
        "dashboard.html",
        wallets=wallets,
        target=target_row,
        dossier=dossier,
        jobs=jobs,
        leaderboard=leaderboard,
        total_trades=total_trades,
        total_wallets=total_wallets,
        total_volume=total_volume,
        collector=collector,
        chart=chart,
    )


@router.get("/discover", response_class=HTMLResponse)
def discover(
    request: Request,
    period: str = Query("day"),
    category: str = Query("overall"),
    sort_by: str = Query("PNL"),
):
    conn = connect()
    try:
        captured, rows = discovery_service.leaderboard_rows(conn, period, category, sort_by, 100)
        combos = discovery_service.available_combos(conn)
        wallets = {row["address"] for row in _wallet_rows(conn)}
    finally:
        conn.close()
    return _render(
        request,
        "discover.html",
        rows=rows,
        captured=captured,
        combos=combos,
        period=period,
        category=category,
        sort_by=sort_by,
        wallets=wallets,
    )


@router.get("/wallets", response_class=HTMLResponse)
def wallets_page(request: Request):
    conn = connect()
    try:
        wallets = _wallet_rows(conn)
        jobs = list_jobs(limit=10)
    finally:
        conn.close()
    return _render(request, "wallets.html", wallets=wallets, jobs=jobs)


@router.get("/wallets/{address}", response_class=HTMLResponse)
def wallet_detail(request: Request, address: str, days: int = Query(7)):
    address = address.lower()
    if days not in (0, 1, 7, 30, 90):
        days = 7
    conn = connect()
    try:
        wallet = conn.execute("SELECT * FROM wallets WHERE address=?", (address,)).fetchone()
        if not wallet:
            wallet = {"address": address, "label": None, "tracked": 0}
        dossier = metrics.dossier(conn, address, days)
        trades = [
            dict(row)
            for row in conn.execute(
                """SELECT * FROM trades WHERE address=? ORDER BY ts DESC LIMIT 200""",
                (address,),
            ).fetchall()
        ]
        pairs = [
            dict(row)
            for row in conn.execute(
                """SELECT * FROM pair_markets WHERE address=?
                   ORDER BY (settlement_pnl IS NULL), ABS(COALESCE(settlement_pnl,0)) DESC LIMIT 25""",
                (address,),
            ).fetchall()
        ]
        jobs = list_jobs(limit=8, address=address)
        positions = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM positions WHERE address=? ORDER BY COALESCE(total_pnl,0) DESC LIMIT 20",
                (address,),
            ).fetchall()
        ]
    finally:
        conn.close()
    chart = _chart_payload(dossier)
    return _render(
        request,
        "wallet_detail.html",
        address=address,
        wallet=dict(wallet),
        dossier=dossier,
        chart=chart,
        trades=trades,
        pairs=pairs,
        jobs=jobs,
        positions=positions,
        days=days,
        days_options=[1, 7, 30, 90, 0],
    )


@router.get("/compare", response_class=HTMLResponse)
def compare(request: Request):
    conn = connect()
    try:
        wallets = _wallet_rows(conn)
        rows = []
        for wallet in wallets:
            profile = metrics.profile_row(conn, wallet["address"])
            features = (profile or {}).get("features", {}) or {}
            feature_values = features.get("features", {}) if isinstance(features, dict) else {}
            rows.append(
                {
                    "address": wallet["address"],
                    "label": wallet["label"],
                    "classification": wallet.get("classification"),
                    "confidence": wallet.get("confidence"),
                    "cluster": wallet.get("cluster_label"),
                    "features": feature_values,
                    "verdict": features.get("verdict") if isinstance(features, dict) else None,
                }
            )
        clusters = cluster_wallets(conn)
        scatter = [
            {
                "name": row["label"] or row["address"][:8],
                "address": row["address"],
                "x": (row["features"].get("maker_share") or 0) * 100,
                "y": (row["features"].get("entry_edge_mean") or 0) * 100,
                "r": min(40, max(6, (row["features"].get("fills_per_day") or 0) ** 0.5 / 3)),
                "classification": row["classification"],
            }
            for row in rows
        ]
        rebate_scatter = [
            {
                "name": row["label"] or row["address"][:8],
                "x": (row["features"].get("rebate_share_of_economic") or 0) * 100,
                "y": (row["features"].get("weighted_pair_cost") or 0),
                "r": min(40, max(6, (row["features"].get("volume_usdc") or 0) ** 0.5 / 250)),
                "classification": row["classification"],
            }
            for row in rows
        ]
    finally:
        conn.close()
    return _render(
        request,
        "compare.html",
        rows=rows,
        clusters=clusters,
        scatter=scatter,
        rebate_scatter=rebate_scatter,
    )


@router.get("/lab", response_class=HTMLResponse)
def lab_page(request: Request):
    conn = connect()
    try:
        runs = sweep_service.list_runs(conn)
        strategies = sweep_service.list_strategies(conn)
    finally:
        conn.close()
    return _render(request, "lab.html", runs=runs, strategies=strategies)


@router.get("/lab/{run_id}", response_class=HTMLResponse)
def lab_detail(request: Request, run_id: str):
    conn = connect()
    try:
        run = sweep_service.get_run(conn, run_id)
    finally:
        conn.close()
    if not run:
        return _render(request, "not_found.html", message="Sweep run 不存在")
    results = run.get("results") or []
    scatter = []
    for result in results:
        if result.get("train_roi") is None or result.get("val_roi") is None:
            continue
        params = result.get("params") or {}
        scatter.append(
            {
                "name": f"{params.get('offset')} / {params.get('window_set')} / {params.get('size')}",
                "x": (result["train_roi"] or 0) * 100,
                "y": (result["val_roi"] or 0) * 100,
                "r": 7,
                "classification": params.get("window_set") or "—",
            }
        )
    top = [result for result in results if result.get("val_pnl") is not None][:12]
    chart = {
        "scatter": scatter,
        "top": {
            "labels": [
                f"#{result['rank']} {result['params'].get('window_set')} {result['params'].get('size')}"
                for result in top
            ],
            "val_pnl": [result.get("val_pnl") or 0 for result in top],
            "val_roi": [(result.get("val_roi") or 0) * 100 for result in top],
        },
    }
    return _render(request, "lab_detail.html", run=run, summary=run.get("summary") or {}, results=results, chart=chart)


@router.get("/shadow", response_class=HTMLResponse)
def shadow_page(request: Request):
    conn = connect()
    try:
        runs = list_shadow_runs(conn)
        wallets = _wallet_rows(conn)
    finally:
        conn.close()
    return _render(request, "shadow.html", runs=runs, wallets=wallets)


@router.get("/shadow/{run_id}", response_class=HTMLResponse)
def shadow_detail(request: Request, run_id: str):
    conn = connect()
    try:
        run = get_shadow_run(conn, run_id)
    finally:
        conn.close()
    if not run:
        return _render(request, "not_found.html", message="Shadow run 不存在")
    summary = run.get("summary") or {}
    chart = {
        "by_day": {
            "labels": [item["day"] for item in summary.get("by_day", [])],
            "pnl": [item["pnl"] for item in summary.get("by_day", [])],
        },
        "by_bucket": {
            "labels": [item["bucket"] for item in summary.get("by_bucket", [])],
            "pnl": [item["pnl"] for item in summary.get("by_bucket", [])],
        },
        "by_window": {
            "labels": [item["window"] for item in summary.get("by_window", [])],
            "pnl": [item["pnl"] for item in summary.get("by_window", [])],
        },
    }
    return _render(request, "shadow_detail.html", run=run, summary=summary, chart=chart)


@router.get("/replica", response_class=HTMLResponse)
def replica_page(request: Request):
    conn = connect()
    try:
        runs = list_replica_runs(conn)
    finally:
        conn.close()
    return _render(request, "replica.html", runs=runs)


@router.get("/replica/{run_id}", response_class=HTMLResponse)
def replica_detail(request: Request, run_id: str):
    conn = connect()
    try:
        run = get_replica_run(conn, run_id)
    finally:
        conn.close()
    if not run:
        return _render(request, "not_found.html", message="Replica run 不存在")
    summary = run.get("summary") or {}
    equity = run.get("equity") or []
    chart = {
        "labels": [item["day"] for item in equity],
        "pnl": [item["pnl"] for item in equity],
        "daily": [item["daily"] for item in equity],
    }
    return _render(request, "replica_detail.html", run=run, summary=summary, chart=chart)


@router.get("/monitor", response_class=HTMLResponse)
def monitor_page(request: Request):
    conn = connect()
    try:
        wallets = _wallet_rows(conn)
        rows = []
        for wallet in wallets:
            window = drift(conn, wallet["address"])
            snapshots = snapshot_rows(conn, wallet["address"], limit=12)
            rows.append({**wallet, "drift": window, "snapshots": snapshots})
    finally:
        conn.close()
    return _render(request, "monitor.html", rows=rows)


@router.get("/collector", response_class=HTMLResponse)
def collector_page(request: Request):
    status = collector_service.collector_status()
    conn = connect()
    try:
        books = collector_service.recent_books(conn, 30)
        trades = collector_service.recent_trades(conn, 30)
        markets = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM collector_markets ORDER BY start_epoch DESC LIMIT 8"
            ).fetchall()
        ]
    finally:
        conn.close()
    return _render(
        request,
        "collector.html",
        status=status,
        books=books,
        trades=trades,
        markets=markets,
    )


def _chart_payload(dossier: dict[str, Any]) -> dict[str, Any]:
    features = dossier.get("features") or {}
    ov = features.get("overview") or {}
    ex = features.get("execution") or {}
    pair = features.get("pairing") or {}
    timing = features.get("timing") or {}
    pnl = dossier.get("pnl") or {}
    points = pnl.get("points") or []
    daily = pnl.get("daily") or []
    horizons = ex.get("horizons") or []
    by_window = ex.get("by_window") or {}
    by_bucket = ex.get("by_bucket") or {}
    pair_hist = pair.get("pair_cost_hist") or {}
    edge_hist = ex.get("entry_edge_hist") or {}
    hours = timing.get("hours") or {}
    return {
        "pnl": {
            "labels": [datetime.fromtimestamp(point["ts"], timezone.utc).strftime("%m-%d %H:%M") for point in points],
            "realized": [round(point["realized"], 2) for point in points],
            "economic": [round(point["economic"], 2) for point in points],
            "rebate": [round(point["rebate"], 2) for point in points],
        },
        "daily": {
            "labels": [item["day"] for item in daily],
            "volume": [round(item["volume"], 2) for item in daily],
            "fills": [item["fills"] for item in daily],
            "rebate": [round(item["rebate"], 2) for item in daily],
            "economic": [round(item["economic"], 2) for item in daily],
        },
        "markout": {
            "labels": [item["label"] for item in horizons],
            "mean": [round((item["mean"] or 0) * 100, 3) for item in horizons],
            "median": [round((item["median"] or 0) * 100, 3) for item in horizons],
            "positive": [round((item["positive_share"] or 0) * 100, 1) for item in horizons],
        },
        "window": {
            "labels": list(by_window.keys()),
            "edge": [round((values.get("entry_edge") or 0) * 100, 3) for values in by_window.values()],
            "end": [round((values.get("markout_end") or 0) * 100, 3) for values in by_window.values()],
            "count": [values.get("n", 0) for values in by_window.values()],
        },
        "bucket": {
            "labels": list(by_bucket.keys()),
            "edge": [round((values.get("entry_edge") or 0) * 100, 3) for values in by_bucket.values()],
            "end": [round((values.get("markout_end") or 0) * 100, 3) for values in by_bucket.values()],
        },
        "pair_hist": pair_hist,
        "edge_hist": edge_hist,
        "hours": {
            "labels": [f"{hour:02d}:00" for hour in range(24)],
            "counts": [hours.get(str(hour), 0) for hour in range(24)],
        },
        "bars": {
            "seller": [
                {"label": "maker", "value": ov.get("maker", 0)},
                {"label": "taker", "value": ov.get("taker", 0)},
            ],
            "outcomes": ov.get("outcomes", {}),
        },
    }
