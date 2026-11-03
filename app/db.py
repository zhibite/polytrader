from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import get_settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wallets (
    address TEXT PRIMARY KEY,
    label TEXT,
    source TEXT NOT NULL DEFAULT 'manual',
    notes TEXT,
    tracked INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS wallet_stats (
    address TEXT PRIMARY KEY,
    fetched_at INTEGER NOT NULL,
    join_date INTEGER,
    markets INTEGER,
    fills INTEGER,
    volume_shares REAL,
    volume_usdc REAL,
    realized_pnl REAL,
    unrealized_pnl REAL,
    economic_pnl REAL,
    settled_pnl REAL,
    trade_pnl REAL,
    maker_rebate REAL,
    taker_rebate REAL,
    reward_income REAL,
    referral_income REAL,
    fees REAL,
    biggest_win REAL,
    views INTEGER,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS pnl_points (
    address TEXT NOT NULL,
    ts INTEGER NOT NULL,
    realized_pnl REAL,
    unrealized_pnl REAL,
    economic_pnl REAL,
    position_pnl REAL,
    maker_rebate REAL,
    taker_rebate REAL,
    fees REAL,
    volume_usdc REAL,
    volume_shares REAL,
    trade_count INTEGER,
    PRIMARY KEY (address, ts)
);

CREATE TABLE IF NOT EXISTS leaderboard_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at INTEGER NOT NULL,
    time_period TEXT NOT NULL,
    category TEXT NOT NULL,
    sort_by TEXT NOT NULL,
    rank INTEGER,
    address TEXT NOT NULL,
    pnl REAL,
    volume REAL,
    user_name TEXT,
    profile_image TEXT,
    x_username TEXT,
    verified INTEGER,
    raw_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_leaderboard_capture
    ON leaderboard_snapshots(captured_at, time_period, category, sort_by);
CREATE INDEX IF NOT EXISTS idx_leaderboard_address
    ON leaderboard_snapshots(address);

CREATE TABLE IF NOT EXISTS markets (
    slug TEXT PRIMARY KEY,
    condition_id TEXT,
    title TEXT,
    asset TEXT,
    period TEXT,
    start_epoch INTEGER,
    end_epoch INTEGER,
    resolved TEXT,
    up_token TEXT,
    down_token TEXT,
    up_outcome TEXT,
    down_outcome TEXT,
    volume REAL,
    liquidity REAL,
    source TEXT,
    updated_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);
CREATE INDEX IF NOT EXISTS idx_markets_start ON markets(start_epoch);

CREATE TABLE IF NOT EXISTS samples (
    slug TEXT NOT NULL,
    ts INTEGER NOT NULL,
    up_price REAL,
    down_price REAL,
    up_bid REAL,
    up_ask REAL,
    down_bid REAL,
    down_ask REAL,
    PRIMARY KEY (slug, ts)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_samples_slug_ts ON samples(slug, ts);

CREATE TABLE IF NOT EXISTS trades (
    address TEXT NOT NULL,
    trade_key TEXT NOT NULL,
    transaction_hash TEXT,
    token_id TEXT,
    condition_id TEXT,
    slug TEXT,
    outcome TEXT,
    outcome_index INTEGER,
    side TEXT,
    price REAL,
    size REAL,
    usdc_size REAL,
    ts INTEGER NOT NULL,
    is_taker INTEGER NOT NULL DEFAULT 0,
    title TEXT,
    event_slug TEXT,
    market_start INTEGER,
    market_end INTEGER,
    elapsed INTEGER,
    resolved TEXT,
    mid_at_fill REAL,
    mid_1s REAL,
    mid_10s REAL,
    mid_60s REAL,
    mid_end REAL,
    entry_edge REAL,
    markout_1s REAL,
    markout_10s REAL,
    markout_60s REAL,
    markout_end REAL,
    window_label TEXT,
    price_bucket TEXT,
    enriched_at INTEGER,
    PRIMARY KEY (address, trade_key)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(address, ts);
CREATE INDEX IF NOT EXISTS idx_trades_slug ON trades(address, slug);
CREATE INDEX IF NOT EXISTS idx_trades_cond ON trades(address, condition_id);

CREATE TABLE IF NOT EXISTS activity (
    address TEXT NOT NULL,
    activity_key TEXT NOT NULL,
    ts INTEGER NOT NULL,
    type TEXT NOT NULL,
    condition_id TEXT,
    token_id TEXT,
    side TEXT,
    size REAL,
    usdc_size REAL,
    price REAL,
    outcome TEXT,
    outcome_index INTEGER,
    title TEXT,
    slug TEXT,
    transaction_hash TEXT,
    PRIMARY KEY (address, activity_key)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_activity_ts ON activity(address, ts);
CREATE INDEX IF NOT EXISTS idx_activity_type ON activity(address, type);

CREATE TABLE IF NOT EXISTS positions (
    address TEXT NOT NULL,
    token_id TEXT,
    condition_id TEXT,
    status TEXT,
    current_size REAL,
    avg_price REAL,
    current_price REAL,
    current_value REAL,
    realized_pnl REAL,
    unrealized_pnl REAL,
    total_pnl REAL,
    title TEXT,
    slug TEXT,
    outcome TEXT,
    end_date TEXT,
    updated_at INTEGER,
    raw_json TEXT,
    PRIMARY KEY (address, token_id)
);

CREATE TABLE IF NOT EXISTS pair_markets (
    address TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    slug TEXT,
    title TEXT,
    market_start INTEGER,
    market_end INTEGER,
    resolved TEXT,
    up_shares REAL NOT NULL DEFAULT 0,
    down_shares REAL NOT NULL DEFAULT 0,
    up_cost REAL NOT NULL DEFAULT 0,
    down_cost REAL NOT NULL DEFAULT 0,
    up_avg REAL,
    down_avg REAL,
    matched_shares REAL NOT NULL DEFAULT 0,
    pair_cost REAL,
    locked_profit REAL NOT NULL DEFAULT 0,
    residual_shares REAL NOT NULL DEFAULT 0,
    residual_side TEXT,
    winning_shares REAL,
    payout REAL,
    total_cost REAL NOT NULL DEFAULT 0,
    settlement_pnl REAL,
    n_fills INTEGER NOT NULL DEFAULT 0,
    up_fills INTEGER NOT NULL DEFAULT 0,
    down_fills INTEGER NOT NULL DEFAULT 0,
    first_ts INTEGER,
    last_ts INTEGER,
    updated_at INTEGER,
    PRIMARY KEY (address, condition_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_pair_addr ON pair_markets(address);

CREATE TABLE IF NOT EXISTS profiles (
    address TEXT PRIMARY KEY,
    generated_at INTEGER NOT NULL,
    classification TEXT,
    confidence REAL,
    tags_json TEXT,
    evidence_json TEXT,
    features_json TEXT,
    cluster_id INTEGER,
    cluster_label TEXT
);

CREATE TABLE IF NOT EXISTS metric_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT,
    captured_at INTEGER NOT NULL,
    window_days INTEGER NOT NULL,
    metrics_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_metric_snap_addr
    ON metric_snapshots(address, captured_at);

CREATE TABLE IF NOT EXISTS ingest_checkpoints (
    address TEXT NOT NULL,
    stream TEXT NOT NULL,
    days INTEGER NOT NULL,
    window_start INTEGER NOT NULL,
    window_end INTEGER NOT NULL,
    cursor TEXT,
    rows INTEGER NOT NULL DEFAULT 0,
    signature TEXT,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (address, stream, days)
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    address TEXT,
    status TEXT NOT NULL DEFAULT 'QUEUED',
    params_json TEXT,
    progress REAL NOT NULL DEFAULT 0,
    total INTEGER NOT NULL DEFAULT 0,
    done INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    error TEXT,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, created_at);

CREATE TABLE IF NOT EXISTS shadow_runs (
    id TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'QUEUED',
    params_json TEXT,
    summary_json TEXT,
    created_at INTEGER NOT NULL,
    finished_at INTEGER,
    error TEXT
);

CREATE TABLE IF NOT EXISTS shadow_trades (
    run_id TEXT NOT NULL,
    trade_key TEXT NOT NULL,
    ts INTEGER,
    slug TEXT,
    condition_id TEXT,
    outcome TEXT,
    side TEXT,
    target_price REAL,
    copy_price REAL,
    size REAL,
    delay_s REAL,
    resolved TEXT,
    payout REAL,
    pnl REAL,
    mid_end REAL,
    entry_edge REAL,
    markout_end REAL,
    PRIMARY KEY (run_id, trade_key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS replica_runs (
    id TEXT PRIMARY KEY,
    strategy TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'QUEUED',
    params_json TEXT,
    summary_json TEXT,
    equity_json TEXT,
    created_at INTEGER NOT NULL,
    finished_at INTEGER,
    error TEXT
);

CREATE TABLE IF NOT EXISTS replica_trades (
    run_id TEXT NOT NULL,
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT,
    condition_id TEXT,
    ts INTEGER,
    outcome TEXT,
    price REAL,
    size REAL,
    mid_at_fill REAL,
    resolved TEXT,
    payout REAL,
    pnl REAL,
    elapsed INTEGER
);
CREATE INDEX IF NOT EXISTS idx_replica_trades_run ON replica_trades(run_id);

CREATE TABLE IF NOT EXISTS sweep_runs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'QUEUED',
    params_json TEXT,
    summary_json TEXT,
    created_at INTEGER NOT NULL,
    finished_at INTEGER,
    error TEXT
);

CREATE TABLE IF NOT EXISTS sweep_results (
    run_id TEXT NOT NULL,
    combo_id TEXT NOT NULL,
    params_json TEXT NOT NULL,
    family TEXT NOT NULL DEFAULT 'mid_maker',
    train_pnl REAL,
    train_roi REAL,
    train_fills INTEGER,
    train_win_rate REAL,
    val_pnl REAL,
    val_roi REAL,
    val_fills INTEGER,
    val_win_rate REAL,
    pnl REAL,
    roi REAL,
    fills INTEGER,
    win_rate REAL,
    cost REAL,
    max_drawdown REAL,
    score REAL,
    rank INTEGER,
    PRIMARY KEY (run_id, combo_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_sweep_results_rank ON sweep_results(run_id, rank);

CREATE TABLE IF NOT EXISTS strategies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    family TEXT NOT NULL,
    params_json TEXT NOT NULL,
    metrics_json TEXT,
    source_run TEXT,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_strategies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    source_run TEXT,
    params_json TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    starting_balance REAL NOT NULL,
    cash REAL NOT NULL,
    realized_pnl REAL NOT NULL DEFAULT 0,
    fees_paid REAL NOT NULL DEFAULT 0,
    last_tick_at INTEGER,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_positions (
    id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    slug TEXT NOT NULL,
    condition_id TEXT,
    outcome TEXT NOT NULL,
    shares REAL NOT NULL,
    cost REAL NOT NULL,
    avg_price REAL NOT NULL,
    opened_at INTEGER NOT NULL,
    market_end INTEGER,
    status TEXT NOT NULL DEFAULT 'OPEN',
    resolved TEXT,
    payout REAL,
    settlement_pnl REAL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(strategy_id, slug, outcome)
);
CREATE INDEX IF NOT EXISTS idx_paper_positions_strategy ON paper_positions(strategy_id, status);

CREATE TABLE IF NOT EXISTS paper_trades (
    id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    slug TEXT NOT NULL,
    condition_id TEXT,
    outcome TEXT NOT NULL,
    ts INTEGER NOT NULL,
    price REAL NOT NULL,
    shares REAL NOT NULL,
    notional REAL NOT NULL,
    mid REAL,
    window_label TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_paper_trades_strategy ON paper_trades(strategy_id, ts);

CREATE TABLE IF NOT EXISTS paper_equity (
    strategy_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    cash REAL NOT NULL,
    positions_value REAL NOT NULL,
    equity REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    PRIMARY KEY (strategy_id, ts)
);

CREATE TABLE IF NOT EXISTS paper_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS monitor_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    captured_at INTEGER NOT NULL,
    window_days INTEGER NOT NULL,
    metrics_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_monitor_addr ON monitor_snapshots(address, captured_at);

CREATE TABLE IF NOT EXISTS collector_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collector_markets (
    slug TEXT PRIMARY KEY,
    asset TEXT,
    period TEXT,
    condition_id TEXT,
    start_epoch INTEGER,
    end_epoch INTEGER,
    up_token TEXT,
    down_token TEXT,
    status TEXT,
    discovered_at INTEGER,
    updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS collector_books (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT,
    ts INTEGER NOT NULL,
    best_bid REAL,
    best_ask REAL,
    mid REAL,
    spread REAL,
    bid_depth REAL,
    ask_depth REAL,
    raw_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_collector_books_ts ON collector_books(slug, ts);

CREATE TABLE IF NOT EXISTS collector_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    token_id TEXT,
    outcome TEXT,
    ts INTEGER NOT NULL,
    price REAL,
    size REAL,
    side TEXT,
    raw_json TEXT,
    UNIQUE(slug, token_id, ts, price, size, side)
);
CREATE INDEX IF NOT EXISTS idx_collector_trades_ts ON collector_trades(slug, ts);
"""


def connect(path: Path | str | None = None) -> sqlite3.Connection:
    settings = get_settings()
    db_path = Path(path) if path is not None else settings.database_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@contextmanager
def db_session() -> Iterator[sqlite3.Connection]:
    conn = connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    settings = get_settings()
    settings.ensure_dirs()
    with db_session() as conn:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES('version', '1')"
        )


def now_ts() -> int:
    return int(time.time())


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def from_json(value: str | None, default: Any = None) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def utc_str(ts: int | float | None) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(float(ts), timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def utc_day(ts: int | float | None) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(float(ts), timezone.utc).strftime("%Y-%m-%d")


def ensure_target_wallet() -> None:
    settings = get_settings()
    address = settings.target_address.lower()
    now = now_ts()
    with db_session() as conn:
        conn.execute(
            """INSERT INTO wallets(address, label, source, notes, tracked, created_at, updated_at)
               VALUES(?, ?, 'seed', ?, 1, ?, ?)
               ON CONFLICT(address) DO UPDATE SET updated_at=excluded.updated_at""",
            (
                address,
                "Target · BTC 5m maker",
                "Built-in research target from docs/1.md",
                now,
                now,
            ),
        )


def get_state(key: str, default: str | None = None) -> str | None:
    with db_session() as conn:
        row = conn.execute("SELECT value FROM collector_state WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_state(key: str, value: str) -> None:
    with db_session() as conn:
        conn.execute(
            "INSERT INTO collector_state(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
