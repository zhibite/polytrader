"""Forward paper trading on the live collector.

The engine reads the latest BTC 5m market and book snapshots written by
CollectorManager, simulates maker fills when a trade prints through our bid,
holds to settlement and records trades, positions and equity.

This is paper only: no keys, no signatures, no real orders.
"""

from __future__ import annotations

import secrets
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any

from ..config import get_settings
from ..db import connect, from_json, get_state, now_ts, set_state, to_json
from .klines import fetch_resolutions

WINDOW_ORDER = ["0-30s", "30-60s", "1-2m", "2-3m", "3-4m", "4-5m"]


def _window_label(elapsed: int) -> str:
    if elapsed < 0:
        return "pre-open"
    if elapsed < 30:
        return "0-30s"
    if elapsed < 60:
        return "30-60s"
    if elapsed < 120:
        return "1-2m"
    if elapsed < 180:
        return "2-3m"
    if elapsed < 240:
        return "3-4m"
    if elapsed < 300:
        return "4-5m"
    return "5m+"


def _latest_book(conn: sqlite3.Connection, slug: str, token_id: str | None) -> dict[str, Any] | None:
    if not token_id:
        return None
    row = conn.execute(
        """SELECT * FROM collector_books
           WHERE slug=? AND token_id=? ORDER BY id DESC LIMIT 1""",
        (slug, token_id),
    ).fetchone()
    return dict(row) if row else None


def _current_market(conn: sqlite3.Connection, params: dict[str, Any]) -> dict[str, Any] | None:
    now = now_ts()
    row = conn.execute(
        """SELECT * FROM collector_markets
           WHERE start_epoch <= ? AND end_epoch > ? AND asset=? AND period=?
           ORDER BY start_epoch DESC LIMIT 1""",
        (now, now - 30, params.get("asset", "btc"), params.get("period", "5m")),
    ).fetchone()
    if row:
        return dict(row)
    # Fall back to the markets table when the collector has not written a
    # collector_markets row yet.
    row = conn.execute(
        """SELECT * FROM markets
           WHERE start_epoch <= ? AND end_epoch > ?
             AND asset=? AND period=?
           ORDER BY start_epoch DESC LIMIT 1""",
        (now, now - 30, params.get("asset", "btc"), params.get("period", "5m")),
    ).fetchone()
    return dict(row) if row else None


def _ensure_market(conn: sqlite3.Connection, market: dict[str, Any]) -> None:
    conn.execute(
        """INSERT OR IGNORE INTO markets(
             slug, condition_id, title, asset, period, start_epoch, end_epoch,
             resolved, up_token, down_token, source, updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            market.get("slug"),
            market.get("condition_id"),
            market.get("title"),
            market.get("asset", "btc"),
            market.get("period", "5m"),
            market.get("start_epoch"),
            market.get("end_epoch"),
            market.get("resolved"),
            market.get("up_token"),
            market.get("down_token"),
            "collector",
            now_ts(),
        ),
    )


def activate_paper_strategy(
    name: str,
    params: dict[str, Any],
    source_run: str | None = None,
    starting_balance: float | None = None,
    freeze_hours: float | None = None,
) -> str:
    settings = get_settings()
    balance = float(starting_balance or settings.paper_starting_balance)
    freeze = settings.paper_freeze_hours if freeze_hours is None else freeze_hours
    freeze_until = now_ts() + int(max(0.0, freeze) * 3600) if freeze and freeze > 0 else None
    strategy_id = f"paper_{secrets.token_hex(6)}"
    conn = connect()
    try:
        conn.execute("UPDATE paper_strategies SET active=0, updated_at=? WHERE active=1", (now_ts(),))
        conn.execute(
            """INSERT INTO paper_strategies(
                 id, name, source_run, params_json, active, starting_balance, cash,
                 realized_pnl, fees_paid, peak_equity, freeze_until, created_at, updated_at)
               VALUES(?,?,?,?,1,?,?,0,0,?,?,?,?)""",
            (
                strategy_id,
                name,
                source_run,
                to_json(params),
                balance,
                balance,
                balance,
                freeze_until,
                now_ts(),
                now_ts(),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return strategy_id


def _strategy_params(row: dict[str, Any]) -> dict[str, Any]:
    return from_json(row.get("params_json"), {}) or {}


def _open_cost(conn: sqlite3.Connection, strategy_id: str) -> float:
    row = conn.execute(
        "SELECT COALESCE(SUM(cost),0) AS cost FROM paper_positions WHERE strategy_id=? AND status='OPEN'",
        (strategy_id,),
    ).fetchone()
    return float(row["cost"] or 0)


def _settle(conn: sqlite3.Connection, now: int) -> None:
    positions = conn.execute(
        """SELECT p.*, m.end_epoch, m.resolved AS market_resolved
           FROM paper_positions p LEFT JOIN markets m ON m.slug=p.slug
           WHERE p.status='OPEN' AND COALESCE(p.market_end, m.end_epoch, 0) <= ?""",
        (now,),
    ).fetchall()
    if not positions:
        return
    conditions = [row["condition_id"] for row in positions if row["condition_id"]]
    if conditions:
        last = get_state("paper_last_resolution_fetch")
        try:
            last_ts = int(last) if last else 0
        except ValueError:
            last_ts = 0
        if now - last_ts >= 30:
            try:
                fetch_resolutions(conn, list(dict.fromkeys(conditions)))
                set_state("paper_last_resolution_fetch", str(now))
            except Exception:
                pass
    for position in positions:
        resolved = (position["market_resolved"] or "").upper()
        if resolved not in ("UP", "DOWN"):
            continue
        payout = float(position["shares"]) if resolved == position["outcome"] else 0.0
        pnl = payout - float(position["cost"])
        conn.execute(
            """UPDATE paper_positions SET status='SETTLED', resolved=?, payout=?,
                                       settlement_pnl=?, updated_at=? WHERE id=?""",
            (resolved, payout, pnl, now, position["id"]),
        )
        conn.execute(
            """UPDATE paper_strategies SET cash=cash+?, realized_pnl=realized_pnl+?,
                                         updated_at=? WHERE id=?""",
            (payout, pnl, now, position["strategy_id"]),
        )


def _valuation(conn: sqlite3.Connection, strategy: dict[str, Any]) -> tuple[float, float]:
    positions = conn.execute(
        "SELECT * FROM paper_positions WHERE strategy_id=? AND status='OPEN'",
        (strategy["id"],),
    ).fetchall()
    positions_value = 0.0
    cost = 0.0
    for position in positions:
        market = conn.execute(
            "SELECT up_token, down_token FROM markets WHERE slug=?", (position["slug"],)
        ).fetchone()
        token = None
        if market:
            token = market["up_token"] if position["outcome"] == "UP" else market["down_token"]
        book = _latest_book(conn, position["slug"], token)
        price = float(book["mid"]) if book and book["mid"] is not None else float(position["avg_price"])
        positions_value += price * float(position["shares"])
        cost += float(position["cost"])
    return positions_value, cost


def _current_equity(conn: sqlite3.Connection, strategy: dict[str, Any]) -> float:
    positions_value, _ = _valuation(conn, strategy)
    return float(strategy["cash"]) + positions_value


def _drawdown_breached(strategy: dict[str, Any], equity: float, max_drawdown: float) -> str | None:
    starting = float(strategy.get("starting_balance") or 0)
    peak = float(strategy.get("peak_equity") or starting or equity)
    peak = max(peak, equity)
    if peak <= 0 or max_drawdown <= 0:
        return None
    drawdown = (peak - equity) / peak
    if drawdown >= max_drawdown:
        return f"peak drawdown {drawdown:.1%} >= {max_drawdown:.1%}"
    return None


def _mark_equity(conn: sqlite3.Connection, strategy: dict[str, Any], now: int) -> None:
    positions_value, cost = _valuation(conn, strategy)
    unrealized = positions_value - cost
    cash = float(strategy["cash"])
    equity = cash + positions_value
    ts = now // 60 * 60
    conn.execute(
        """INSERT INTO paper_equity(strategy_id, ts, cash, positions_value, equity,
                                    realized_pnl, unrealized_pnl)
           VALUES(?,?,?,?,?,?,?)
           ON CONFLICT(strategy_id, ts) DO UPDATE SET
             cash=excluded.cash, positions_value=excluded.positions_value,
             equity=excluded.equity, realized_pnl=excluded.realized_pnl,
             unrealized_pnl=excluded.unrealized_pnl""",
        (strategy["id"], ts, cash, positions_value, equity, strategy["realized_pnl"], unrealized),
    )


def paper_tick() -> dict[str, Any]:
    conn = connect()
    now = now_ts()
    opened = 0
    settled_before = conn.execute(
        "SELECT COUNT(*) AS n FROM paper_positions WHERE status='OPEN'"
    ).fetchone()["n"]
    try:
        strategies = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM paper_strategies WHERE active=1 ORDER BY created_at DESC"
            ).fetchall()
        ]
        settings = get_settings()
        max_drawdown = float(settings.paper_max_drawdown_pct)
        for strategy in strategies:
            equity = _current_equity(conn, strategy)
            peak = max(float(strategy.get("peak_equity") or strategy["starting_balance"]), equity)
            conn.execute(
                "UPDATE paper_strategies SET peak_equity=? WHERE id=?",
                (peak, strategy["id"]),
            )
            strategy = {**strategy, "peak_equity": peak}
            stop = _drawdown_breached(strategy, equity, max_drawdown)
            if stop:
                conn.execute(
                    "UPDATE paper_strategies SET active=0, stop_reason=?, updated_at=? WHERE id=?",
                    (stop, now, strategy["id"]),
                )
                continue
            params = _strategy_params(strategy)
            market = _current_market(conn, params)
            if not market:
                continue
            _ensure_market(conn, market)
            market_start = int(market.get("start_epoch") or now)
            market_end = int(market.get("end_epoch") or (market_start + 300))
            if now >= market_end:
                continue
            elapsed = now - market_start
            window = _window_label(elapsed)
            allowed = set(params.get("windows") or WINDOW_ORDER)
            if window not in allowed:
                continue
            band = params.get("band") or [0.0, 1.0]
            try:
                low, high = float(band[0]), float(band[1])
            except (TypeError, ValueError, IndexError):
                low, high = 0.0, 1.0
            offset = float(params.get("offset", 0.005))
            size = float(params.get("size", 10))
            max_cost = float(params.get("max_cost_per_market", 25))
            max_open = float(params.get("max_open_cost", 500))
            fee_bps = float(params.get("fee_bps", 0))
            rebate_bps = float(params.get("rebate_bps", 0))
            last_tick = int(strategy.get("last_tick_at") or now)
            if last_tick >= now:
                last_tick = now - 1
            book_cost = _open_cost(conn, strategy["id"])
            for outcome, token_key in (("UP", "up_token"), ("DOWN", "down_token")):
                existing = conn.execute(
                    "SELECT 1 FROM paper_positions WHERE strategy_id=? AND slug=? AND outcome=?",
                    (strategy["id"], market["slug"], outcome),
                ).fetchone()
                if existing:
                    continue
                token = market.get(token_key)
                book = _latest_book(conn, market["slug"], token)
                if not book or book.get("mid") is None:
                    continue
                mid = float(book["mid"])
                if not (low <= mid <= high):
                    continue
                quote = mid - offset
                if quote <= 0:
                    continue
                trade = conn.execute(
                    """SELECT * FROM collector_trades
                       WHERE slug=? AND token_id=? AND ts>? AND ts<=? AND price<=?
                       ORDER BY ts ASC LIMIT 1""",
                    (market["slug"], token, last_tick, now, quote),
                ).fetchone()
                if not trade:
                    continue
                shares = min(size, max_cost / quote)
                gross = shares * quote
                # Per-market cap across both outcomes.
                other = conn.execute(
                    """SELECT COALESCE(SUM(cost),0) AS cost FROM paper_positions
                       WHERE strategy_id=? AND slug=? AND status='OPEN'""",
                    (strategy["id"], market["slug"]),
                ).fetchone()["cost"]
                if float(other) + gross > max_cost > 0:
                    scale = max(0.0, (max_cost - float(other)) / gross)
                    shares *= scale
                    gross = shares * quote
                    if shares <= 0:
                        continue
                if book_cost + gross > max_open > 0:
                    continue
                fee = gross * fee_bps / 10000.0
                rebate = gross * rebate_bps / 10000.0
                net_cost = gross + fee - rebate
                if net_cost > float(strategy["cash"]):
                    continue
                position_id = f"pos_{secrets.token_hex(6)}"
                conn.execute(
                    """INSERT INTO paper_positions(
                         id, strategy_id, slug, condition_id, outcome, shares, cost,
                         avg_price, opened_at, market_end, status, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,'OPEN',?,?)""",
                    (
                        position_id,
                        strategy["id"],
                        market["slug"],
                        market.get("condition_id"),
                        outcome,
                        shares,
                        net_cost,
                        quote,
                        now,
                        market_end,
                        now,
                        now,
                    ),
                )
                conn.execute(
                    """INSERT INTO paper_trades(
                         id, strategy_id, slug, condition_id, outcome, ts, price,
                         shares, notional, mid, window_label, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        f"pt_{secrets.token_hex(6)}",
                        strategy["id"],
                        market["slug"],
                        market.get("condition_id"),
                        outcome,
                        int(trade["ts"]),
                        quote,
                        shares,
                        gross,
                        mid,
                        window,
                        now,
                    ),
                )
                conn.execute(
                    """UPDATE paper_strategies SET cash=cash-?, fees_paid=fees_paid+?,
                                                 updated_at=? WHERE id=?""",
                    (net_cost, fee - rebate, now, strategy["id"]),
                )
                book_cost += gross
                opened += 1
            conn.execute(
                "UPDATE paper_strategies SET last_tick_at=?, updated_at=? WHERE id=?",
                (now, now, strategy["id"]),
            )
        _settle(conn, now)
        strategies = [
            dict(row)
            for row in conn.execute("SELECT * FROM paper_strategies").fetchall()
        ]
        for strategy in strategies:
            _mark_equity(conn, strategy, now)
            equity = _current_equity(conn, strategy)
            peak = max(float(strategy.get("peak_equity") or strategy["starting_balance"]), equity)
            conn.execute(
                "UPDATE paper_strategies SET peak_equity=? WHERE id=?",
                (peak, strategy["id"]),
            )
            stop = _drawdown_breached({**strategy, "peak_equity": peak}, equity, max_drawdown)
            if stop and int(strategy.get("active") or 0):
                conn.execute(
                    "UPDATE paper_strategies SET active=0, stop_reason=?, updated_at=? WHERE id=?",
                    (stop, now, strategy["id"]),
                )
        conn.commit()
    finally:
        conn.close()
    return {"opened": opened, "open_positions_before": settled_before}


class PaperWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_error: str | None = None

    def start(self) -> None:
        if not self.settings.paper_enabled:
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="paper-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                paper_tick()
                self.last_error = None
            except Exception as exc:
                self.last_error = str(exc)[:500]
            self._stop.wait(max(2.0, self.settings.paper_tick_seconds))


def active_strategy(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM paper_strategies WHERE active=1 ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def settled_count(conn: sqlite3.Connection, strategy_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM paper_positions WHERE strategy_id=? AND status='SETTLED'",
        (strategy_id,),
    ).fetchone()
    return int(row["n"]) if row else 0


def source_val_roi(conn: sqlite3.Connection, strategy: dict[str, Any]) -> float | None:
    params = _strategy_params(strategy)
    value = params.get("source_val_roi")
    if isinstance(value, (int, float)):
        return float(value)
    source_run = strategy.get("source_run")
    if source_run:
        row = conn.execute(
            "SELECT val_roi FROM sweep_results WHERE run_id=? ORDER BY rank ASC LIMIT 1",
            (source_run,),
        ).fetchone()
        if row and row["val_roi"] is not None:
            return float(row["val_roi"])
    return None


def promotion_decision(
    conn: sqlite3.Connection,
    candidate_val_roi: float | None,
    min_retention_hours: float = 24.0,
    min_settled_trades: int = 100,
    margin: float = 0.01,
) -> dict[str, Any]:
    """Decide whether a new paper strategy may replace the active one.

    A candidate is eligible only after the active strategy has run for at
    least N hours OR settled at least M positions. It must then beat the
    active strategy's source validation ROI by ``margin``.
    """
    active = active_strategy(conn)
    if not active:
        if candidate_val_roi is None or float(candidate_val_roi) <= 0:
            return {
                "replace": False,
                "reason": "candidate val ROI <= 0; staying flat",
                "active_id": None,
            }
        return {"replace": True, "reason": "no active paper strategy", "active_id": None}
    freeze_until = active.get("freeze_until")
    if freeze_until and now_ts() < int(freeze_until):
        until = datetime.fromtimestamp(int(freeze_until), timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return {
            "replace": False,
            "reason": f"frozen until {until}",
            "active_id": active["id"],
            "active_val_roi": source_val_roi(conn, active) or 0.0,
        }
    if candidate_val_roi is None or float(candidate_val_roi) <= 0:
        return {
            "replace": False,
            "reason": "candidate val ROI <= 0",
            "active_id": active["id"],
            "active_val_roi": source_val_roi(conn, active) or 0.0,
        }
    age_hours = max(0.0, (now_ts() - int(active["created_at"])) / 3600.0)
    settled = settled_count(conn, active["id"])
    active_roi = source_val_roi(conn, active) or 0.0
    if age_hours < min_retention_hours and settled < min_settled_trades:
        return {
            "replace": False,
            "reason": f"retention: {age_hours:.1f}h / {settled} settled",
            "active_id": active["id"],
            "age_hours": age_hours,
            "settled": settled,
            "active_val_roi": active_roi,
        }
    if candidate_val_roi is None:
        return {
            "replace": False,
            "reason": "candidate has no validation ROI",
            "active_id": active["id"],
            "age_hours": age_hours,
            "settled": settled,
            "active_val_roi": active_roi,
        }
    if float(candidate_val_roi) < active_roi + margin:
        return {
            "replace": False,
            "reason": (
                f"candidate {float(candidate_val_roi) * 100:.2f}% < "
                f"active {active_roi * 100:.2f}% + {margin * 100:.1f}%"
            ),
            "active_id": active["id"],
            "age_hours": age_hours,
            "settled": settled,
            "active_val_roi": active_roi,
        }
    return {
        "replace": True,
        "reason": (
            f"candidate {float(candidate_val_roi) * 100:.2f}% > "
            f"active {active_roi * 100:.2f}% + {margin * 100:.1f}%"
        ),
        "active_id": active["id"],
        "age_hours": age_hours,
        "settled": settled,
        "active_val_roi": active_roi,
    }


def list_strategies(conn: sqlite3.Connection, limit: int = 20) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM paper_strategies ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["params"] = _strategy_params(item)
        item["open_positions"] = conn.execute(
            "SELECT COUNT(*) AS n FROM paper_positions WHERE strategy_id=? AND status='OPEN'",
            (item["id"],),
        ).fetchone()["n"]
        item["fills"] = conn.execute(
            "SELECT COUNT(*) AS n FROM paper_trades WHERE strategy_id=?", (item["id"],)
        ).fetchone()["n"]
        out.append(item)
    return out


def get_strategy(conn: sqlite3.Connection, strategy_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM paper_strategies WHERE id=?", (strategy_id,)).fetchone()
    if not row:
        return None
    item = dict(row)
    item["params"] = _strategy_params(item)
    item["positions"] = [
        dict(pos)
        for pos in conn.execute(
            "SELECT * FROM paper_positions WHERE strategy_id=? ORDER BY opened_at DESC LIMIT 200",
            (strategy_id,),
        ).fetchall()
    ]
    item["trades"] = [
        dict(trade)
        for trade in conn.execute(
            "SELECT * FROM paper_trades WHERE strategy_id=? ORDER BY ts DESC LIMIT 300",
            (strategy_id,),
        ).fetchall()
    ]
    item["equity"] = [
        dict(point)
        for point in conn.execute(
            "SELECT * FROM paper_equity WHERE strategy_id=? ORDER BY ts ASC LIMIT 2000",
            (strategy_id,),
        ).fetchall()
    ]
    return item


def paper_tick_ts(ts: int | None) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%m-%d %H:%M:%S")
