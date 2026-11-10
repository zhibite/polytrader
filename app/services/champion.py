"""Champion / Challenger live pilot.

`promote_paper_strategy` freezes a paper strategy into `live_strategies`.
The challenger (paper pipeline) never modifies the champion. When the second
explicit switch `LAB_LIVE_STRATEGY_ENABLED=1` is set (on top of the live
execution gates), `live_tick` mirrors the frozen snapshot against the live
CLOB with tiny risk limits.

Paper remains the primary mode; both switches are off by default.
"""

from __future__ import annotations

import asyncio
import secrets
from typing import Any

from ..config import get_settings
from ..db import connect, from_json, now_ts, to_json
from .klines import fetch_resolutions
from .live import LiveError, _record_event, _redact, executor

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


def promote_paper_strategy(
    paper_id: str,
    name: str | None = None,
    starting_balance: float | None = None,
) -> str:
    settings = get_settings()
    balance = float(starting_balance or settings.live_pilot_starting_balance)
    conn = connect()
    try:
        paper = conn.execute(
            "SELECT * FROM paper_strategies WHERE id=?", (paper_id,)
        ).fetchone()
        if not paper:
            raise LiveError("paper strategy not found")
        params = from_json(paper["params_json"], {}) or {}
        conn.execute(
            "UPDATE live_strategies SET active=0, status='PAUSED', updated_at=? WHERE active=1",
            (now_ts(),),
        )
        live_id = f"champ_{secrets.token_hex(6)}"
        conn.execute(
            """INSERT INTO live_strategies(
                 id, name, source_paper_id, source_run, params_json, status, active,
                 starting_balance, cash, realized_pnl, created_at, updated_at)
               VALUES(?,?,?,?,?, 'PILOT', 1, ?, ?, 0, ?, ?)""",
            (
                live_id,
                name or f"{paper['name']} · live pilot",
                paper_id,
                paper["source_run"],
                to_json(params),
                balance,
                balance,
                now_ts(),
                now_ts(),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    _record_event("WARNING", f"champion promoted from paper {paper_id} -> {live_id}")
    return live_id


def active_live_strategy(conn) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM live_strategies WHERE active=1 ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def list_live_strategies(conn, limit: int = 20) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM live_strategies ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["params"] = from_json(item.get("params_json"), {}) or {}
        item["open_positions"] = conn.execute(
            "SELECT COUNT(*) AS n FROM live_positions WHERE strategy_id=? AND status='OPEN'",
            (item["id"],),
        ).fetchone()["n"]
        item["settled_positions"] = conn.execute(
            "SELECT COUNT(*) AS n FROM live_positions WHERE strategy_id=? AND status='SETTLED'",
            (item["id"],),
        ).fetchone()["n"]
        source = conn.execute(
            "SELECT cash, realized_pnl, active FROM paper_strategies WHERE id=?",
            (item.get("source_paper_id"),),
        ).fetchone()
        item["source_paper"] = dict(source) if source else None
        out.append(item)
    return out


def _current_market(conn, params: dict[str, Any]) -> dict[str, Any] | None:
    now = now_ts()
    row = conn.execute(
        """SELECT * FROM collector_markets
           WHERE start_epoch <= ? AND end_epoch > ? AND asset=? AND period=?
           ORDER BY start_epoch DESC LIMIT 1""",
        (now, now - 30, params.get("asset", "btc"), params.get("period", "5m")),
    ).fetchone()
    return dict(row) if row else None


def _latest_book(conn, slug: str, token_id: str | None) -> dict[str, Any] | None:
    if not token_id:
        return None
    row = conn.execute(
        "SELECT * FROM collector_books WHERE slug=? AND token_id=? ORDER BY id DESC LIMIT 1",
        (slug, token_id),
    ).fetchone()
    return dict(row) if row else None


def _ensure_market(conn, market: dict[str, Any]) -> None:
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


def _apply_fill(conn, strategy: dict[str, Any], order: dict[str, Any], delta: float, market_end: int | None = None) -> None:
    if delta <= 0:
        return
    price = float(order["price"])
    cost = delta * price
    position = conn.execute(
        "SELECT * FROM live_positions WHERE strategy_id=? AND slug=? AND outcome=?",
        (strategy["id"], order["slug"], order["outcome"]),
    ).fetchone()
    now = now_ts()
    if position:
        shares = float(position["shares"]) + delta
        total_cost = float(position["cost"]) + cost
        conn.execute(
            """UPDATE live_positions SET shares=?, cost=?, avg_price=?, updated_at=?
               WHERE id=?""",
            (shares, total_cost, total_cost / shares if shares else price, now, position["id"]),
        )
    else:
        conn.execute(
            """INSERT INTO live_positions(
                 id, strategy_id, slug, condition_id, token_id, outcome, shares, cost,
                 avg_price, opened_at, market_end, status, order_id, created_at, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?,?)""",
            (
                f"lpos_{secrets.token_hex(6)}",
                strategy["id"],
                order["slug"],
                order["condition_id"],
                order["token_id"],
                order["outcome"],
                delta,
                cost,
                price,
                now,
                market_end,
                order["id"],
                now,
                now,
            ),
        )
    conn.execute(
        "UPDATE live_strategies SET cash=cash-?, updated_at=? WHERE id=?",
        (cost, now, strategy["id"]),
    )
    conn.execute(
        """INSERT OR IGNORE INTO live_fills(
             id, order_id, trade_id, token_id, side, price, size, fee, ts, raw_json)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (
            f"lf_{secrets.token_hex(8)}",
            order["id"],
            f"{order['id']}:{float(order.get('filled_size') or 0) + delta:.6f}",
            order["token_id"],
            order["side"],
            price,
            delta,
            0.0,
            now,
            None,
        ),
    )


def _settle(conn, strategy: dict[str, Any], now: int) -> None:
    positions = conn.execute(
        "SELECT * FROM live_positions WHERE strategy_id=? AND status='OPEN' AND market_end<=?",
        (strategy["id"], now),
    ).fetchall()
    if not positions:
        return
    conditions = [row["condition_id"] for row in positions if row["condition_id"]]
    last = conn.execute(
        "SELECT value FROM live_state WHERE key='last_resolution_fetch'"
    ).fetchone()
    try:
        last_ts = int(last["value"]) if last else 0
    except (TypeError, ValueError):
        last_ts = 0
    if conditions and now - last_ts >= 30:
        try:
            fetch_resolutions(conn, list(dict.fromkeys(conditions)))
            conn.execute(
                "INSERT INTO live_state(key,value) VALUES('last_resolution_fetch',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(now),),
            )
            conn.commit()
        except Exception as exc:
            _record_event("WARNING", f"live settlement resolution failed: {_redact(str(exc))}")
    for position in positions:
        market = conn.execute(
            "SELECT resolved FROM markets WHERE slug=?", (position["slug"],)
        ).fetchone()
        resolved = (market["resolved"] if market else "") or ""
        if resolved not in ("UP", "DOWN"):
            continue
        payout = float(position["shares"]) if resolved == position["outcome"] else 0.0
        pnl = payout - float(position["cost"])
        conn.execute(
            """UPDATE live_positions SET status='SETTLED', resolved=?, payout=?,
                                       settlement_pnl=?, updated_at=? WHERE id=?""",
            (resolved, payout, pnl, now, position["id"]),
        )
        conn.execute(
            "UPDATE live_strategies SET cash=cash+?, realized_pnl=realized_pnl+?, updated_at=? WHERE id=?",
            (payout, pnl, now, strategy["id"]),
        )


async def live_tick() -> dict[str, Any]:
    settings = get_settings()
    if not settings.live_strategy_enabled or not settings.live_enabled:
        return {"skipped": "live strategy runner disabled"}
    status = executor.configured_status()
    if not status.get("ready"):
        return {"skipped": "live execution not ready"}
    if status.get("kill_switch"):
        try:
            await executor.cancel_all("kill_switch")
        except LiveError:
            pass
        return {"skipped": "kill switch"}
    conn = connect()
    try:
        strategy = active_live_strategy(conn)
        if not strategy or strategy.get("status") != "PILOT":
            return {"skipped": "no active live strategy"}
        params = from_json(strategy.get("params_json"), {}) or {}
        now = now_ts()
        market = _current_market(conn, params)
        if not market:
            return {"skipped": "no current market"}
        _ensure_market(conn, market)
        market_start = int(market.get("start_epoch") or now)
        market_end = int(market.get("end_epoch") or (market_start + 300))
        slug = market["slug"]

        # 1) Poll fills of existing orders.
        open_orders = conn.execute(
            "SELECT * FROM live_orders WHERE strategy_id=? AND status IN ('PLACED','LIVE')",
            (strategy["id"],),
        ).fetchall()
        for order in open_orders:
            before = float(order["filled_size"] or 0)
            try:
                result = await executor.refresh_order(order["id"])
            except LiveError:
                continue
            after = float(result.get("filled_size") or 0)
            if after > before:
                market_row = conn.execute(
                    "SELECT end_epoch FROM markets WHERE slug=?", (order["slug"],)
                ).fetchone()
                _apply_fill(
                    conn,
                    strategy,
                    {**dict(order), "filled_size": after},
                    after - before,
                    int(market_row["end_epoch"]) if market_row and market_row["end_epoch"] else None,
                )
                conn.commit()

        # 2) Cancel orders once their market is over (including stale rows).
        for order in conn.execute(
            "SELECT * FROM live_orders WHERE strategy_id=? AND status IN ('PLACED','LIVE')",
            (strategy["id"],),
        ).fetchall():
            market_row = conn.execute(
                "SELECT end_epoch FROM markets WHERE slug=?", (order["slug"],)
            ).fetchone()
            if market_row and market_row["end_epoch"] and now >= int(market_row["end_epoch"]):
                try:
                    await executor.cancel_order(order["id"])
                except LiveError:
                    pass
        # 3) Place at most one post-only bid per outcome per market.
        if now < market_end:
            elapsed = now - market_start
            window = _window_label(elapsed)
            allowed = set(params.get("windows") or WINDOW_ORDER)
            band = params.get("band") or [0.0, 1.0]
            try:
                low, high = float(band[0]), float(band[1])
            except (TypeError, ValueError, IndexError):
                low, high = 0.0, 1.0
            offset = float(params.get("offset", 0.005))
            size = float(params.get("size", 10))
            if window in allowed:
                for outcome, token_key in (("UP", "up_token"), ("DOWN", "down_token")):
                    exists = conn.execute(
                        "SELECT 1 FROM live_orders WHERE strategy_id=? AND slug=? AND outcome=?",
                        (strategy["id"], slug, outcome),
                    ).fetchone()
                    if exists:
                        continue
                    token = market.get(token_key)
                    book = _latest_book(conn, slug, token)
                    if not book or book.get("mid") is None:
                        continue
                    mid = float(book["mid"])
                    if not (low <= mid <= high):
                        continue
                    quote = round(mid - offset, 2)
                    if quote <= 0:
                        continue
                    amount = min(size, settings.live_max_order_notional_usdc / quote)
                    if amount < settings.live_min_order_size:
                        continue
                    try:
                        placed = await executor.place_limit(token, "BUY", quote, amount)
                    except LiveError:
                        continue
                    conn.execute(
                        """UPDATE live_orders SET strategy_id=?, slug=?, condition_id=?,
                                                   outcome=?, token_id=?, updated_at=?
                           WHERE id=?""",
                        (strategy["id"], slug, market.get("condition_id"), outcome, token, now, placed["order_id"]),
                    )
                    conn.commit()

        # 4) Settle and enforce the daily loss limit.
        _settle(conn, strategy, now)
        refreshed = conn.execute(
            "SELECT * FROM live_strategies WHERE id=?", (strategy["id"],)
        ).fetchone()
        if refreshed and float(refreshed["realized_pnl"] or 0) <= -settings.live_daily_loss_limit_usdc:
            conn.execute(
                "UPDATE live_strategies SET active=0, status='STOPPED', stop_reason=?, updated_at=? WHERE id=?",
                (f"daily loss {float(refreshed['realized_pnl']):.2f}", now, strategy["id"]),
            )
            conn.commit()
            try:
                await executor.cancel_all("daily_loss")
            except LiveError:
                pass
        conn.execute(
            "UPDATE live_strategies SET last_tick_at=?, updated_at=? WHERE id=?",
            (now, now, strategy["id"]),
        )
        conn.commit()
        return {"ok": True, "slug": slug, "strategy": strategy["id"]}
    finally:
        conn.close()


async def live_strategy_loop() -> None:
    settings = get_settings()
    while True:
        try:
            await live_tick()
        except Exception as exc:
            _record_event("ERROR", f"live strategy tick failed: {_redact(str(exc))}")
        await asyncio.sleep(max(2.0, settings.live_strategy_tick_seconds))
