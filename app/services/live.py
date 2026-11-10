"""Live CLOB execution layer (disabled by default).

This module provides the private-key signing / order placement / cancel /
reconcile / kill-switch capability. It is deliberately NOT wired to the
paper pipeline: the automated strategy only trades in the paper engine.

Enablement requires all of:
  LAB_TRADING_MODE=live
  LAB_LIVE_ENABLED=1
  LAB_LIVE_CONFIRM=I_UNDERSTAND_LIVE_TRADING
  private key + L2 API credentials (+ funder for proxy/safe wallets)
  no active kill switch

The SDK (py-clob-client-v2) is an optional dependency; without it the page
still works and reports "SDK missing".
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import secrets
import time
from pathlib import Path
from typing import Any

from ..config import get_settings
from ..db import connect, now_ts, to_json

log = logging.getLogger("polytrader.live")

KILL_SWITCH_RELEASE = "RELEASE_KILL_SWITCH"
LIVE_ORDER_STATUSES_OPEN = ("PLACED", "LIVE")


class LiveError(RuntimeError):
    pass


def _redact(text: str) -> str:
    settings = get_settings()
    out = str(text)
    for secret in (
        settings.live_private_key,
        settings.live_api_secret,
        settings.live_api_passphrase,
    ):
        if secret:
            out = out.replace(secret, "***")
    return out


def _record_event(level: str, message: str, raw: Any = None) -> None:
    try:
        conn = connect()
        try:
            conn.execute(
                "INSERT INTO live_events(ts, level, message, raw_json) VALUES(?,?,?,?)",
                (now_ts(), level, _redact(message)[:1000], to_json(raw) if raw is not None else None),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def kill_switch_path() -> Path:
    return get_settings().data_dir / "KILL_SWITCH"


def kill_switch_active() -> bool:
    if kill_switch_path().exists():
        return True
    conn = connect()
    try:
        row = conn.execute("SELECT value FROM live_state WHERE key='kill_switch'").fetchone()
        return bool(row and row["value"] == "1")
    finally:
        conn.close()


def _set_state(key: str, value: str) -> None:
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO live_state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def _get_state(key: str) -> str | None:
    conn = connect()
    try:
        row = conn.execute("SELECT value FROM live_state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None
    finally:
        conn.close()


class LiveExecutor:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._client = None
        self._client_error: str | None = None
        self._last_order_ts: list[float] = []

    # ── readiness ───────────────────────────────────────────────────────
    def configured_status(self) -> dict[str, Any]:
        s = self.settings
        sdk = importlib.util.find_spec("py_clob_client_v2") is not None
        reasons: list[str] = []
        if s.trading_mode != "live":
            reasons.append("trading_mode is not live")
        if not s.live_enabled:
            reasons.append("live_enabled is false")
        if (s.live_confirm or "") != s.live_confirm_phrase:
            reasons.append("live confirm phrase missing")
        if not s.live_private_key:
            reasons.append("private key missing")
        if not (s.live_api_key and s.live_api_secret and s.live_api_passphrase):
            reasons.append("L2 API credentials missing")
        if s.live_signature_type in (1, 2, 3) and not s.live_funder_address:
            reasons.append("funder address missing for proxy/safe signature type")
        if not sdk:
            reasons.append("py-clob-client-v2 not installed")
        if kill_switch_active():
            reasons.append("kill switch active")
        credential_reasons = []
        if not sdk:
            credential_reasons.append("py-clob-client-v2 not installed")
        if not s.live_private_key:
            credential_reasons.append("private key missing")
        if s.live_signature_type in (1, 2, 3) and not s.live_funder_address:
            credential_reasons.append("funder address missing for proxy/safe signature type")
        return {
            "mode": s.trading_mode,
            "enabled": s.live_enabled,
            "strategy_runner_enabled": s.live_strategy_enabled and s.live_enabled,
            "confirm_ok": (s.live_confirm or "") == s.live_confirm_phrase,
            "sdk_available": sdk,
            "private_key_set": bool(s.live_private_key),
            "api_creds_set": bool(s.live_api_key and s.live_api_secret and s.live_api_passphrase),
            "api_creds_can_derive": bool(s.live_private_key and sdk),
            "funder_set": bool(s.live_funder_address),
            "signature_type": s.live_signature_type,
            "credentials_ready": not credential_reasons,
            "credential_reasons": credential_reasons,
            "ready": not reasons,
            "reasons": reasons,
            "kill_switch": kill_switch_active(),
            "limits": {
                "max_order_notional_usdc": s.live_max_order_notional_usdc,
                "max_open_notional_usdc": s.live_max_open_notional_usdc,
                "max_orders_per_minute": s.live_max_orders_per_minute,
                "min_order_size": s.live_min_order_size,
            },
        }

    def _ensure_ready(self) -> None:
        status = self.configured_status()
        if not status["ready"]:
            raise LiveError("; ".join(status["reasons"]))
        if kill_switch_active():
            raise LiveError("kill switch is active")

    async def _get_client(self, require_trading: bool = True):
        if self._client is not None:
            return self._client
        if self._client_error:
            raise LiveError(self._client_error)
        if require_trading:
            self._ensure_ready()
        else:
            status = self.configured_status()
            if not status["credentials_ready"]:
                raise LiveError("; ".join(status["credential_reasons"]))
        try:
            from py_clob_client_v2 import ClobClient
            from py_clob_client_v2.clob_types import ApiCreds

            if self.settings.live_api_key and self.settings.live_api_secret and self.settings.live_api_passphrase:
                creds = ApiCreds(
                    api_key=self.settings.live_api_key,
                    api_secret=self.settings.live_api_secret,
                    api_passphrase=self.settings.live_api_passphrase,
                )
            else:
                from .live_auth import create_or_derive_clob_api_creds

                derived = await asyncio.to_thread(
                    create_or_derive_clob_api_creds,
                    self.settings.live_clob_host,
                    self.settings.live_private_key,
                    self.settings.live_chain_id,
                )
                creds = ApiCreds(
                    api_key=derived.api_key,
                    api_secret=derived.api_secret,
                    api_passphrase=derived.api_passphrase,
                )

            def _build():
                return ClobClient(
                    host=self.settings.live_clob_host,
                    chain_id=self.settings.live_chain_id,
                    key=self.settings.live_private_key,
                    creds=creds,
                    signature_type=self.settings.live_signature_type,
                    funder=self.settings.live_funder_address or None,
                )

            self._client = await asyncio.to_thread(_build)
        except Exception as exc:
            self._client_error = _redact(f"CLOB client init failed: {exc}")
            _record_event("ERROR", self._client_error)
            raise LiveError(self._client_error) from exc
        return self._client

    # ── risk checks ─────────────────────────────────────────────────────
    def _validate_order(self, token_id: str, side: str, price: float, size: float) -> float:
        side = side.upper()
        if side not in ("BUY", "SELL"):
            raise LiveError("side must be BUY or SELL")
        if not token_id:
            raise LiveError("token_id is required")
        if not (0.0 < price < 1.0):
            raise LiveError("price must be between 0 and 1")
        if size < self.settings.live_min_order_size:
            raise LiveError(f"size below CLOB minimum ({self.settings.live_min_order_size})")
        notional = price * size
        if notional > self.settings.live_max_order_notional_usdc:
            raise LiveError(f"order notional ${notional:.2f} exceeds limit")
        conn = connect()
        try:
            row = conn.execute(
                "SELECT COALESCE(SUM(price * size),0) AS n FROM live_orders "
                "WHERE status IN ('PLACED','LIVE','MISSING')"
            ).fetchone()
            position_row = conn.execute(
                "SELECT COALESCE(SUM(cost),0) AS n FROM live_positions WHERE status='OPEN'"
            ).fetchone()
            open_notional = float(row["n"] or 0) + float(position_row["n"] or 0)
        finally:
            conn.close()
        if open_notional + notional > self.settings.live_max_open_notional_usdc:
            raise LiveError(f"open notional ${open_notional:.2f} + ${notional:.2f} exceeds limit")
        cutoff = time.time() - 60
        self._last_order_ts = [ts for ts in self._last_order_ts if ts >= cutoff]
        if len(self._last_order_ts) >= self.settings.live_max_orders_per_minute:
            raise LiveError("orders-per-minute limit reached")
        return notional

    def _record_order(self, token_id: str, side: str, price: float, size: float,
                      exchange_order_id: str | None, status: str, raw: Any = None) -> str:
        order_id = f"live_{secrets.token_hex(6)}"
        conn = connect()
        try:
            conn.execute(
                """INSERT INTO live_orders(
                     id, exchange_order_id, token_id, side, price, size, status,
                     created_at, updated_at, raw_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    order_id,
                    exchange_order_id,
                    token_id,
                    side.upper(),
                    price,
                    size,
                    status,
                    now_ts(),
                    now_ts(),
                    to_json(raw) if raw is not None else None,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        self._last_order_ts.append(time.time())
        return order_id

    # ── orders ──────────────────────────────────────────────────────────
    async def place_limit(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
    ) -> dict[str, Any]:
        """Place a post-only GTC limit order. Live must be explicitly enabled."""
        self._ensure_ready()
        self._validate_order(token_id, side, price, size)
        client = await self._get_client()
        try:
            from py_clob_client_v2 import OrderType, Side as ClobSide
            from py_clob_client_v2.clob_types import (
                OrderArgsV2,
                PartialCreateOrderOptions,
            )

            tick_size = "0.01"
            get_tick = getattr(client, "get_tick_size", None)
            if get_tick is not None:
                try:
                    value = await asyncio.to_thread(get_tick, token_id)
                    if value:
                        tick_size = str(value)
                except Exception:
                    pass
            args = OrderArgsV2(
                token_id=token_id,
                price=float(price),
                size=float(size),
                side=ClobSide.BUY if side.upper() == "BUY" else ClobSide.SELL,
                expiration=0,
            )
            options = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=False)
            response = await asyncio.to_thread(
                client.create_and_post_order,
                order_args=args,
                options=options,
                order_type=OrderType.GTC,
                post_only=True,
            )
            exchange_id = str(
                (response or {}).get("orderID") or (response or {}).get("order_id") or ""
            )
            if not exchange_id:
                error = str((response or {}).get("errorMsg") or (response or {}).get("error") or "no orderID")
                _record_event("ERROR", f"order rejected: {error}", response)
                raise LiveError(f"order rejected: {_redact(error)}")
            order_id = self._record_order(
                token_id, side, price, size, exchange_id, "LIVE", response
            )
            _record_event("INFO", f"order placed {order_id} {side} {size}@{price}", response)
            return {
                "ok": True,
                "order_id": order_id,
                "exchange_order_id": exchange_id,
                "status": "LIVE",
                "raw": response,
            }
        except LiveError:
            raise
        except Exception as exc:
            message = _redact(f"place_limit failed: {exc}")
            _record_event("ERROR", message)
            raise LiveError(message) from exc

    async def refresh_order(self, order_id: str) -> dict[str, Any]:
        """Poll one order; return filled_size/status and update the local row."""
        client = await self._get_client(require_trading=False)
        conn = connect()
        try:
            row = conn.execute(
                "SELECT * FROM live_orders WHERE id=? OR exchange_order_id=? LIMIT 1",
                (order_id, order_id),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            raise LiveError("order not found")
        exchange_id = row["exchange_order_id"] or order_id
        try:
            response = await asyncio.to_thread(client.get_order, exchange_id)
            response = response or {}
            filled = float(response.get("size_matched") or response.get("sizeMatched") or 0)
            status = str(response.get("status") or row["status"] or "LIVE").upper()
            conn = connect()
            try:
                conn.execute(
                    "UPDATE live_orders SET filled_size=?, status=?, updated_at=?, raw_json=? WHERE id=?",
                    (filled, status, now_ts(), to_json(response), row["id"]),
                )
                conn.commit()
            finally:
                conn.close()
            return {
                "ok": True,
                "order_id": row["id"],
                "exchange_order_id": exchange_id,
                "filled_size": filled,
                "status": status,
                "raw": response,
            }
        except Exception as exc:
            message = _redact(f"refresh_order failed: {exc}")
            _record_event("ERROR", message)
            raise LiveError(message) from exc

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        client = await self._get_client(require_trading=False)
        try:
            from py_clob_client_v2.clob_types import OrderPayload

            conn = connect()
            try:
                row = conn.execute(
                    "SELECT * FROM live_orders WHERE id=? OR exchange_order_id=? LIMIT 1",
                    (order_id, order_id),
                ).fetchone()
            finally:
                conn.close()
            exchange_id = (row["exchange_order_id"] if row else None) or order_id
            await asyncio.to_thread(client.cancel_order, OrderPayload(orderID=exchange_id))
            conn = connect()
            try:
                conn.execute(
                    "UPDATE live_orders SET status='CANCELLED', updated_at=? "
                    "WHERE id=? OR exchange_order_id=?",
                    (now_ts(), order_id, exchange_id),
                )
                conn.commit()
            finally:
                conn.close()
            _record_event("INFO", f"order cancelled {exchange_id}")
            return {"ok": True, "order_id": exchange_id}
        except Exception as exc:
            message = _redact(f"cancel_order failed: {exc}")
            _record_event("ERROR", message)
            raise LiveError(message) from exc

    async def cancel_all(self, reason: str = "manual") -> dict[str, Any]:
        client = await self._get_client(require_trading=False)
        conn = connect()
        try:
            rows = conn.execute(
                "SELECT * FROM live_orders WHERE status IN ('PLACED','LIVE','MISSING')"
            ).fetchall()
        finally:
            conn.close()
        ids = [row["exchange_order_id"] for row in rows if row["exchange_order_id"]]
        if not ids:
            return {"ok": True, "cancelled": [], "reason": reason}
        try:
            result = await asyncio.to_thread(
                client.cancel_orders, [{"orderID": order_id} for order_id in ids]
            )
            cancelled = ids
            not_cancelled: list[str] = []
            if isinstance(result, dict):
                raw_cancelled = result.get("canceled")
                raw_not = result.get("not_canceled") or []
                if isinstance(raw_cancelled, list):
                    cancelled = [str(item) for item in raw_cancelled]
                not_cancelled = [str(item) for item in raw_not if item]
            conn = connect()
            try:
                if cancelled:
                    conn.execute(
                        "UPDATE live_orders SET status='CANCELLED', updated_at=? "
                        "WHERE exchange_order_id IN ({})".format(",".join("?" * len(cancelled))),
                        [now_ts(), *cancelled],
                    )
                conn.commit()
            finally:
                conn.close()
            if not_cancelled:
                _record_event(
                    "WARNING",
                    f"cancel_all ({reason}): {len(not_cancelled)} orders not cancelled",
                    not_cancelled,
                )
            _record_event("INFO", f"cancel_all ({reason}): {len(cancelled)} cancelled", result)
            return {"ok": True, "requested": ids, "cancelled": cancelled, "raw": result}
        except Exception as exc:
            message = _redact(f"cancel_all failed: {exc}")
            _record_event("ERROR", message)
            raise LiveError(message) from exc

    # ── reconcile ───────────────────────────────────────────────────────
    async def reconcile(self) -> dict[str, Any]:
        client = await self._get_client(require_trading=False)
        try:
            get_open = getattr(client, "get_open_orders", None) or getattr(client, "get_orders", None)
            if get_open is None:
                raise LiveError("SDK has no get_open_orders/get_orders")
            orders = await asyncio.to_thread(get_open)
            if orders is None:
                raise LiveError("open-orders query returned None")
            server_ids = set()
            conn = connect()
            try:
                for order in orders:
                    exchange_id = str(order.get("id") or order.get("orderID") or "")
                    if not exchange_id:
                        continue
                    server_ids.add(exchange_id)
                    token = str(order.get("asset_id") or order.get("assetId") or order.get("token_id") or "")
                    side = str(order.get("side") or "")
                    price = float(order.get("price") or 0)
                    size = float(order.get("original_size") or order.get("size") or 0)
                    status = str(order.get("status") or "LIVE").upper()
                    existing = conn.execute(
                        "SELECT id FROM live_orders WHERE exchange_order_id=?",
                        (exchange_id,),
                    ).fetchone()
                    if existing:
                        conn.execute(
                            "UPDATE live_orders SET status=?, updated_at=?, raw_json=? WHERE id=?",
                            (status, now_ts(), to_json(order), existing["id"]),
                        )
                    else:
                        conn.execute(
                            """INSERT INTO live_orders(
                                 id, exchange_order_id, token_id, side, price, size, status,
                                 created_at, updated_at, raw_json)
                               VALUES(?,?,?,?,?,?,?,?,?,?)""",
                            (
                                f"live_{secrets.token_hex(6)}",
                                exchange_id,
                                token,
                                side,
                                price,
                                size,
                                status,
                                now_ts(),
                                now_ts(),
                                to_json(order),
                            ),
                        )
                local_open = conn.execute(
                    "SELECT * FROM live_orders WHERE status IN ('PLACED','LIVE','MISSING')"
                ).fetchall()
                for row in local_open:
                    if row["exchange_order_id"] and row["exchange_order_id"] not in server_ids:
                        conn.execute(
                            "UPDATE live_orders SET status='MISSING', updated_at=? WHERE id=?",
                            (now_ts(), row["id"]),
                        )
                conn.commit()
            finally:
                conn.close()
            balance = None
            try:
                from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

                response = await asyncio.to_thread(
                    client.get_balance_allowance,
                    BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
                )
                if isinstance(response, dict):
                    balance = {
                        "balance": float(response.get("balance") or 0) / 1_000_000,
                        "allowance": float(response.get("allowance") or 0) / 1_000_000,
                    }
                    _set_state("balance_json", json.dumps(balance))
            except Exception as exc:
                _record_event("WARNING", f"balance query failed: {_redact(str(exc))}")
            _set_state("last_reconcile", str(now_ts()))
            _record_event("INFO", f"reconcile ok: {len(orders)} open orders")
            return {"ok": True, "open_orders": len(orders), "balance": balance}
        except Exception as exc:
            message = _redact(f"reconcile failed: {exc}")
            _record_event("ERROR", message)
            raise LiveError(message) from exc

    # ── kill switch ─────────────────────────────────────────────────────
    async def engage_kill(self, reason: str = "manual") -> dict[str, Any]:
        kill_switch_path().parent.mkdir(parents=True, exist_ok=True)
        kill_switch_path().write_text(json.dumps({"since": now_ts(), "reason": reason}), encoding="utf-8")
        _set_state("kill_switch", "1")
        _set_state("kill_reason", reason)
        _record_event("WARNING", f"kill switch engaged: {reason}")
        cancelled = None
        if self.configured_status().get("credentials_ready"):
            try:
                cancelled = await self.cancel_all(reason="kill_switch")
            except Exception as exc:
                _record_event("ERROR", f"kill switch cancel_all failed: {_redact(str(exc))}")
        return {"ok": True, "kill_switch": True, "reason": reason, "cancel": cancelled}

    def release_kill(self, confirm: str) -> dict[str, Any]:
        if confirm != KILL_SWITCH_RELEASE:
            raise LiveError(f"confirm must be {KILL_SWITCH_RELEASE}")
        try:
            kill_switch_path().unlink()
        except FileNotFoundError:
            pass
        _set_state("kill_switch", "0")
        _set_state("kill_reason", "")
        _record_event("WARNING", "kill switch released")
        return {"ok": True, "kill_switch": False}

    # ── status for UI ───────────────────────────────────────────────────
    def status(self) -> dict[str, Any]:
        conn = connect()
        try:
            open_orders = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM live_orders WHERE status IN ('PLACED','LIVE','MISSING') "
                    "ORDER BY created_at DESC LIMIT 100"
                ).fetchall()
            ]
            events = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM live_events ORDER BY id DESC LIMIT 30"
                ).fetchall()
            ]
            order_count = conn.execute(
                "SELECT COUNT(*) AS n FROM live_orders"
            ).fetchone()["n"]
        finally:
            conn.close()
        balance = None
        try:
            raw = _get_state("balance_json")
            balance = json.loads(raw) if raw else None
        except Exception:
            balance = None
        return {
            **self.configured_status(),
            "balance": balance,
            "last_reconcile": _get_state("last_reconcile"),
            "order_count": order_count,
            "open_orders": open_orders,
            "events": events,
            "kill_reason": _get_state("kill_reason"),
        }


executor = LiveExecutor()
