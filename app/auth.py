from __future__ import annotations

import base64
import binascii
import hmac
from typing import Any

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from .config import get_settings

COOKIE_NAME = "lab_session"
MAX_AGE_SECONDS = 7 * 24 * 3600


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(
        get_settings().session_secret_key,
        salt="trader-strategy-lab-session",
    )


def issue_session(user: str) -> str:
    return _serializer().dumps({"user": user})


def read_session(token: str | None) -> str | None:
    if not token:
        return None
    try:
        payload: Any = _serializer().loads(token, max_age=MAX_AGE_SECONDS)
    except (BadSignature, SignatureExpired, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    user = payload.get("user")
    return str(user) if user else None


def credentials_ok(username: str | None, password: str | None) -> bool:
    settings = get_settings()
    if not settings.auth_enabled:
        return False
    if username is None or password is None:
        return False
    return hmac.compare_digest(username, settings.login_user or "") and hmac.compare_digest(
        password, settings.login_password or ""
    )


def parse_basic(header: str | None) -> tuple[str | None, str | None]:
    if not header or not header.lower().startswith("basic "):
        return None, None
    try:
        decoded = base64.b64decode(header[6:].strip()).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return None, None
    if ":" not in decoded:
        return None, None
    username, password = decoded.split(":", 1)
    return username, password


def safe_next(value: str | None) -> str:
    """Only allow local paths as post-login redirects."""
    if not value:
        return "/"
    if not value.startswith("/") or value.startswith("//") or "\\" in value:
        return "/"
    return value
