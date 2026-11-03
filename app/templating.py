from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi.templating import Jinja2Templates

from .config import get_settings

settings = get_settings()
templates = Jinja2Templates(directory=str(settings.templates_dir))


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def money(value: Any, digits: int = 2) -> str:
    number = _float(value)
    if number is None:
        return "—"
    sign = "-" if number < 0 else ""
    return f"{sign}${abs(number):,.{digits}f}"


def num(value: Any, digits: int = 0) -> str:
    number = _float(value)
    if number is None:
        return "—"
    return f"{number:,.{digits}f}"


def compact(value: Any, digits: int = 1) -> str:
    number = _float(value)
    if number is None:
        return "—"
    absolute = abs(number)
    for unit, size in (("B", 1_000_000_000), ("M", 1_000_000), ("K", 1_000)):
        if absolute >= size:
            return f"{number / size:,.{digits}f}{unit}"
    return f"{number:,.{digits}f}"


def pct(value: Any, digits: int = 1) -> str:
    number = _float(value)
    if number is None:
        return "—"
    return f"{number * 100:,.{digits}f}%"


def cents(value: Any, digits: int = 2, sign: bool = True) -> str:
    number = _float(value)
    if number is None:
        return "—"
    return f"{number * 100:+.{digits}f}¢" if sign else f"{number * 100:.{digits}f}¢"


def cents_raw(value: Any, digits: int = 2) -> str:
    number = _float(value)
    if number is None:
        return "—"
    return f"{number * 100:.{digits}f}¢"


def price(value: Any, digits: int = 3) -> str:
    number = _float(value)
    if number is None:
        return "—"
    return f"{number:.{digits}f}"


def ts(value: Any, fmt: str = "%Y-%m-%d %H:%M UTC") -> str:
    number = _float(value)
    if not number:
        return "—"
    return datetime.fromtimestamp(number, timezone.utc).strftime(fmt)


def day(value: Any) -> str:
    return ts(value, "%Y-%m-%d")


def short_addr(value: Any) -> str:
    text = str(value or "")
    if len(text) <= 12:
        return text
    return f"{text[:6]}…{text[-4:]}"


def duration(seconds: Any) -> str:
    number = _float(seconds)
    if number is None:
        return "—"
    if number < 60:
        return f"{number:.0f}s"
    if number < 3600:
        return f"{number / 60:.1f}m"
    if number < 86400:
        return f"{number / 3600:.1f}h"
    return f"{number / 86400:.1f}d"


def delta_class(value: Any) -> str:
    number = _float(value)
    if number is None:
        return "muted"
    return "positive" if number > 0 else ("negative" if number < 0 else "muted")


def tone(value: Any) -> str:
    text = str(value or "")
    if any(key in text for key in ("做市", "返佣", "maker", "Maker")):
        return "maker"
    if any(key in text for key in ("配对", "pair")):
        return "pair"
    if any(key in text for key in ("taker", "Taker", "方向")):
        return "taker"
    if any(key in text for key in ("长尾", "波段", "tail", "swing")):
        return "tail"
    return "neutral"


def status_tone(value: Any) -> str:
    text = str(value or "").upper()
    if text in ("DONE", "OK", "SUCCESS"):
        return "ok"
    if text in ("RUNNING", "QUEUED", "PENDING"):
        return "warn"
    if text in ("ERROR", "FAILED"):
        return "bad"
    return "neutral"


templates.env.filters.update(
    {
        "money": money,
        "num": num,
        "compact": compact,
        "pct": pct,
        "cents": cents,
        "cents_raw": cents_raw,
        "price": price,
        "ts": ts,
        "day": day,
        "short_addr": short_addr,
        "duration": duration,
        "delta_class": delta_class,
        "tone": tone,
        "status_tone": status_tone,
    }
)
templates.env.globals.update(
    app_name=settings.app_name,
    app_version=settings.app_version,
    target_address=settings.target_address,
)
