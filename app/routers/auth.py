from __future__ import annotations

import time
from typing import Annotated

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

from ..auth import (
    COOKIE_NAME,
    MAX_AGE_SECONDS,
    credentials_ok,
    issue_session,
    read_session,
    safe_next,
)
from ..config import get_settings
from ..templating import templates

router = APIRouter()
settings = get_settings()


def _login_page(request: Request, next_path: str, error: str | None = None, username: str = ""):
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={
            "request": request,
            "app_name": settings.app_name,
            "app_version": settings.app_version,
            "next": next_path,
            "error": error,
            "username": username,
        },
    )


@router.get("/login")
def login_form(request: Request, next: str = "/"):
    if not settings.auth_enabled:
        return RedirectResponse("/", status_code=303)
    next_path = safe_next(next)
    if read_session(request.cookies.get(COOKIE_NAME)):
        return RedirectResponse(next_path, status_code=303)
    return _login_page(request, next_path)


@router.post("/login")
def login_submit(
    request: Request,
    username: Annotated[str, Form()] = "",
    password: Annotated[str, Form()] = "",
    next: Annotated[str, Form()] = "/",
):
    next_path = safe_next(next)
    if credentials_ok(username.strip(), password):
        response = RedirectResponse(next_path, status_code=303)
        response.set_cookie(
            COOKIE_NAME,
            issue_session(username.strip()),
            max_age=MAX_AGE_SECONDS,
            httponly=True,
            samesite="lax",
            secure=settings.cookie_secure,
            path="/",
        )
        return response
    # Slow down trivial credential stuffing a little.
    time.sleep(0.35)
    return _login_page(request, next_path, "用户名或密码不正确", username.strip())


@router.get("/logout")
def logout(request: Request):
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(COOKIE_NAME, path="/")
    return response
