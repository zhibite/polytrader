"""Container entrypoint.

Coolify's Traefik route and the injected ``PORT`` env var can disagree
(``PORT=8000`` while the proxy still routes 8021). To make the deployment
robust regardless of which side of that config is stale, bind both ports in
one uvicorn process by default.

Override with ``LAB_BIND_PORTS=8021`` (or any comma-separated list) if a
single port is preferred.
"""

from __future__ import annotations

import logging
import os
import socket

import uvicorn

from .main import app

log = logging.getLogger("polytrader.entrypoint")


def _bind(port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", port))
    sock.listen(2048)
    sock.set_inheritable(True)
    return sock


def main() -> None:
    raw = os.environ.get("LAB_BIND_PORTS", "8000,8021")
    ports: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ports.append(int(part))
        except ValueError:
            continue
    if not ports:
        ports = [8000]
    sockets: list[socket.socket] = []
    for port in ports:
        try:
            sockets.append(_bind(port))
            log.info("listening on 0.0.0.0:%s", port)
        except OSError as exc:
            log.warning("cannot bind 0.0.0.0:%s: %s", port, exc)
    if not sockets:
        raise SystemExit("no listen sockets could be bound")
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=ports[0],
        workers=1,
        log_level=os.environ.get("LAB_LOG_LEVEL", "info"),
        proxy_headers=True,
        forwarded_allow_ips="*",
    )
    uvicorn.Server(config).run(sockets=sockets)


if __name__ == "__main__":
    main()
