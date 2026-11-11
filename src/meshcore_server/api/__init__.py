"""HTTP API (FastAPI) a jeho běh uvnitř smyčky serveru."""

from __future__ import annotations

import contextlib
import logging

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .. import __version__
from .context import ServerContext
from .routes import health_router, router
from .ws import router as ws_router

logger = logging.getLogger("meshcore_server.api")


def create_app(ctx: ServerContext) -> FastAPI:
    docs = ctx.cfg.server.docs_enabled
    app = FastAPI(
        title="meshcore-server",
        version=__version__,
        docs_url="/docs" if docs else None,
        redoc_url=None,
        openapi_url="/openapi.json" if docs else None,
    )
    app.state.ctx = ctx
    if ctx.cfg.server.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=ctx.cfg.server.cors_origins,
            allow_methods=["*"],
            allow_headers=["Authorization", "Content-Type"],
        )
    app.include_router(health_router)
    app.include_router(router)
    app.include_router(ws_router)
    return app


class _EmbeddedServer(uvicorn.Server):
    """Uvicorn bez vlastní obsluhy signálů – ukončení řídí Application."""

    @contextlib.contextmanager
    def capture_signals(self):
        yield


def build_server(ctx: ServerContext) -> uvicorn.Server:
    cfg = ctx.cfg.server
    config = uvicorn.Config(
        create_app(ctx),
        host=cfg.host,
        port=cfg.port,
        log_config=None,          # použije naše nastavení logování
        access_log=False,
        proxy_headers=True,
        forwarded_allow_ips=",".join(cfg.trusted_proxies) or "127.0.0.1",
        ws_ping_interval=20,
        ws_ping_timeout=20,
    )
    return _EmbeddedServer(config)
