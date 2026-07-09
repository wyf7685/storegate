import contextlib
import sys
import traceback
from contextlib import AbstractAsyncContextManager
from copy import deepcopy

import anyio.lowlevel
from a2wsgi import WSGIMiddleware
from a2wsgi.asgi_typing import Receive, Scope, Send
from a2wsgi.wsgi_typing import WSGIApp
from wsgidav.wsgidav_app import WsgiDAVApp

from app.log import LOGGING_CONFIG
from app.storage import AbstractStorage

from .provider import StorageProvider
from .utils import current_event_loop_token


def create_wsgi_app(storage: AbstractStorage, host: str, port: int) -> WsgiDAVApp:
    provider = StorageProvider(storage)
    config = {
        "host": host,
        "port": port,
        "provider_mapping": {"/": provider},
        "simple_dc": {"user_mapping": {"*": True}},
        "verbose": 3,
    }
    return WsgiDAVApp(config)


def configure_logging() -> None:
    import logging.config

    config = deepcopy(LOGGING_CONFIG)
    config["loggers"] = loggers = {}

    for name in sys.modules:
        if name.startswith("wsgidav"):
            loggers[name] = {"handlers": ["default"], "level": "INFO", "propagate": False}

    logging.config.dictConfig(config)


class WSGIMiddlewareWithLifespan(WSGIMiddleware):
    def __init__(
        self,
        app: WSGIApp,
        workers: int = 10,
        send_queue_size: int = 10,
        lifespan: AbstractAsyncContextManager[object] | None = None,
    ) -> None:
        super().__init__(app, workers=workers, send_queue_size=send_queue_size)
        self.lifespan = lifespan

    async def handle_lifespan(self, receive: Receive, send: Send) -> None:
        # Lifespan handler from starlette.routing:Router.lifespan

        started = False
        await receive()
        try:
            async with self.lifespan or contextlib.nullcontext():
                await send({"type": "lifespan.startup.complete"})
                started = True
                await receive()
        except BaseException:
            exc_text = traceback.format_exc()
            if started:
                await send({"type": "lifespan.shutdown.failed", "message": exc_text})
            else:
                await send({"type": "lifespan.startup.failed", "message": exc_text})
            raise
        else:
            await send({"type": "lifespan.shutdown.complete"})

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self.handle_lifespan(receive, send)
            return

        await super().__call__(scope, receive, send)


class DAVServer:
    def __init__(
        self,
        storage: AbstractStorage,
        host: str = "127.0.0.1",
        port: int = 8080,
    ):
        self.storage = storage
        self.host = host
        self.port = port

    async def serve(self):
        import uvicorn

        configure_logging()
        wsgi_app = create_wsgi_app(self.storage, self.host, self.port)
        app = WSGIMiddlewareWithLifespan(wsgi_app, lifespan=self.storage)
        config = uvicorn.Config(
            app,
            host=self.host,
            port=self.port,
            log_level="info",
            log_config=LOGGING_CONFIG,
            lifespan="on",
            interface="asgi3",
        )
        server = uvicorn.Server(config)

        with current_event_loop_token.set(anyio.lowlevel.current_token()):
            await server.serve()
