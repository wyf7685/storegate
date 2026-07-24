import contextlib
import traceback
from contextlib import AbstractAsyncContextManager
from typing import final, override

import anyio.lowlevel
from a2wsgi import WSGIMiddleware
from a2wsgi.asgi_typing import Receive, Scope, Send
from a2wsgi.wsgi_typing import WSGIApp
from wsgidav.wsgidav_app import WsgiDAVApp

from storegate.storage import AbstractStorage

from ..abstract import AbstractServer, _is_loopback
from .provider import StorageProvider
from .utils import current_event_loop_token


def create_wsgi_app(
    storage: AbstractStorage,
    host: str,
    port: int,
    *,
    read_only: bool = False,
) -> WsgiDAVApp:
    provider = StorageProvider(storage, read_only=read_only)
    config = {
        "host": host,
        "port": port,
        "provider_mapping": {"/": provider},
        "simple_dc": {"user_mapping": {"*": True}},
        "verbose": 3,
        "logging": {"enable": False},
    }
    return WsgiDAVApp(config)


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


@final
class DAVServer(AbstractServer):
    """Asynchronous WebDAV server backed by an ``AbstractStorage`` implementation.

    Usage::

        storage = MemoryStorage()
        server = DAVServer(storage, host="127.0.0.1", port=8080)
        await server.serve()
    """

    host: str
    port: int

    def __init__(
        self,
        storage: AbstractStorage,
        *,
        host: str = "127.0.0.1",
        port: int = 8080,
        read_only: bool = False,
        allow_insecure_public: bool = False,
    ) -> None:
        super().__init__(storage)
        self.host = host
        self.port = port
        self.read_only = read_only

        if not _is_loopback(host) and not allow_insecure_public:
            raise ValueError(
                f"Binding DAV server to {host!r} exposes anonymous access without "
                "authentication. Set allow_insecure_public=True to confirm this "
                "is intentional."
            )

    @override
    async def serve(self) -> None:
        import uvicorn

        wsgi_app = create_wsgi_app(self.storage, self.host, self.port, read_only=self.read_only)
        app = WSGIMiddlewareWithLifespan(wsgi_app, lifespan=self.storage)
        config = uvicorn.Config(
            app,
            host=self.host,
            port=self.port,
            log_level="info",
            log_config=None,
            lifespan="on",
            interface="asgi3",
        )
        server = uvicorn.Server(config)

        with current_event_loop_token.set(anyio.lowlevel.current_token()):
            await server.serve()
