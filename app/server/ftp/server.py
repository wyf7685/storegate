"""FTP server — listens for connections and dispatches to handlers."""

from typing import TYPE_CHECKING, final, override

import anyio
import anyio.abc
from anyio.abc import SocketStream

from app.utils import logger_wrapper

from ..abstract import AbstractServer
from .handler import FTPHandler
from .session import FTPSession

if TYPE_CHECKING:
    from app.storage.abstract import AbstractStorage

logger = logger_wrapper("ftp.server")


@final
class FTPServer(AbstractServer):
    """Asynchronous FTP server backed by an ``AbstractStorage`` implementation.

    Usage::

        storage = MemoryStorage()
        server = FTPServer(storage, host="127.0.0.1", port=2121)
        await server.serve()
    """

    host: str
    port: int

    def __init__(
        self,
        storage: AbstractStorage,
        *,
        host: str = "127.0.0.1",
        port: int = 2121,
    ) -> None:
        super().__init__(storage)
        self.host = host
        self.port = port

    @override
    async def serve(self) -> None:
        """Start the FTP server. Blocks until cancelled."""
        async with self.storage:
            listener = await anyio.create_tcp_listener(local_host=self.host, local_port=self.port)
            logger.info(f"FTP server listening on <g><b>{self.host}</>:{self.port}</>")
            async with listener:
                await listener.serve(self._handle_client)

    async def _handle_client(self, stream: SocketStream) -> None:
        """Handle a single client connection."""
        peer = stream.extra_attributes.get(anyio.abc.SocketAttribute.remote_address, lambda: ("unknown", 0))()
        colored_peer = f"<g><b>{peer[0]}</>:{peer[1]}</>"
        logger.info(f"New connection from {colored_peer}")

        session = FTPSession()
        handler = FTPHandler(storage=self.storage, session=session, stream=stream, host=self.host)

        try:
            async with stream:
                await handler.run()
        except Exception:
            logger.exception("Unhandled client error")
        finally:
            # Clean up any lingering PASV listener
            if session.pasv_listener is not None:
                await session.pasv_listener.aclose()
            logger.info(f"Connection closed: {colored_peer}")
