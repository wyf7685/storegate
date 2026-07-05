"""FTP server — listens for connections and dispatches to handlers."""

from typing import TYPE_CHECKING

import anyio
import anyio.abc
from anyio.abc import SocketStream

from app.utils import logger_wrapper

from .handler import FTPHandler
from .session import FTPSession

if TYPE_CHECKING:
    from app.storage.abstract import AbstractStorage

logger = logger_wrapper("ftp.server")


class FTPServer:
    """Asynchronous FTP server backed by an ``AbstractStorage`` implementation.

    Usage::

        storage = MemoryStorage()
        server = FTPServer(storage, host="127.0.0.1", port=2121)
        await server.serve()
    """

    _storage: AbstractStorage
    _host: str
    _port: int

    def __init__(self, storage: AbstractStorage, *, host: str = "127.0.0.1", port: int = 2121) -> None:
        self._storage = storage
        self._host = host
        self._port = port

    async def serve(self) -> None:
        """Start the FTP server. Blocks until cancelled."""
        async with self._storage:
            listener = await anyio.create_tcp_listener(local_host=self._host, local_port=self._port)
            logger.info(f"FTP server listening on <g><b>{self._host}</>:{self._port}</>")
            async with listener:
                await listener.serve(self._handle_client)

    async def _handle_client(self, stream: SocketStream) -> None:
        """Handle a single client connection."""
        peer = stream.extra_attributes.get(anyio.abc.SocketAttribute.remote_address, lambda: ("unknown", 0))()
        logger.info(f"New connection from <g><b>{peer[0]}</>:{peer[1]}</>")

        session = FTPSession()
        handler = FTPHandler(storage=self._storage, session=session, stream=stream, host=self._host)

        try:
            async with stream:
                await handler.run()
        except Exception:
            logger.exception("Unhandled client error")
        finally:
            # Clean up any lingering PASV listener
            if session.pasv_listener is not None:
                await session.pasv_listener.aclose()
            logger.info(f"Connection closed: <g><b>{peer[0]}</>:{peer[1]}</>")
