"""FTP data connection management (PASV and PORT modes)."""

import socket
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Self

import anyio
import anyio.lowlevel
from anyio.abc import SocketListener, SocketStream


class DataConnection:
    """Manages a single FTP data transfer channel.

    Lifecycle: setup → connect → transfer → close.
    """

    _mode: str | None = None
    _raw_socket: socket.socket | None = None
    _listener: SocketListener | None = None
    _stream: SocketStream | None = None

    # PORT mode client address
    _host: str = ""
    _port: int = 0

    async def setup_pasv(self, host: str) -> int:
        """Create a TCP listener on a random port for passive mode.

        Returns:
            The assigned port number (for the 227 response).
        """
        self._mode = "pasv"

        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw.setblocking(False)
        raw.bind((host, 0))
        raw.listen(65536)

        port: int = raw.getsockname()[1]
        self._raw_socket = raw
        self._listener = await SocketListener.from_socket(raw)
        return port

    def setup_port(self, host: str, port: int) -> None:
        """Store the client's address for active (PORT) mode."""
        self._mode = "port"
        self._host = host
        self._port = port

    async def connect(self) -> None:
        """Establish the data connection (accept for PASV, connect for PORT)."""
        if self._mode == "pasv" and self._listener is not None:
            await self._accept_one()
        elif self._mode == "port":
            self._stream = await anyio.connect_tcp(self._host, self._port)
        else:
            raise RuntimeError("No data channel configured (use PASV or PORT first)")

    async def _accept_one(self) -> None:
        """Accept exactly one connection from the PASV listener."""
        assert self._listener is not None

        with anyio.move_on_after(30) as scope:
            self._stream = await self._listener.accept()

        if scope.cancel_called:
            raise TimeoutError("Client did not connect to PASV data port within 30 seconds")

    async def send(self, data: bytes) -> None:
        """Send raw bytes through the data connection."""
        if self._stream is None:
            raise RuntimeError("Data connection not established")
        await self._stream.send(data)

    async def send_all(self, data: str) -> None:
        """Send a string (encoded as UTF-8) through the data connection."""
        await self.send(data.encode("utf-8"))

    async def receive(self, max_bytes: int = 65536) -> bytes:
        """Receive raw bytes from the data connection."""
        if self._stream is None:
            raise RuntimeError("Data connection not established")
        return await self._stream.receive(max_bytes)

    def receive_chunks(self, chunk_size: int = 65536) -> _DataStreamReader:
        """Return an async iterable that yields chunks from the data connection."""
        if self._stream is None:
            raise RuntimeError("Data connection not established")
        return _DataStreamReader(self._stream, chunk_size)

    def detach_listener(self) -> SocketListener | None:
        """Return and relinquish the current listener for ownership transfer."""
        listener, self._listener = self._listener, None
        return listener

    @classmethod
    def from_session_state(
        cls,
        *,
        mode: str,
        listener: SocketListener | None = None,
        host: str = "",
        port: int = 0,
    ) -> DataConnection:
        """Create a pre-configured DataConnection from session state."""
        self = cls.__new__(cls)
        self._mode = mode
        self._listener = listener
        self._host = host
        self._port = port
        self._stream = None
        self._raw_socket = None
        return self

    async def close(self) -> None:
        """Close the data stream and/or listener. Idempotent."""
        if self._stream is not None:
            await self._stream.aclose()
            self._stream = None
        if self._listener is not None:
            await self._listener.aclose()
            self._listener = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()


class _DataStreamReader:
    """Async iterable wrapper around a data stream's receive()."""

    def __init__(self, stream: SocketStream, chunk_size: int = 65536) -> None:
        self._stream = stream
        self._chunk_size = chunk_size

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        try:
            chunk = await self._stream.receive(self._chunk_size)
        except anyio.EndOfStream:
            raise StopAsyncIteration from None
        if not chunk:
            raise StopAsyncIteration
        await anyio.lowlevel.checkpoint()
        return chunk


async def read_all_from_stream(stream: SocketStream, chunk_size: int = 65536) -> bytearray:
    """Read all data from a socket stream into a bytearray."""
    buf = bytearray()
    while True:
        try:
            chunk = await stream.receive(chunk_size)
        except anyio.EndOfStream:
            break
        if not chunk:
            break
        buf.extend(chunk)
    return buf
