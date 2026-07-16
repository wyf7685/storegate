import asyncio
import queue
import socket
import threading
import time
from collections.abc import Generator

import pytest


@pytest.fixture(scope="session")
def _dav_server() -> Generator[str]:
    """Start a local wsgidav server backed by MemoryStorage."""
    from app.server.dav.server import DAVServer
    from app.storage.memory import MemoryStorage

    stop_event = asyncio.Event()

    async def _runner(port: int, event: asyncio.Event) -> None:
        server = DAVServer(MemoryStorage("/"), host="127.0.0.1", port=port)
        server_task = asyncio.create_task(server.serve())
        stop_task = asyncio.create_task(event.wait())
        _, pending = await asyncio.wait((server_task, stop_task), return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    thread = threading.Thread(target=lambda: asyncio.run(_runner(port, stop_event)), daemon=True)
    thread.start()
    _wait_for_port("127.0.0.1", port)

    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        stop_event.set()
        thread.join(timeout=5)


def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"Server did not start on {host}:{port}")


@pytest.fixture(scope="session")
def _ftp_server() -> Generator[tuple[str, int]]:
    """Start a local aioftp server in a dedicated thread and event loop."""
    from app.server.ftp import FTPServer
    from app.storage.memory import MemoryStorage

    host = "127.0.0.1"
    stop_event = threading.Event()
    startup: queue.Queue[int | BaseException] = queue.Queue(maxsize=1)
    errors: list[BaseException] = []

    async def _runner() -> None:
        storage = MemoryStorage("/")
        server = FTPServer(storage, host=host, port=0)
        async with storage:
            await server.server.start(server.host, server.port)
            startup.put(server.server.server_port)
            try:
                await asyncio.to_thread(stop_event.wait)
            finally:
                await server.server.close()

    def _thread_main() -> None:
        try:
            asyncio.run(_runner())
        except BaseException as exc:
            if startup.empty():
                startup.put(exc)
            else:
                errors.append(exc)

    thread = threading.Thread(target=_thread_main, daemon=True)
    thread.start()
    try:
        result = startup.get(timeout=5)
    except queue.Empty as exc:
        raise RuntimeError("FTP server did not start within 5 seconds") from exc
    if isinstance(result, BaseException):
        raise RuntimeError("FTP server failed to start") from result  # noqa: TRY004

    try:
        yield host, result
    finally:
        stop_event.set()
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError("FTP server did not stop within 5 seconds")
        if errors:
            raise RuntimeError("FTP server thread failed") from errors[0]


@pytest.fixture(scope="session")
def ftp_endpoint(_ftp_server: tuple[str, int]) -> tuple[str, int]:
    """Return the shared local FTP endpoint."""
    return _ftp_server
