"""Shared fixtures for storage tests."""

import asyncio
import queue
import shutil
import socket
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import AsyncIterator, Generator
from pathlib import Path

import pytest

from app.storage.abstract import AbstractStorage


@pytest.fixture(autouse=True, scope="session")
def configure_logging() -> None:
    """Configure logging for tests."""
    from app.log import log_format, log_level_filter, logger, remove_loguru_sinks

    remove_loguru_sinks()
    logger.add(
        sys.stdout,
        level="DEBUG",
        diagnose=False,
        enqueue=False,
        format=log_format,
        filter=log_level_filter(),
    )


def uid() -> str:
    """Return a short unique identifier for test isolation."""
    return uuid.uuid4().hex[:12]


def _param_of(impl: str):  # noqa: ANN202
    """Return a pytest.param for the given storage implementation."""
    return pytest.param(impl, marks=getattr(pytest.mark, impl), id=impl)


@pytest.fixture(
    params=[
        _param_of("memory"),
        _param_of("local"),
        _param_of("s3"),
        _param_of("cached"),
        _param_of("index"),
        _param_of("dav"),
    ]
)
async def storage(request: pytest.FixtureRequest) -> AsyncIterator[AbstractStorage]:
    """Parametrized fixture: yields each storage backend for every test."""
    match request.param:
        case "memory":
            from app.storage.memory import MemoryStorage

            async with MemoryStorage("/") as s:
                yield s

        case "local":
            from app.storage.local import LocalStorage

            root = Path(tempfile.mkdtemp(prefix="storegate_test_"))
            try:
                async with LocalStorage(root) as s:
                    yield s
            finally:
                shutil.rmtree(root, ignore_errors=True)

        case "s3":
            from app.storage.s3 import S3Storage

            config_path = Path("data/s3/mock.json")
            if not config_path.exists():
                pytest.skip("S3 config file not found")
            async with S3Storage(config_path) as s:
                yield s

        case "cached":
            from app.storage.cached import CachedStorage
            from app.storage.memory import MemoryStorage

            async with MemoryStorage("/") as inner, CachedStorage(inner) as s:
                yield s

        case "index":
            from app.storage.index import IndexStorage
            from app.storage.memory import MemoryStorage

            async with IndexStorage(
                index=MemoryStorage("/"),
                chunks=MemoryStorage("/"),
                block_size=16 * 1024,
            ) as s:
                yield s

        case "dav":
            from app.storage.dav import DavConfig, DavStorage

            base_url = request.getfixturevalue("_dav_server")
            config = DavConfig(base_url=base_url, auth_mode="anonymous")
            async with DavStorage(config) as s:
                yield s


@pytest.fixture(scope="session")
def _dav_server() -> Generator[str]:
    """Start a local wsgidav server (backed by MemoryStorage) on a random port.

    Runs in a dedicated thread + event loop so it is independent of the
    pytest-asyncio event loop scope. Returns the base URL.
    """

    from app.server.dav.server import DAVServer
    from app.storage.memory import MemoryStorage

    stop_event = asyncio.Event()

    async def _runner(port: int, stop_event: asyncio.Event) -> None:
        server = DAVServer(MemoryStorage("/"), host="127.0.0.1", port=port)
        server_task = asyncio.create_task(server.serve())
        stop_task = asyncio.create_task(stop_event.wait())
        _, pending = await asyncio.wait(
            (server_task, stop_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    thread = threading.Thread(target=lambda: asyncio.run(_runner(port, stop_event)), daemon=True)
    thread.start()
    _wait_dav_port("127.0.0.1", port)

    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        stop_event.set()
        thread.join(timeout=5)


def _wait_dav_port(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"DAV server did not start on {host}:{port}")


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
