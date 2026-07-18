import asyncio
import socket
import threading
import time
from collections.abc import AsyncIterator, Generator

import pytest
import uvicorn
from a2wsgi import WSGIMiddleware
from wsgidav.fs_dav_provider import FilesystemProvider
from wsgidav.wsgidav_app import WsgiDAVApp

from app.storage.dav import DavConfig, DavStorage


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
def _dav_server(tmp_path_factory: pytest.TempPathFactory) -> Generator[str]:
    """Start an independent local WsgiDAV filesystem server for client tests."""
    host = "127.0.0.1"
    root = tmp_path_factory.mktemp("dav-client")
    with socket.socket() as sock:
        sock.bind((host, 0))
        port = sock.getsockname()[1]

    wsgi_app = WsgiDAVApp(
        {
            "host": host,
            "port": port,
            "provider_mapping": {"/": FilesystemProvider(str(root))},
            "simple_dc": {"user_mapping": {"*": True}},
            "verbose": 1,
        }
    )
    server = uvicorn.Server(
        uvicorn.Config(
            WSGIMiddleware(wsgi_app),
            host=host,
            port=port,
            log_level="warning",
            lifespan="off",
            interface="asgi3",
        )
    )
    thread = threading.Thread(target=lambda: asyncio.run(server.serve()), daemon=True)
    thread.start()
    _wait_for_port(host, port)

    try:
        yield f"http://{host}:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=5)


@pytest.fixture
async def dav_storage(_dav_server: str) -> AsyncIterator[DavStorage]:
    config = DavConfig(base_url=_dav_server, auth_mode="anonymous")
    async with DavStorage(config) as s:
        yield s
