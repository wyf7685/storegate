"""Backend exception details must not reach anonymous WebDAV clients.

wsgidav stores the originating exception on ``DAVError.src_exception`` and
embeds its ``repr()`` in the generated HTML error page. Any storage error that
escapes untranslated therefore leaks backend identifiers -- S3 keys, bucket
names, SFTP hosts -- to whoever made the request.
"""

from __future__ import annotations

import functools
from collections.abc import AsyncIterable, AsyncIterator
from typing import override

import anyio
import anyio.lowlevel
import anyio.to_thread
import pytest
from wsgidav.wsgidav_app import WsgiDAVApp

from storegate.server.dav.server import create_wsgi_app
from storegate.server.dav.utils import current_event_loop_token
from storegate.storage.abstract import BytesLike, PathLike
from storegate.storage.memory import MemoryStorage
from storegate.utils import httpx

SECRET = "s3://internal-bucket/AKIAIOSFODNN7EXAMPLE/prefix"


@pytest.fixture
async def dav_thread_bridge() -> AsyncIterator[None]:
    reset_token = current_event_loop_token.set(anyio.lowlevel.current_token())
    try:
        yield
    finally:
        current_event_loop_token.reset(reset_token)


pytestmark = [pytest.mark.integration, pytest.mark.httpx, pytest.mark.usefixtures("dav_thread_bridge")]


class LeakyStorage(MemoryStorage):  # ty: ignore[subclass-of-final-class]
    """Fails every mutation with an exception carrying a backend secret.

    ``seeding`` lets a test place fixture content through the real MemoryStorage
    implementation before arming the failure.
    """

    seeding = False

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        if self.seeding:
            await super().upload_stream(stream, remote_path, overwrite=overwrite)
            return
        async for _ in stream:
            pass
        raise RuntimeError(SECRET)

    @override
    async def mkdir(self, path: PathLike, *, parents: bool = False, exist_ok: bool = False) -> None:
        raise RuntimeError(SECRET)


def _request(app: WsgiDAVApp, method: str, path: str, *, content: bytes | None = None) -> httpx.Response:
    with httpx.Client(transport=httpx.WSGITransport(app=app), base_url="http://testserver") as client:
        return client.request(method, path, content=content)


@pytest.mark.parametrize(
    ("method", "path", "content"),
    [
        ("PUT", "/leak.txt", b"payload"),
        ("MKCOL", "/leakdir", None),
    ],
)
async def test_backend_error_details_are_not_disclosed(method: str, path: str, content: bytes | None) -> None:
    app = create_wsgi_app(LeakyStorage("/"), "127.0.0.1", 8080)

    response = await anyio.to_thread.run_sync(functools.partial(_request, app, method, path, content=content))

    assert response.status_code == 500
    assert SECRET not in response.text
    assert "RuntimeError" not in response.text


async def test_put_over_existing_file_reports_commit_failure() -> None:
    """A PUT whose backend commit fails must not be answered with success.

    Overwriting an existing resource goes through ResourceWriter, whose
    upload_fn runs on a worker thread. LeakyStorage.upload_stream drains the
    body and only then raises, which used to die with that thread and let
    wsgidav report 204 for an upload that never landed.
    """
    storage = LeakyStorage("/")
    async with storage:
        storage.seeding = True
        await storage.upload_bytes(b"original", "/existing.txt")
        storage.seeding = False
        app = create_wsgi_app(storage, "127.0.0.1", 8080)

        response = await anyio.to_thread.run_sync(
            functools.partial(_request, app, "PUT", "/existing.txt", content=b"payload")
        )

    assert response.status_code >= 500, f"a failed commit must not report success, got {response.status_code}"
    assert SECRET not in response.text
