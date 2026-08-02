"""WebDAV client path construction and escape rejection tests."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from types import TracebackType
from typing import cast

import anyio
import anyio.lowlevel
import pytest

from storegate.storage.dav.client import AsyncDavClient
from storegate.storage.dav.client.models import DavConfig
from storegate.utils import httpx

pytestmark = pytest.mark.httpx


def _client(*, root_prefix: str = "/storegate") -> AsyncDavClient:
    return AsyncDavClient(
        DavConfig(
            base_url="https://host/dav",
            auth_mode="anonymous",
            root_prefix=root_prefix,
        )
    )


class TestBuildPath:
    def test_no_prefix(self) -> None:
        client = AsyncDavClient(DavConfig(base_url="https://host/dav", auth_mode="anonymous"))
        assert client._build_path("foo/bar") == "/foo/bar"
        assert client._build_path("/foo/bar") == "/foo/bar"
        assert client._build_path("") == "/"

    def test_with_prefix(self) -> None:
        client = _client()
        assert client._build_path("foo/bar") == "/storegate/foo/bar"
        assert client._build_path("") == "/storegate"

    def test_build_url_absolute(self) -> None:
        client = _client()
        assert client._build_url("foo") == "https://host/dav/storegate/foo"

    def test_allows_hidden_and_double_dot_names(self) -> None:
        client = _client(root_prefix="/root")
        assert client._build_path(".hidden") == "/root/.hidden"
        assert client._build_path("a..b") == "/root/a..b"
        assert client._build_url(".hidden/a..b") == "https://host/dav/root/.hidden/a..b"

    @pytest.mark.parametrize(
        "path",
        [
            "../x",
            "a/../../x",
            "x/../y",
            "../../escape",
            "bad\x00name",
            "a/\x00/b",
        ],
    )
    def test_build_path_rejects_escape_and_nul(self, path: str) -> None:
        client = _client(root_prefix="/root")
        with pytest.raises(ValueError, match=r"NUL|\.\."):
            client._build_path(path)


@asynccontextmanager
async def _client_with_transport(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    root_prefix: str = "/root",
    max_concurrency: int = 8,
) -> AsyncIterator[tuple[AsyncDavClient, list[httpx.Request]]]:
    requests: list[httpx.Request] = []

    def capturing(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    client = AsyncDavClient(
        DavConfig(
            base_url="https://example.com/base",
            auth_mode="anonymous",
            root_prefix=root_prefix,
            http2=False,
            max_concurrency=max_concurrency,
        )
    )
    transport = httpx.MockTransport(capturing)
    async with httpx.AsyncClient(transport=transport, base_url="https://example.com") as raw:
        client._client = raw
        yield client, requests


class TestStreamGetLease:
    """stream_get must hold its concurrency lease for the body and close under cancellation."""

    async def test_closes_response_and_releases_lease_on_cancel(self) -> None:
        opened = anyio.Event()
        released = anyio.Event()
        streams: list[CheckpointingCloseStream] = []

        class CheckpointingCloseStream(httpx.AsyncByteStream):
            def __init__(self) -> None:
                self.closed = anyio.Event()

            async def __aiter__(self) -> AsyncIterator[bytes]:
                yield b"payload-bytes"

            async def aclose(self) -> None:
                # A real transport close can checkpoint. This must complete even though
                # the consumer's surrounding cancel scope is already cancelled.
                await anyio.lowlevel.checkpoint()
                self.closed.set()

        def handler(_request: httpx.Request) -> httpx.Response:
            stream = CheckpointingCloseStream()
            streams.append(stream)
            return httpx.Response(200, stream=stream)

        async with _client_with_transport(handler, max_concurrency=1) as (client, _requests):
            inner_semaphore = client._semaphore

            class TrackingSemaphore:
                async def __aenter__(self) -> None:
                    await inner_semaphore.__aenter__()

                async def __aexit__(
                    self,
                    exc_type: type[BaseException] | None,
                    exc_val: BaseException | None,
                    exc_tb: TracebackType | None,
                ) -> None:
                    await inner_semaphore.__aexit__(exc_type, exc_val, exc_tb)
                    released.set()

            client._semaphore = cast("anyio.Semaphore", TrackingSemaphore())

            async def reader() -> None:
                async with client.stream_get("file.bin"):
                    opened.set()
                    await anyio.sleep_forever()

            async with anyio.create_task_group() as tg:
                tg.start_soon(reader)
                await opened.wait()
                tg.cancel_scope.cancel()

            with anyio.fail_after(1):
                await streams[0].closed.wait()
                await released.wait()

            # The lease is free and the client remains usable after cancellation.
            async with client.stream_get("file.bin") as response:
                assert b"".join([chunk async for chunk in response.aiter_bytes()]) == b"payload-bytes"

    async def test_lease_covers_body_so_streams_are_bounded(self) -> None:
        """A streamed GET pins a connection, so it must count against max_concurrency."""
        active = 0
        peak = 0

        class SlowStream(httpx.AsyncByteStream):
            async def __aiter__(self) -> AsyncIterator[bytes]:
                yield b"chunk"

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, stream=SlowStream())

        async with _client_with_transport(handler, max_concurrency=1) as (client, _requests):

            async def reader() -> None:
                nonlocal active, peak
                async with client.stream_get("file.bin") as response:
                    active += 1
                    peak = max(peak, active)
                    # Yield control so an unbounded implementation overlaps here.
                    await anyio.lowlevel.checkpoint()
                    assert b"".join([chunk async for chunk in response.aiter_bytes()]) == b"chunk"
                    active -= 1

            async with anyio.create_task_group() as tg:
                for _ in range(4):
                    tg.start_soon(reader)

        assert peak == 1


class TestRequestCaptureNoEscape:
    """Prove invalid paths never emit HTTP requests for GET/PUT/DELETE/COPY/MOVE/PROPFIND."""

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_stream_get_rejects_before_request(self, path: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(200, content=b"ok")) as (
            client,
            requests,
        ):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                async with client.stream_get(path):
                    pass
        assert requests == []

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_put_rejects_before_request(self, path: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(201)) as (client, requests):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await client.put(path, b"data")
        assert requests == []

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_delete_rejects_before_request(self, path: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(204)) as (client, requests):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await client.delete(path)
        assert requests == []

    @pytest.mark.parametrize(
        ("src", "dst"),
        [
            ("../x", "safe"),
            ("safe", "a/../../x"),
            ("x/../y", "z"),
            ("safe", "bad\x00name"),
        ],
    )
    async def test_copy_rejects_before_request(self, src: str, dst: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(201)) as (client, requests):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await client.copy(src, dst)
        assert requests == []

    @pytest.mark.parametrize(
        ("src", "dst"),
        [
            ("../x", "safe"),
            ("safe", "a/../../x"),
            ("x/../y", "z"),
            ("safe", "bad\x00name"),
        ],
    )
    async def test_move_rejects_before_request(self, src: str, dst: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(201)) as (client, requests):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await client.move(src, dst)
        assert requests == []

    @pytest.mark.parametrize(
        "path",
        ["../x", "a/../../x", "x/../y", "bad\x00name"],
    )
    async def test_propfind_rejects_before_request(self, path: str) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(207, content=b"<D:multistatus/>")) as (
            client,
            requests,
        ):
            with pytest.raises(ValueError, match=r"NUL|\.\."):
                await client.propfind(path)
        assert requests == []

    async def test_valid_paths_still_emit_expected_urls(self) -> None:
        async with _client_with_transport(lambda _req: httpx.Response(204)) as (client, requests):
            await client.delete(".hidden/a..b")
            await client.put("inside/file", b"ok")

        assert [str(req.url) for req in requests] == [
            "https://example.com/base/root/.hidden/a..b",
            "https://example.com/base/root/inside/file",
        ]
