"""Streaming download and signed GET lifecycle tests for AsyncS3Client/S3Storage."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from types import TracebackType
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest
from anyio.lowlevel import checkpoint
from pydantic import SecretStr

from storegate.storage.s3.client import AsyncS3Client, HeadObjectOutput, S3Config, S3HttpStatusError
from storegate.storage.s3.storage import S3Storage
from storegate.utils import httpx

pytestmark = pytest.mark.httpx


def _config(*, max_concurrency: int = 1) -> S3Config:
    return S3Config(
        access_key_id=SecretStr("test-access-key"),
        secret_access_key=SecretStr("test-secret-key"),
        region="us-east-1",
        bucket="test-bucket",
        max_concurrency=max_concurrency,
    )


_NOW = datetime(2026, 7, 23, tzinfo=UTC)


@contextlib.asynccontextmanager
async def _mocked_client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    max_concurrency: int = 1,
) -> AsyncIterator[AsyncS3Client]:
    client = AsyncS3Client(_config(max_concurrency=max_concurrency))
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://test-bucket.s3.us-east-1.amazonaws.com",
    ) as raw:
        client._client = raw
        yield client


async def test_stream_get_full_object_and_range_headers() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert "Authorization" in request.headers
        body = b"abcdefghijklmnopqrstuvwxyz"
        if request.headers.get("Range") == "bytes=10-":
            return httpx.Response(206, content=body[10:], headers={"Content-Range": "bytes 10-25/26"})
        assert "Range" not in request.headers
        return httpx.Response(200, content=body)

    async with _mocked_client(handler) as client:
        async with client.stream_get("file.bin") as response:
            assert b"".join([chunk async for chunk in response.aiter_bytes()]) == b"abcdefghijklmnopqrstuvwxyz"
        async with client.stream_get("file.bin", range_start=10) as response:
            assert b"".join([chunk async for chunk in response.aiter_bytes()]) == b"klmnopqrstuvwxyz"

    assert len(requests) == 2
    assert "Range" not in requests[0].headers
    assert requests[1].headers["Range"] == "bytes=10-"
    # Range header participates in SigV4 signed headers.
    assert "range" in requests[1].headers["Authorization"].lower()


@pytest.mark.parametrize(
    ("headers", "expected_body"),
    [
        ({}, "<body omitted: unknown content length>"),
        ({"Content-Length": "invalid"}, "<body omitted: invalid content length>"),
        ({"Content-Length": "-1"}, "<body omitted: content length exceeds diagnostic limit>"),
        ({"Content-Length": "4097"}, "<body omitted: content length exceeds diagnostic limit>"),
    ],
)
async def test_stream_get_error_body_consumption_is_bounded_and_closes_response(
    headers: dict[str, str],
    expected_body: str,
) -> None:
    consumed = 0
    closed = False

    class CountingErrorStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            nonlocal consumed
            for _ in range(64):
                chunk = b"x" * 65_536
                consumed += len(chunk)
                yield chunk

        async def aclose(self) -> None:
            nonlocal closed
            closed = True

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, headers=headers, stream=CountingErrorStream())

    async with _mocked_client(handler) as client:
        with pytest.raises(S3HttpStatusError) as caught:
            async with client.stream_get("broken.bin"):
                raise AssertionError("stream body should not be yielded on error")

    assert caught.value.status_code == 500
    assert caught.value.body == expected_body
    assert consumed == 0
    assert closed is True


async def test_stream_get_error_body_ignores_inaccurate_small_content_length() -> None:
    consumed = 0
    closed = False

    class CountingErrorStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            nonlocal consumed
            for _ in range(64):
                chunk = b"x" * 1024
                consumed += len(chunk)
                yield chunk

        async def aclose(self) -> None:
            nonlocal closed
            closed = True

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500,
            headers={"Content-Length": "1"},
            stream=CountingErrorStream(),
        )

    async with _mocked_client(handler) as client:
        with pytest.raises(S3HttpStatusError) as caught:
            async with client.stream_get("broken.bin"):
                raise AssertionError("stream body should not be yielded on error")

    assert caught.value.status_code == 500
    assert caught.value.body == "x" * 4096
    assert consumed == 4096
    assert closed is True


async def test_stream_get_closes_response_and_releases_semaphore_on_cancel() -> None:
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
            await checkpoint()
            self.closed.set()

    def handler(_request: httpx.Request) -> httpx.Response:
        stream = CheckpointingCloseStream()
        streams.append(stream)
        return httpx.Response(200, stream=stream)

    async with _mocked_client(handler, max_concurrency=1) as client:
        original_semaphore = client._semaphore

        class TrackingSemaphore:
            def __init__(self, inner: anyio.Semaphore) -> None:
                self._inner = inner

            async def __aenter__(self) -> None:
                await self._inner.__aenter__()

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc_val: BaseException | None,
                exc_tb: TracebackType | None,
            ) -> None:
                await self._inner.__aexit__(exc_type, exc_val, exc_tb)
                released.set()

        client._semaphore = cast("anyio.Semaphore", TrackingSemaphore(original_semaphore))

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

        # The semaphore is free and the client remains usable after cancellation.
        async with client.stream_get("file.bin") as response:
            assert b"".join([chunk async for chunk in response.aiter_bytes()]) == b"payload-bytes"


async def test_storage_download_uses_single_get_for_full_and_offset() -> None:
    storage = S3Storage(_config())
    client = MagicMock(spec=AsyncS3Client)
    storage._client = cast("AsyncS3Client", client)

    total = 2_621_440  # > 2 MiB so old 1 MiB Range loop would issue multiple GETs
    payload = b"a" * total
    head = HeadObjectOutput(content_length=total, etag='"etag"', last_modified=_NOW)
    client.head_object = AsyncMock(return_value=head)

    class FakeResponse:
        def __init__(self, data: bytes) -> None:
            self._data = data
            self.closed = False

        async def aiter_bytes(self) -> AsyncIterator[bytes]:
            yield self._data

        async def aclose(self) -> None:
            self.closed = True

    responses: list[FakeResponse] = []
    ranges: list[int | None] = []

    @contextlib.asynccontextmanager
    async def stream_get(key: str, *, range_start: int | None = None) -> AsyncIterator[FakeResponse]:
        ranges.append(range_start)
        assert key == "big.bin"
        data = payload if range_start is None else payload[range_start:]
        response = FakeResponse(data)
        responses.append(response)
        try:
            yield response
        finally:
            await response.aclose()

    client.stream_get = stream_get  # type: ignore[method-assign]

    full = await storage.download_bytes("big.bin")
    assert full == payload
    assert ranges == [None]
    assert responses[0].closed is True

    offset = 524_288
    partial = bytearray()
    async for chunk in storage.download_stream("big.bin", offset=offset):
        partial.extend(chunk)
    assert bytes(partial) == payload[offset:]
    assert ranges == [None, offset]
    assert responses[1].closed is True


async def test_storage_download_aclose_closes_response() -> None:
    storage = S3Storage(_config())
    client = MagicMock(spec=AsyncS3Client)
    storage._client = cast("AsyncS3Client", client)
    client.head_object = AsyncMock(return_value=HeadObjectOutput(content_length=8, etag='"e"', last_modified=_NOW))

    closed = anyio.Event()

    class FakeResponse:
        async def aiter_bytes(self) -> AsyncIterator[bytes]:
            yield b"abcd"
            yield b"efgh"

        async def aclose(self) -> None:
            closed.set()

    @contextlib.asynccontextmanager
    async def stream_get(key: str, *, range_start: int | None = None) -> AsyncIterator[FakeResponse]:
        del key, range_start
        response = FakeResponse()
        try:
            yield response
        finally:
            await response.aclose()

    client.stream_get = stream_get  # type: ignore[method-assign]

    stream = storage.download_stream("file.bin")
    assert await anext(stream) == b"abcd"
    await stream.aclose()
    with anyio.fail_after(1):
        await closed.wait()
