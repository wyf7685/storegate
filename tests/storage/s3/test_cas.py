"""S3 compare-exchange (CAS) tests using signed conditional PUT."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

from storegate.storage.s3.client import AsyncS3Client, HeadObjectOutput, S3Config, S3HttpStatusError
from storegate.storage.s3.storage import S3Storage
from storegate.utils import httpx

pytestmark = pytest.mark.httpx


def _config() -> S3Config:
    return S3Config(
        access_key_id=SecretStr("test-access-key"),
        secret_access_key=SecretStr("test-secret-key"),
        region="us-east-1",
        bucket="test-bucket",
    )


@contextlib.asynccontextmanager
async def _mocked_client(handler: Callable[[httpx.Request], httpx.Response]) -> AsyncIterator[AsyncS3Client]:
    client = AsyncS3Client(_config())
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="https://test-bucket.s3.us-east-1.amazonaws.com",
    ) as raw:
        client._client = raw
        yield client


@pytest.mark.parametrize(
    ("header", "value"),
    [
        ("If-None-Match", "*"),
        ("If-Match", '"v1"'),
    ],
)
async def test_put_object_returns_etag_and_signs_conditional_headers(header: str, value: str) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, headers={"ETag": '"abc123"'})

    async with _mocked_client(handler) as client:
        etag = await client.put_object("file.bin", b"payload", headers={header: value})
        assert etag == '"abc123"'

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "PUT"
    assert request.headers[header] == value
    assert header.lower() in request.headers["Authorization"].lower()


async def test_compare_exchange_create_if_absent_success() -> None:
    storage = S3Storage(_config())
    client = MagicMock(spec=AsyncS3Client)
    storage._client = cast("AsyncS3Client", client)
    storage.is_dir = AsyncMock(return_value=False)  # type: ignore[method-assign]

    client.put_object = AsyncMock(return_value='"v1"')
    created = await storage.compare_exchange("/locks/a.lock", expected_token=None, data=b"owner-1")
    assert created is not None
    assert created.data == b"owner-1"
    assert created.token == '"v1"'
    client.put_object.assert_awaited_once_with(
        key="locks/a.lock",
        data=b"owner-1",
        headers={"If-None-Match": "*"},
    )


async def test_compare_exchange_if_match_success() -> None:
    storage = S3Storage(_config())
    client = MagicMock(spec=AsyncS3Client)
    storage._client = cast("AsyncS3Client", client)
    storage.is_dir = AsyncMock(return_value=False)  # type: ignore[method-assign]

    client.put_object = AsyncMock(return_value='"v2"')
    updated = await storage.compare_exchange("/locks/a.lock", expected_token='"v1"', data=b"owner-2")
    assert updated is not None
    assert updated.token == '"v2"'
    client.put_object.assert_awaited_once_with(
        key="locks/a.lock",
        data=b"owner-2",
        headers={"If-Match": '"v1"'},
    )


@pytest.mark.parametrize("expected_token", [None, '"stale"'])
async def test_compare_exchange_conflicts_leave_namespace_unchanged(expected_token: str | None) -> None:
    storage = S3Storage(_config())
    client = MagicMock(spec=AsyncS3Client)
    storage._client = cast("AsyncS3Client", client)
    storage.is_dir = AsyncMock(return_value=False)  # type: ignore[method-assign]

    objects = {
        "locks/": b"existing-parent-marker",
        "locks/a.lock": b"owner-1",
        "unrelated.bin": b"unchanged",
    }
    tokens = {"locks/a.lock": '"v1"'}

    async def conditional_put(*, key: str, data: bytes, headers: dict[str, str]) -> str:
        current_token = tokens.get(key)
        if headers.get("If-None-Match") == "*" and key in objects:
            raise S3HttpStatusError("PUT", f"https://example/{key}", 412, "PreconditionFailed")
        if (if_match := headers.get("If-Match")) is not None and if_match != current_token:
            raise S3HttpStatusError("PUT", f"https://example/{key}", 412, "PreconditionFailed")
        objects[key] = data
        tokens[key] = '"new"'
        return '"new"'

    client.put_object = AsyncMock(side_effect=conditional_put)
    storage.mkdir = AsyncMock(  # type: ignore[method-assign]
        side_effect=lambda *_args, **_kwargs: objects.__setitem__("locks/", b"rewritten-parent-marker")
    )
    before_objects = objects.copy()
    before_tokens = tokens.copy()

    result = await storage.compare_exchange("/locks/a.lock", expected_token=expected_token, data=b"owner-2")

    assert result is None
    assert objects == before_objects
    assert tokens == before_tokens
    storage.mkdir.assert_not_awaited()


async def test_read_versioned_uses_data_and_etag_from_same_get() -> None:
    storage = S3Storage(_config())
    client = MagicMock(spec=AsyncS3Client)
    storage._client = cast("AsyncS3Client", client)

    class SameVersionResponse:
        def __init__(self) -> None:
            self.headers = {"ETag": '"v2"'}

        async def aread(self) -> bytes:
            return b"version-2-data"

    get_keys: list[str] = []

    @contextlib.asynccontextmanager
    async def stream_get(key: str, *, range_start: int | None = None) -> AsyncIterator[SameVersionResponse]:
        assert range_start is None
        get_keys.append(key)
        yield SameVersionResponse()

    client.stream_get = stream_get  # type: ignore[method-assign]
    # Model an overwrite between the old HEAD/GET implementation's requests.
    client.head_object = AsyncMock(
        return_value=HeadObjectOutput(
            content_length=4,
            etag='"v1"',
            last_modified=datetime(2026, 7, 23, tzinfo=UTC),
        )
    )
    client.get_object = AsyncMock(return_value=b"version-2-data")

    versioned = await storage.read_versioned("/file.bin")

    assert versioned is not None
    assert versioned.data == b"version-2-data"
    assert versioned.token == '"v2"'
    assert get_keys == ["file.bin"]
    client.head_object.assert_not_awaited()
    client.get_object.assert_not_awaited()


async def test_read_versioned_preserves_missing_and_directory_results() -> None:
    storage = S3Storage(_config())
    client = MagicMock(spec=AsyncS3Client)
    storage._client = cast("AsyncS3Client", client)
    storage.is_dir = AsyncMock(side_effect=[False, True])  # type: ignore[method-assign]
    get_keys: list[str] = []

    @contextlib.asynccontextmanager
    async def stream_get(key: str, *, range_start: int | None = None) -> AsyncIterator[object]:
        assert range_start is None
        get_keys.append(key)
        if key in {"missing.bin", "directory"}:
            raise S3HttpStatusError("GET", f"https://example/{key}", 404, "NoSuchKey")
        yield object()

    client.stream_get = stream_get  # type: ignore[method-assign]

    assert await storage.read_versioned("/missing.bin") is None
    with pytest.raises(IsADirectoryError, match="Not a regular file"):
        await storage.read_versioned("/directory")
    assert get_keys == ["missing.bin", "directory"]


async def test_capabilities_advertise_compare_exchange() -> None:
    storage = S3Storage(_config())
    assert storage.capabilities.compare_exchange is True
