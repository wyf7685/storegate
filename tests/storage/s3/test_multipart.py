"""Focused tests for S3 multipart upload orchestration."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import anyio
import anyio.lowlevel
import pytest
from pydantic import SecretStr
from pytest_mock import MockerFixture

from storegate.storage.s3.client import AsyncS3Client, S3Config, S3HttpStatusError
from storegate.storage.s3.storage import UPLOAD_CHUNK_SIZE, S3Storage
from storegate.storage.s3.utils import MultipartUploadTask
from storegate.utils import httpx

pytestmark = pytest.mark.httpx


def _config() -> S3Config:
    return S3Config(
        access_key_id=SecretStr("test-access-key"),
        secret_access_key=SecretStr("test-secret-key"),
        region="us-east-1",
        bucket="test-bucket",
    )


def _mock_client() -> tuple[AsyncS3Client, MagicMock]:
    raw = MagicMock(spec=AsyncS3Client)
    return cast("AsyncS3Client", raw), raw


async def _chunks(*values: bytes) -> AsyncIterator[bytes]:
    for value in values:
        yield value


async def test_create_completes_with_parts_sorted_by_number() -> None:
    client, raw = _mock_client()
    raw.create_multipart_upload = AsyncMock(return_value="upload-1")
    raw.upload_part = AsyncMock(side_effect=["etag-2", "etag-1"])
    raw.complete_multipart_upload = AsyncMock()
    raw.abort_multipart_upload = AsyncMock()

    async with MultipartUploadTask.create(client, "large.bin") as task:
        await task.put_chunk(2, b"second")
        await task.put_chunk(1, b"first")

    raw.complete_multipart_upload.assert_awaited_once_with(
        key="large.bin",
        upload_id="upload-1",
        parts=[
            {"PartNumber": 1, "ETag": "etag-1"},
            {"PartNumber": 2, "ETag": "etag-2"},
        ],
    )
    raw.abort_multipart_upload.assert_not_awaited()


async def test_create_aborts_and_preserves_body_error_when_abort_fails() -> None:
    client, raw = _mock_client()
    raw.create_multipart_upload = AsyncMock(return_value="upload-1")
    raw.complete_multipart_upload = AsyncMock()
    raw.abort_multipart_upload = AsyncMock(side_effect=OSError("abort failed"))

    with pytest.raises(BaseExceptionGroup) as caught:
        async with MultipartUploadTask.create(client, "large.bin"):
            raise ValueError("upload failed")

    assert [type(exc) for exc in caught.value.exceptions] == [ValueError, OSError]
    assert [str(exc) for exc in caught.value.exceptions] == ["upload failed", "abort failed"]
    raw.abort_multipart_upload.assert_awaited_once_with(key="large.bin", upload_id="upload-1")
    raw.complete_multipart_upload.assert_not_awaited()


async def test_create_aborts_on_anyio_cancellation() -> None:
    client, raw = _mock_client()
    raw.create_multipart_upload = AsyncMock(return_value="upload-cancel")
    raw.complete_multipart_upload = AsyncMock()
    raw.abort_multipart_upload = AsyncMock()

    async def body() -> None:
        with anyio.CancelScope() as scope:
            async with MultipartUploadTask.create(client, "cancel.bin") as _task:
                scope.cancel()
                await anyio.sleep(10)

    # CancelScope converts CancelledError into scope.cancelled_caught; the body
    # returns normally after abort. The important contract is that abort ran.
    await body()
    raw.abort_multipart_upload.assert_awaited_once_with(key="cancel.bin", upload_id="upload-cancel")
    raw.complete_multipart_upload.assert_not_awaited()


async def test_create_cancellation_with_abort_failure_keeps_primary_first_group() -> None:
    client, raw = _mock_client()
    raw.create_multipart_upload = AsyncMock(return_value="upload-cancel-fail")
    raw.complete_multipart_upload = AsyncMock()
    raw.abort_multipart_upload = AsyncMock(side_effect=OSError("abort failed"))

    # Raise a real CancelledError into the multipart context so primary is preserved
    # even when abort also fails (primary-first group contract).
    cancelled = anyio.get_cancelled_exc_class()("cancelled during multipart")

    with pytest.raises(BaseExceptionGroup) as caught:
        async with MultipartUploadTask.create(client, "cancel-fail.bin") as _task:
            raise cancelled

    primary, secondary = caught.value.exceptions
    assert primary is cancelled
    assert isinstance(secondary, OSError)
    assert str(secondary) == "abort failed"
    raw.abort_multipart_upload.assert_awaited_once_with(key="cancel-fail.bin", upload_id="upload-cancel-fail")
    raw.complete_multipart_upload.assert_not_awaited()


async def test_storage_upload_aborts_when_stream_body_cancels(mocker: MockerFixture) -> None:
    storage = S3Storage(_config())
    client, raw = _mock_client()
    storage._client = client
    mocker.patch.object(storage, "stat", side_effect=FileNotFoundError)
    mocker.patch.object(storage, "mkdir", new=AsyncMock())
    raw.create_multipart_upload = AsyncMock(return_value="upload-stream-cancel")
    raw.upload_part = AsyncMock(return_value="etag-1")
    raw.complete_multipart_upload = AsyncMock()
    raw.abort_multipart_upload = AsyncMock()
    raw.put_object = AsyncMock()

    started = anyio.Event()

    async def infinite_chunks() -> AsyncIterator[bytes]:
        yield b"x" * UPLOAD_CHUNK_SIZE
        yield b"y" * UPLOAD_CHUNK_SIZE
        started.set()
        while True:
            await anyio.lowlevel.checkpoint()
            yield b"z" * UPLOAD_CHUNK_SIZE

    cancel_scope = anyio.CancelScope()

    async def upload() -> None:
        with cancel_scope:
            await storage.upload_stream(infinite_chunks(), "stream-cancel.bin")

    async with anyio.create_task_group() as tg:
        tg.start_soon(upload)
        await started.wait()
        cancel_scope.cancel()

    raw.abort_multipart_upload.assert_awaited()
    raw.complete_multipart_upload.assert_not_awaited()


async def test_put_chunk_retries_request_errors_then_records_part(mocker: MockerFixture) -> None:
    mocker.patch("storegate.storage.s3.utils.RETRY_BACKOFF_SECONDS", 0)
    client, raw = _mock_client()
    request = httpx.Request("PUT", "https://s3.example.test/large.bin")
    raw.upload_part = AsyncMock(
        side_effect=[
            httpx.ConnectError("temporary failure", request=request),
            httpx.ReadError("temporary failure", request=request),
            "etag-1",
        ]
    )
    task = MultipartUploadTask(client, "large.bin")
    task.upload_id = "upload-1"

    await task.put_chunk(1, b"payload")

    assert raw.upload_part.await_count == 3
    assert task.parts == [{"PartNumber": 1, "ETag": "etag-1"}]


async def test_put_chunk_raises_after_retry_budget_is_exhausted(mocker: MockerFixture) -> None:
    mocker.patch("storegate.storage.s3.utils.RETRY_BACKOFF_SECONDS", 0)
    client, raw = _mock_client()
    request = httpx.Request("PUT", "https://s3.example.test/large.bin")
    raw.upload_part = AsyncMock(
        side_effect=[
            httpx.ConnectError("failure 1", request=request),
            httpx.ConnectError("failure 2", request=request),
            httpx.ConnectError("failure 3", request=request),
        ]
    )
    task = MultipartUploadTask(client, "large.bin")
    task.upload_id = "upload-1"

    with pytest.raises(RuntimeError, match="after 3 attempts"):
        await task.put_chunk(1, b"payload")

    assert task.parts == []


@pytest.mark.parametrize("status_code", [429, 500, 502, 503])
async def test_put_chunk_retries_transient_status_errors(status_code: int, mocker: MockerFixture) -> None:
    """503 SlowDown and friends are S3's normal throttle signals, not fatal errors.

    Regression: the retry loop caught only httpx.RequestError, so a routine
    throttle aborted the whole upload and wasted every part already sent.
    """
    mocker.patch("storegate.storage.s3.utils.RETRY_BACKOFF_SECONDS", 0)
    client, raw = _mock_client()
    raw.upload_part = AsyncMock(
        side_effect=[
            S3HttpStatusError("PUT", "https://s3.example.test/large.bin", status_code, "SlowDown"),
            "etag-1",
        ]
    )
    task = MultipartUploadTask(client, "large.bin")
    task.upload_id = "upload-1"

    await task.put_chunk(1, b"payload")

    assert raw.upload_part.await_count == 2
    assert task.parts == [{"PartNumber": 1, "ETag": "etag-1"}]


@pytest.mark.parametrize("status_code", [400, 403, 404])
async def test_put_chunk_fails_fast_on_client_errors(status_code: int, mocker: MockerFixture) -> None:
    """A 4xx other than 429 will not fix itself; retrying only delays the abort."""
    mocker.patch("storegate.storage.s3.utils.RETRY_BACKOFF_SECONDS", 0)
    client, raw = _mock_client()
    raw.upload_part = AsyncMock(
        side_effect=S3HttpStatusError("PUT", "https://s3.example.test/large.bin", status_code, "denied")
    )
    task = MultipartUploadTask(client, "large.bin")
    task.upload_id = "upload-1"

    with pytest.raises(S3HttpStatusError) as caught:
        await task.put_chunk(1, b"payload")

    assert caught.value.status_code == status_code
    assert raw.upload_part.await_count == 1
    assert task.parts == []


async def test_put_chunk_exhausts_retries_on_persistent_throttling(mocker: MockerFixture) -> None:
    mocker.patch("storegate.storage.s3.utils.RETRY_BACKOFF_SECONDS", 0)
    client, raw = _mock_client()
    raw.upload_part = AsyncMock(
        side_effect=S3HttpStatusError("PUT", "https://s3.example.test/large.bin", 503, "SlowDown")
    )
    task = MultipartUploadTask(client, "large.bin")
    task.upload_id = "upload-1"

    with pytest.raises(RuntimeError, match="after 3 attempts"):
        await task.put_chunk(1, b"payload")

    assert raw.upload_part.await_count == 3
    assert task.parts == []


async def test_put_chunk_backs_off_between_retries(mocker: MockerFixture) -> None:
    """An immediate retry against a throttling server just earns another 503."""
    mocker.patch("storegate.storage.s3.utils.RETRY_BACKOFF_SECONDS", 0.01)
    sleep = mocker.patch("storegate.storage.s3.utils.anyio.sleep", new=AsyncMock())
    client, raw = _mock_client()
    raw.upload_part = AsyncMock(
        side_effect=[
            S3HttpStatusError("PUT", "https://s3.example.test/large.bin", 503, "SlowDown"),
            S3HttpStatusError("PUT", "https://s3.example.test/large.bin", 503, "SlowDown"),
            "etag-1",
        ]
    )
    task = MultipartUploadTask(client, "large.bin")
    task.upload_id = "upload-1"

    await task.put_chunk(1, b"payload")

    # Exponential, and no sleep after the final attempt.
    assert [call.args[0] for call in sleep.await_args_list] == [0.01, 0.02]


async def test_upload_from_assigns_monotonic_part_numbers() -> None:
    client, raw = _mock_client()

    async def upload_part(*, part_number: int, **_kwargs: object) -> str:
        return f"etag-{part_number}"

    raw.upload_part = AsyncMock(side_effect=upload_part)
    task = MultipartUploadTask(client, "large.bin")
    task.upload_id = "upload-1"

    await task.upload_from(_chunks(b"first", b"second", b"third"), max_workers=2)

    assert sorted(task.parts, key=lambda part: part["PartNumber"]) == [
        {"PartNumber": 1, "ETag": "etag-1"},
        {"PartNumber": 2, "ETag": "etag-2"},
        {"PartNumber": 3, "ETag": "etag-3"},
    ]


async def test_storage_upload_uses_multipart_for_payload_above_chunk_size(mocker: MockerFixture) -> None:
    storage = S3Storage(_config())
    client, raw = _mock_client()
    storage._client = client
    mocker.patch.object(storage, "stat", side_effect=FileNotFoundError)
    mocker.patch.object(storage, "mkdir", new=AsyncMock())
    raw.create_multipart_upload = AsyncMock(return_value="upload-1")
    uploaded: list[tuple[int, bytes]] = []

    async def upload_part(*, data: bytes, part_number: int, **_kwargs: object) -> str:
        uploaded.append((part_number, data))
        return f"etag-{part_number}"

    raw.upload_part = AsyncMock(side_effect=upload_part)
    raw.complete_multipart_upload = AsyncMock()
    raw.abort_multipart_upload = AsyncMock()
    raw.put_object = AsyncMock()
    payload = b"x" * (UPLOAD_CHUNK_SIZE + 1)

    await storage.upload_bytes(payload, "large.bin")

    assert [(part_number, len(data)) for part_number, data in sorted(uploaded)] == [
        (1, UPLOAD_CHUNK_SIZE),
        (2, 1),
    ]
    raw.complete_multipart_upload.assert_awaited_once_with(
        key="large.bin",
        upload_id="upload-1",
        parts=[
            {"PartNumber": 1, "ETag": "etag-1"},
            {"PartNumber": 2, "ETag": "etag-2"},
        ],
    )
    raw.put_object.assert_not_awaited()
    raw.abort_multipart_upload.assert_not_awaited()
