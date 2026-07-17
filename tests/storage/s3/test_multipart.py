"""Focused tests for S3 multipart upload orchestration."""

from collections.abc import AsyncIterator
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pydantic import SecretStr
from pytest_mock import MockerFixture

from app.storage.s3.client import AsyncS3Client, S3Config
from app.storage.s3.storage import UPLOAD_CHUNK_SIZE, S3Storage
from app.storage.s3.utils import MultipartUploadTask


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

    with pytest.raises(ValueError, match="upload failed"):
        async with MultipartUploadTask.create(client, "large.bin"):
            raise ValueError("upload failed")

    raw.abort_multipart_upload.assert_awaited_once_with(key="large.bin", upload_id="upload-1")
    raw.complete_multipart_upload.assert_not_awaited()


async def test_put_chunk_retries_request_errors_then_records_part() -> None:
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


async def test_put_chunk_raises_after_retry_budget_is_exhausted() -> None:
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
