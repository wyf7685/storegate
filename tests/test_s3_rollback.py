"""S3Storage rollback path tests using mocked AsyncS3Client.

No real S3 credentials required — the client is replaced with a MagicMock
and individual methods are patched per-test with ``AsyncMock`` side effects.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockerFixture

from app.storage.s3.s3_client.errors import S3HttpStatusError
from app.storage.s3.s3_client.models import (
    CopyPartResult,
    S3Config,
)
from app.storage.s3.storage import UPLOAD_CHUNK_SIZE, S3Storage


@pytest.fixture
def s3_mocked() -> S3Storage:
    """S3Storage with a fake config and mocked connect / close / client."""
    cfg = S3Config(
        access_key_id="test-id",
        secret_access_key="test-key",
        region="ap-guangzhou",
        bucket="test-bucket",
    )
    s = S3Storage(cfg)
    s.connect = AsyncMock()
    s.close = AsyncMock()
    s._client = MagicMock()
    return s


# ---------------------------------------------------------------------------
# move() rollback
# ---------------------------------------------------------------------------


class TestMoveRollback:
    async def test_delete_src_fails_rolls_back_dst(self, s3_mocked: S3Storage, mocker: MockerFixture):
        """When delete_object(src) fails after copy, rollback deletes dst."""
        s = s3_mocked
        src = "/src.txt"
        dst = "/dst.txt"

        mocker.patch.object(s, "copy", new=AsyncMock())

        calls: list[str] = []

        async def _delete(key: str) -> None:
            calls.append(key)
            if key == "src.txt":
                raise S3HttpStatusError("DELETE", "/src.txt", 500, "simulated")

        mocker.patch.object(s._client, "delete_object", side_effect=_delete)

        with pytest.raises(OSError, match="Failed to delete source after move"):
            await s.move(src, dst)

        assert len(calls) == 2
        assert calls == ["src.txt", "dst.txt"]

    async def test_rollback_itself_fails_still_raises(self, s3_mocked: S3Storage, mocker: MockerFixture):
        """When both delete_object(src) and rollback delete_object(dst) fail, still raises."""
        s = s3_mocked
        src = "/src.txt"
        dst = "/dst.txt"

        mocker.patch.object(s, "copy", new=AsyncMock())

        async def _delete(key: str) -> None:
            raise S3HttpStatusError("DELETE", key, 500, "always-fails")

        mock_delete = mocker.patch.object(s._client, "delete_object", side_effect=_delete)

        with pytest.raises(OSError, match="Failed to delete source after move"):
            await s.move(src, dst)

        assert mock_delete.call_count == 2


# ---------------------------------------------------------------------------
# _copy_multipart rollback
# ---------------------------------------------------------------------------


class TestCopyMultipartRollback:
    async def test_abort_on_part_copy_failure(self, s3_mocked: S3Storage, mocker: MockerFixture):
        """When upload_part_copy fails mid-way, abort_multipart_upload is called."""
        s = s3_mocked
        src_key = "src.txt"
        dst_key = "dst.txt"
        upload_id = "upload-abc123"
        src_size = 10 * 1024 * 1024

        mocker.patch.object(s._client, "create_multipart_upload", new=AsyncMock(return_value=upload_id))

        parts_called: list[tuple[int, int]] = []

        async def _upload_part_copy(
            *,
            source_key: str,  # noqa: ARG001
            target_key: str,  # noqa: ARG001
            upload_id: str,  # noqa: ARG001
            part_number: int,
            byte_range: tuple[int, int],
        ) -> CopyPartResult:
            parts_called.append((part_number, byte_range[0]))
            if part_number == 2:
                raise S3HttpStatusError("PUT", "/dst.txt", 500, "simulated part failure")
            return CopyPartResult(etag=f"etag-{part_number}", last_modified=datetime.now(UTC))

        mocker.patch.object(s._client, "upload_part_copy", side_effect=_upload_part_copy)
        mock_abort = mocker.patch.object(s._client, "abort_multipart_upload", new=AsyncMock())

        with pytest.raises(OSError, match="Failed to copy object"):
            await s._copy_multipart(src_key, dst_key, src_size)

        assert len(parts_called) == 2
        assert parts_called[0][1] == 0
        assert parts_called[1][1] > 0

        mock_abort.assert_awaited_once_with(dst_key, upload_id)

    async def test_create_multipart_upload_failure(self, s3_mocked: S3Storage, mocker: MockerFixture):
        """When create_multipart_upload fails, abort is NOT called."""
        s = s3_mocked
        src_key = "src.txt"
        dst_key = "dst.txt"
        src_size = 10 * 1024 * 1024

        mocker.patch.object(
            s._client,
            "create_multipart_upload",
            side_effect=S3HttpStatusError("POST", "/dst.txt", 500, "simulated create failure"),
        )
        mock_abort = mocker.patch.object(s._client, "abort_multipart_upload", new=AsyncMock())

        with pytest.raises(OSError, match="Failed to create multipart upload"):
            await s._copy_multipart(src_key, dst_key, src_size)

        mock_abort.assert_not_awaited()

    async def test_single_part_small_source(self, s3_mocked: S3Storage, mocker: MockerFixture):
        """When source fits in one part, it succeeds without multipart complexity."""
        s = s3_mocked
        src_key = "src.txt"
        dst_key = "dst.txt"
        src_size = UPLOAD_CHUNK_SIZE

        mocker.patch.object(s._client, "create_multipart_upload", new=AsyncMock(return_value="upload-small"))
        mock_upload_part = mocker.patch.object(
            s._client,
            "upload_part_copy",
            new=AsyncMock(return_value=CopyPartResult(etag="etag-1", last_modified=datetime.now(UTC))),
        )
        mock_complete = mocker.patch.object(s._client, "complete_multipart_upload", new=AsyncMock())
        mock_abort = mocker.patch.object(s._client, "abort_multipart_upload", new=AsyncMock())

        await s._copy_multipart(src_key, dst_key, src_size)

        mock_upload_part.assert_awaited_once()
        mock_complete.assert_awaited_once_with(dst_key, "upload-small", [{"PartNumber": 1, "ETag": "etag-1"}])
        mock_abort.assert_not_awaited()
