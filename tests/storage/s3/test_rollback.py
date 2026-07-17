"""S3Storage rollback path tests using mocked AsyncS3Client.

No real S3 credentials required — the client is replaced with a MagicMock
and individual methods are patched per-test with ``AsyncMock`` side effects.
"""

from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr
from pytest_mock import MockerFixture

from app.storage.abstract import FileInfo
from app.storage.s3.client import CopyPartResult, S3Config, S3HttpStatusError
from app.storage.s3.storage import UPLOAD_CHUNK_SIZE, S3Storage
from app.utils import flatten_exception_group


@pytest.fixture
def s3_mocked() -> S3Storage:
    """S3Storage with a fake config and mocked connect / close / client."""
    cfg = S3Config(
        access_key_id=SecretStr("test-id"),
        secret_access_key=SecretStr("test-key"),
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

        async def _head(key: str) -> MagicMock | None:
            return MagicMock() if key == "src.txt" else None

        mocker.patch.object(s._client, "head_object", side_effect=_head)
        calls: list[str] = []

        async def _delete(key: str) -> None:
            calls.append(key)
            if key == "src.txt":
                raise S3HttpStatusError("DELETE", "/src.txt", 500, "simulated")

        mocker.patch.object(s._client, "delete_object", side_effect=_delete)

        with pytest.raises(OSError, match="Failed to delete source after move"):
            await s.move(src, dst)

        assert calls == ["src.txt", "dst.txt"]

    async def test_delete_src_failure_restores_existing_destination(
        self, s3_mocked: S3Storage, mocker: MockerFixture
    ) -> None:
        s = s3_mocked
        src = "/src.txt"
        dst = "/dst.txt"
        destination_head = MagicMock()

        async def _head(key: str) -> MagicMock | None:
            if key in {"src.txt", "dst.txt"}:
                return destination_head
            return None

        mocker.patch.object(s._client, "head_object", side_effect=_head)
        mocker.patch.object(s, "copy", new=AsyncMock())
        copy_calls: list[tuple[str, str]] = []

        async def _copy(source_key: str, target_key: str) -> None:
            copy_calls.append((source_key, target_key))

        mocker.patch.object(s._client, "put_object_copy", side_effect=_copy)
        deleted_keys: list[str] = []

        async def _delete(key: str) -> None:
            deleted_keys.append(key)
            if key == "src.txt":
                raise S3HttpStatusError("DELETE", "/src.txt", 500, "simulated")

        mocker.patch.object(s._client, "delete_object", side_effect=_delete)

        with pytest.raises(OSError, match="Failed to delete source after move"):
            await s.move(src, dst)

        assert len(copy_calls) == 2
        backup_key = copy_calls[0][1]
        assert copy_calls == [("dst.txt", backup_key), (backup_key, "dst.txt")]
        assert deleted_keys == ["src.txt", backup_key]

    async def test_rollback_itself_fails_still_raises(self, s3_mocked: S3Storage, mocker: MockerFixture):
        """When both delete_object(src) and rollback delete_object(dst) fail, still raises."""
        s = s3_mocked
        src = "/src.txt"
        dst = "/dst.txt"

        mocker.patch.object(s, "copy", new=AsyncMock())
        mocker.patch.object(s._client, "head_object", new=AsyncMock(return_value=None))

        async def _delete(key: str) -> None:
            raise S3HttpStatusError("DELETE", key, 500, "always-fails")

        mock_delete = mocker.patch.object(s._client, "delete_object", side_effect=_delete)

        with pytest.raises(OSError, match="Failed to delete source after move"):
            await s.move(src, dst)

        assert mock_delete.call_count == 2



# ---------------------------------------------------------------------------
# copytree() overwrite rollback
# ---------------------------------------------------------------------------


class TestCopytreeRollback:
    @pytest.mark.parametrize("restore_fails", [False, True])
    async def test_failure_restores_overwritten_files_and_directory_markers(
        self, s3_mocked: S3Storage, restore_fails: bool
    ) -> None:
        storage = s3_mocked
        objects: dict[str, bytes] = {
            "src/": b"src marker",
            "src/a.txt": b"new a",
            "src/b.txt": b"new b",
            "dst/": b"old dst marker",
            "dst/a.txt": b"old a",
            "dst/sub/": b"old sub marker",
        }

        async def _head(key: str) -> MagicMock | None:
            return MagicMock() if key in objects else None

        async def _put(key: str, data: bytes) -> None:
            objects[key] = data

        async def _put_copy(source_key: str, target_key: str) -> None:
            if restore_fails and ".storegate-copytree-backup-" in source_key and target_key == "dst/a.txt":
                raise OSError("injected restore failure")
            objects[target_key] = objects[source_key]

        async def _delete_objects(keys: Iterable[str]) -> list[str]:
            deleted: list[str] = []
            for key in keys:
                if key in objects:
                    del objects[key]
                    deleted.append(key)
            return deleted

        async def _walk(_path: object) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
            yield (
                "/src",
                [FileInfo(path="/src/sub", name="sub", is_dir=True)],
                [
                    FileInfo(path="/src/a.txt", name="a.txt", is_dir=False),
                    FileInfo(path="/src/b.txt", name="b.txt", is_dir=False),
                ],
            )

        async def _copy(src: object, dst: object, *, overwrite: bool = True) -> None:
            _ = overwrite
            src_key = storage._remote_path_to_key(src)
            dst_key = storage._remote_path_to_key(dst)
            if dst_key == "dst/b.txt":
                raise OSError("injected second copy failure")
            objects[dst_key] = objects[src_key]

        storage._client.head_object = _head
        storage._client.put_object = _put
        storage._client.put_object_copy = _put_copy
        storage._client.delete_objects = _delete_objects
        storage.walk = _walk
        storage.copy = _copy

        if restore_fails:
            with pytest.raises(BaseExceptionGroup) as exc_info:
                await storage.copytree("/src", "/dst", overwrite=True)
            flattened = list(flatten_exception_group(exc_info.value))
            assert "injected second copy failure" in str(flattened[0])
            assert "Failed to restore copytree target dst/a.txt" in str(flattened[1])
            backup_key = next(key for key in objects if key.endswith("/dst/a.txt"))
            assert objects["dst/a.txt"] == b"new a"
            assert objects[backup_key] == b"old a"
            assert objects["dst/"] == b"old dst marker"
            assert objects["dst/sub/"] == b"old sub marker"
            assert "dst/b.txt" not in objects
        else:
            with pytest.raises(OSError, match="Failed to copy tree"):
                await storage.copytree("/src", "/dst", overwrite=True)
            assert objects["dst/a.txt"] == b"old a"
            assert objects["dst/"] == b"old dst marker"
            assert objects["dst/sub/"] == b"old sub marker"
            assert "dst/b.txt" not in objects
            assert not any(".storegate-copytree-backup-" in key for key in objects)

    @pytest.mark.parametrize("delete_after_mutation", [False, True])
    async def test_success_confirms_copytree_backup_cleanup(
        self, s3_mocked: S3Storage, delete_after_mutation: bool
    ) -> None:
        storage = s3_mocked
        objects: dict[str, bytes] = {
            "src/": b"src marker",
            "src/a.txt": b"new a",
            "dst/": b"old dst marker",
            "dst/a.txt": b"old a",
        }
        delete_attempts = 0

        async def _head(key: str) -> MagicMock | None:
            return MagicMock() if key in objects else None

        async def _put(key: str, data: bytes) -> None:
            objects[key] = data

        async def _put_copy(source_key: str, target_key: str) -> None:
            objects[target_key] = objects[source_key]

        async def _delete_objects(keys: Iterable[str]) -> list[str]:
            nonlocal delete_attempts
            delete_attempts += 1
            pending = list(keys)
            if delete_attempts == 1:
                if delete_after_mutation:
                    for key in pending:
                        objects.pop(key, None)
                raise OSError("injected cleanup response loss")
            for key in pending:
                objects.pop(key, None)
            return pending

        async def _walk(_path: object) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
            yield "/src", [], [FileInfo(path="/src/a.txt", name="a.txt", is_dir=False)]

        async def _copy(src: object, dst: object, *, overwrite: bool = True) -> None:
            _ = overwrite
            objects[storage._remote_path_to_key(dst)] = objects[storage._remote_path_to_key(src)]

        storage._client.head_object = _head
        storage._client.put_object = _put
        storage._client.put_object_copy = _put_copy
        storage._client.delete_objects = _delete_objects
        storage.walk = _walk
        storage.copy = _copy

        await storage.copytree("/src", "/dst", overwrite=True)

        assert objects["dst/a.txt"] == b"new a"
        assert not any(".storegate-copytree-backup-" in key for key in objects)
        assert delete_attempts == (1 if delete_after_mutation else 2)

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
