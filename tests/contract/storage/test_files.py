"""Shared AbstractStorage contract tests."""

import contextlib

import pytest

from storegate.storage import AbstractStorage
from tests.support.ids import uid


class TestDelete:
    """delete() tests."""

    async def test_empty_directory(self, storage: AbstractStorage):
        path = f"test-del-dir-{uid()}"
        await storage.mkdir(path)
        assert await storage.is_dir(path)
        await storage.delete(path)
        assert not await storage.exists(path)

    async def test_nonempty_directory_raises(self, storage: AbstractStorage):
        path = f"test-del-ned-{uid()}"
        try:
            await storage.mkdir(path)
            await storage.upload_bytes(b"hello", f"{path}/file.txt")
            with pytest.raises(OSError, match="Directory not empty"):
                await storage.delete(path)
        finally:
            await storage.rmtree(path)

    async def test_nonempty_directory_failure_preserves_directory(self, storage: AbstractStorage):
        path = f"test-del-preserve-{uid()}"
        child = f"{path}/file.txt"
        try:
            await storage.mkdir(path)
            await storage.upload_bytes(b"hello", child)
            with pytest.raises(OSError, match="Directory not empty"):
                await storage.delete(path)
            assert await storage.is_dir(path)
            assert await storage.download_bytes(child) == b"hello"
            await storage.delete(child)
            await storage.delete(path)
            assert not await storage.exists(path)
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(path)

    async def test_file(self, storage: AbstractStorage):
        path = f"test-del-file-{uid()}"
        await storage.upload_bytes(b"hello", path)
        assert await storage.is_file(path)
        await storage.delete(path)
        assert not await storage.exists(path)

    async def test_nonexistent_raises(self, storage: AbstractStorage):
        with pytest.raises(FileNotFoundError):
            await storage.delete(f"nonexistent-{uid()}")


class TestUnlink:
    """unlink() tests."""

    async def test_file(self, storage: AbstractStorage):
        path = f"test-unlink-file-{uid()}"
        await storage.upload_bytes(b"hello", path)
        assert await storage.is_file(path)
        await storage.unlink(path)
        assert not await storage.exists(path)

    async def test_dir_raises_is_a_directory_error(self, storage: AbstractStorage):
        path = f"test-unlink-dir-{uid()}"
        try:
            await storage.mkdir(path)
            # IsADirectoryError on most platforms; PermissionError on Windows
            # for some backends that delegate to OS-level unlink.
            with pytest.raises(IsADirectoryError):
                await storage.unlink(path)
        finally:
            await storage.delete(path)

    async def test_nonexistent_missing_ok(self, storage: AbstractStorage):
        await storage.unlink(f"nonexistent-{uid()}", missing_ok=True)


class TestUploadStream:
    """upload_stream() / upload_bytes() path-conflict tests."""

    async def test_upload_new_file(self, storage: AbstractStorage):
        path = f"test-upload-new-{uid()}"
        try:
            await storage.upload_bytes(b"hello", path)
            assert await storage.is_file(path)
            assert await storage.download_bytes(path) == b"hello"
        finally:
            await storage.delete(path)

    async def test_overwrite_existing_file(self, storage: AbstractStorage):
        path = f"test-upload-ow-{uid()}"
        try:
            await storage.upload_bytes(b"first", path)
            assert await storage.download_bytes(path) == b"first"
            # overwrite=True (default) should succeed
            await storage.upload_bytes(b"second", path)
            assert await storage.download_bytes(path) == b"second"
        finally:
            await storage.delete(path)

    async def test_no_overwrite_raises(self, storage: AbstractStorage):
        path = f"test-upload-no-ow-{uid()}"
        try:
            await storage.upload_bytes(b"first", path)
            with pytest.raises(FileExistsError):
                await storage.upload_bytes(b"second", path, overwrite=False)
        finally:
            await storage.delete(path)

    async def test_upload_to_directory_raises(self, storage: AbstractStorage):
        path = f"test-upload-dir-{uid()}"
        try:
            await storage.mkdir(path)
            with pytest.raises(IsADirectoryError):
                await storage.upload_bytes(b"hello", path)
        finally:
            await storage.delete(path)


class TestMove:
    """move() tests."""

    async def test_move_file(self, storage: AbstractStorage):
        src = f"test-move-src-{uid()}"
        dst = f"test-move-dst-{uid()}"

        content = b"move test content"
        try:
            await storage.upload_bytes(content, src)
            await storage.move(src, dst)

            assert not await storage.exists(src)
            assert await storage.is_file(dst)
            assert await storage.download_bytes(dst) == content
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(src)
            with contextlib.suppress(Exception):
                await storage.delete(dst)

    async def test_move_directory_source_rejected(self, storage: AbstractStorage):
        src = f"test-move-source-dir-{uid()}"
        dst = f"test-move-source-dst-{uid()}"
        try:
            await storage.mkdir(src)
            with pytest.raises(IsADirectoryError):
                await storage.move(src, dst)
            assert await storage.is_dir(src)
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(src)

    async def test_move_nonexistent_source(self, storage: AbstractStorage):
        with pytest.raises(FileNotFoundError):
            await storage.move(f"nonexistent-{uid()}", f"nonexistent-dst-{uid()}")

    async def test_move_to_existing_file(self, storage: AbstractStorage):
        src = f"test-move-src2-{uid()}"
        dst = f"test-move-dst2-{uid()}"
        try:
            await storage.upload_bytes(b"src content", src)
            await storage.upload_bytes(b"dst content", dst)
            with pytest.raises(FileExistsError):
                await storage.move(src, dst, overwrite=False)
            await storage.move(src, dst, overwrite=True)
            assert await storage.download_bytes(dst) == b"src content"
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(src)
            with contextlib.suppress(Exception):
                await storage.delete(dst)

    async def test_move_same_path_no_overwrite_raises(self, storage: AbstractStorage):
        path = f"test-move-same-{uid()}"
        try:
            await storage.upload_bytes(b"content", path)
            with pytest.raises(FileExistsError):
                await storage.move(path, path, overwrite=False)
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(path)

    @pytest.mark.parametrize("overwrite", [False, True])
    async def test_move_to_directory_rejected(self, storage: AbstractStorage, overwrite: bool):
        src = f"test-move-dir-src-{uid()}"
        dst = f"test-move-dir-dst-{uid()}"
        try:
            await storage.upload_bytes(b"content", src)
            await storage.mkdir(dst)
            await storage.upload_bytes(b"sentinel", f"{dst}/sentinel.txt")
            with pytest.raises(IsADirectoryError):
                await storage.move(src, dst, overwrite=overwrite)
            assert await storage.download_bytes(src) == b"content"
            assert await storage.is_dir(dst)
            assert await storage.download_bytes(f"{dst}/sentinel.txt") == b"sentinel"
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(src)
            with contextlib.suppress(Exception):
                await storage.rmtree(dst)

    async def test_move_same_path_overwrite_is_noop(self, storage: AbstractStorage):
        path = f"test-move-same-true-{uid()}"
        await storage.upload_bytes(b"content", path)
        await storage.move(path, path, overwrite=True)
        assert await storage.download_bytes(path) == b"content"

    async def test_move_same_path_missing_source_raises(self, storage: AbstractStorage):
        path = f"test-move-same-missing-{uid()}"
        with pytest.raises(FileNotFoundError):
            await storage.move(path, path, overwrite=True)

    async def test_move_same_path_directory_raises(self, storage: AbstractStorage):
        path = f"test-move-same-dir-{uid()}"
        try:
            await storage.mkdir(path)
            with pytest.raises(IsADirectoryError):
                await storage.move(path, path, overwrite=True)
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(path)


class TestCopy:
    """copy() tests."""

    async def test_copy_file(self, storage: AbstractStorage):
        src = f"test-copy-src-{uid()}"
        dst = f"test-copy-dst-{uid()}"
        content = b"copy test content"
        try:
            await storage.upload_bytes(content, src)
            await storage.copy(src, dst)

            assert await storage.is_file(src)
            assert await storage.is_file(dst)
            assert await storage.download_bytes(src) == content
            assert await storage.download_bytes(dst) == content
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(src)
            with contextlib.suppress(Exception):
                await storage.delete(dst)

    async def test_copy_nonexistent_source(self, storage: AbstractStorage):
        with pytest.raises(FileNotFoundError):
            await storage.copy(f"nonexistent-{uid()}", f"nonexistent-dst-{uid()}")

    async def test_copy_destination_conflict_policy(self, storage: AbstractStorage):
        src = f"test-copy-src2-{uid()}"
        dst = f"test-copy-dst2-{uid()}"

        try:
            await storage.upload_bytes(b"src content", src)
            await storage.upload_bytes(b"dst content", dst)
            with pytest.raises(FileExistsError):
                await storage.copy(src, dst, overwrite=False)
            await storage.copy(src, dst, overwrite=True)
            assert await storage.download_bytes(dst) == b"src content"
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(src)
            with contextlib.suppress(Exception):
                await storage.delete(dst)

    async def test_copy_directory_source_rejected(self, storage: AbstractStorage):
        src = f"test-copy-source-dir-{uid()}"
        dst = f"test-copy-source-dst-{uid()}"
        try:
            await storage.mkdir(src)
            with pytest.raises(IsADirectoryError):
                await storage.copy(src, dst)
            assert await storage.is_dir(src)
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(src)

    @pytest.mark.parametrize("overwrite", [False, True])
    async def test_copy_to_directory_rejected(self, storage: AbstractStorage, overwrite: bool):
        src = f"test-copy-dir-src-{uid()}"
        dst = f"test-copy-dir-dst-{uid()}"
        try:
            await storage.upload_bytes(b"content", src)
            await storage.mkdir(dst)
            await storage.upload_bytes(b"sentinel", f"{dst}/sentinel.txt")
            with pytest.raises(IsADirectoryError):
                await storage.copy(src, dst, overwrite=overwrite)
            assert await storage.download_bytes(src) == b"content"
            assert await storage.is_dir(dst)
            assert await storage.download_bytes(f"{dst}/sentinel.txt") == b"sentinel"
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(src)
            with contextlib.suppress(Exception):
                await storage.rmtree(dst)

    async def test_copy_same_path_no_overwrite_raises(self, storage: AbstractStorage):
        path = f"test-copy-same-{uid()}"
        try:
            await storage.upload_bytes(b"content", path)
            with pytest.raises(FileExistsError):
                await storage.copy(path, path, overwrite=False)
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(path)

    async def test_copy_same_path_overwrite_is_noop(self, storage: AbstractStorage):
        path = f"test-copy-same-true-{uid()}"
        await storage.upload_bytes(b"content", path)
        await storage.copy(path, path, overwrite=True)
        assert await storage.download_bytes(path) == b"content"

    async def test_copy_same_path_missing_source_raises(self, storage: AbstractStorage):
        path = f"test-copy-same-missing-{uid()}"
        with pytest.raises(FileNotFoundError):
            await storage.copy(path, path, overwrite=True)

    async def test_copy_same_path_directory_raises(self, storage: AbstractStorage):
        path = f"test-copy-same-dir-{uid()}"
        try:
            await storage.mkdir(path)
            with pytest.raises(IsADirectoryError):
                await storage.copy(path, path, overwrite=True)
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(path)


class TestDeleteMany:
    """delete_many() tests."""

    async def test_delete_many_files(self, storage: AbstractStorage):
        p1 = f"test-dm-1-{uid()}"
        p2 = f"test-dm-2-{uid()}"
        p3 = f"test-dm-3-{uid()}"
        try:
            await storage.upload_bytes(b"a", p1)
            await storage.upload_bytes(b"b", p2)
            await storage.upload_bytes(b"c", p3)

            await storage.delete_many(p1, p2, p3)

            assert not await storage.exists(p1)
            assert not await storage.exists(p2)
            assert not await storage.exists(p3)
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(p1)
            with contextlib.suppress(Exception):
                await storage.delete(p2)
            with contextlib.suppress(Exception):
                await storage.delete(p3)

    async def test_delete_many_mixed(self, storage: AbstractStorage):
        d = f"test-dm-dir-{uid()}"
        f1 = f"test-dm-f1-{uid()}"
        f2 = f"test-dm-f2-{uid()}"
        try:
            await storage.mkdir(d)
            await storage.upload_bytes(b"x", f1)
            await storage.upload_bytes(b"y", f2)
            await storage.delete_many(d, f1, f2)
            assert not await storage.exists(d)
            assert not await storage.exists(f1)
            assert not await storage.exists(f2)
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(d)
            with contextlib.suppress(Exception):
                await storage.delete(f1)
            with contextlib.suppress(Exception):
                await storage.delete(f2)

    async def test_delete_many_nonexistent(self, storage: AbstractStorage):
        await storage.delete_many(f"nonexistent-{uid()}")

    async def test_delete_many_mixed_missing_paths(self, storage: AbstractStorage):
        existing = f"test-dm-existing-{uid()}"
        missing = f"test-dm-missing-{uid()}"
        await storage.upload_bytes(b"x", existing)
        await storage.delete_many(missing, existing)
        assert not await storage.exists(existing)

    async def test_delete_many_stops_at_nonempty_directory(self, storage: AbstractStorage):
        deleted = f"test-dm-before-{uid()}"
        blocked = f"test-dm-blocked-{uid()}"
        untouched = f"test-dm-after-{uid()}"
        try:
            await storage.upload_bytes(b"before", deleted)
            await storage.mkdir(blocked)
            await storage.upload_bytes(b"child", f"{blocked}/child.txt")
            await storage.upload_bytes(b"after", untouched)
            with pytest.raises(OSError, match="Directory not empty"):
                await storage.delete_many(deleted, blocked, untouched)
            assert not await storage.exists(deleted)
            assert await storage.is_dir(blocked)
            assert await storage.download_bytes(f"{blocked}/child.txt") == b"child"
            assert await storage.download_bytes(untouched) == b"after"
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(blocked)
            with contextlib.suppress(Exception):
                await storage.delete(untouched)


class TestDownloadStream:
    """download_stream() tests."""

    async def test_download_stream_offset(self, storage: AbstractStorage):
        path = f"test-dl-offset-{uid()}"
        original = b"x" * 100
        try:
            await storage.upload_bytes(original, path)
            chunks = [chunk async for chunk in storage.download_stream(path, offset=50)]
            result = b"".join(chunks)
            assert result == original[50:]
        finally:
            await storage.delete(path)

    @pytest.mark.parametrize("offset", [10, 100])
    async def test_download_stream_at_or_beyond_size_is_empty(self, storage: AbstractStorage, offset: int):
        path = f"test-dl-empty-{uid()}"
        try:
            await storage.upload_bytes(b"x" * 10, path)
            chunks = [chunk async for chunk in storage.download_stream(path, offset=offset)]
            assert chunks == []
        finally:
            await storage.delete(path)

    async def test_download_stream_negative_offset_rejected(self, storage: AbstractStorage):
        path = f"test-dl-negative-{uid()}"
        try:
            await storage.upload_bytes(b"content", path)
            with pytest.raises(ValueError, match="non-negative"):
                await anext(storage.download_stream(path, offset=-1))
        finally:
            await storage.delete(path)
