"""General storage interface tests — run against all storage backends."""

import contextlib

import pytest

from app.storage import AbstractStorage
from tests.conftest import uid


class TestMkdir:
    """mkdir() tests."""

    async def test_create_single(self, storage: AbstractStorage):
        path = f"test-mkdir-{uid()}"
        try:
            await storage.mkdir(path)
            assert await storage.is_dir(path)
            assert not await storage.is_file(path)
        finally:
            await storage.delete(path)

    async def test_exist_ok_idempotent(self, storage: AbstractStorage):
        path = f"test-mkdir-eo-{uid()}"
        try:
            await storage.mkdir(path)
            await storage.mkdir(path, exist_ok=True)
            assert await storage.is_dir(path)
        finally:
            await storage.delete(path)

    async def test_duplicate_raises(self, storage: AbstractStorage):
        path = f"test-mkdir-dup-{uid()}"
        try:
            await storage.mkdir(path)
            with pytest.raises(FileExistsError):
                await storage.mkdir(path)
        finally:
            await storage.delete(path)

    async def test_file_name_conflict_raises(self, storage: AbstractStorage):
        path = f"test-mkdir-conflict-{uid()}"
        try:
            await storage.upload_bytes(b"hello", path)
            with pytest.raises(FileExistsError):
                await storage.mkdir(path)
        finally:
            await storage.delete(path)

    async def test_parents_creates_ancestors(self, storage: AbstractStorage):
        base = f"test-mkdir-p-{uid()}"
        path = f"{base}/a/b"
        try:
            await storage.mkdir(path, parents=True)
            assert await storage.is_dir(base)
            assert await storage.is_dir(f"{base}/a")
            assert await storage.is_dir(path)
        finally:
            await storage.rmtree(base)

    async def test_missing_parent_raises(self, storage: AbstractStorage):
        base = f"test-mkdir-np-{uid()}"
        path = f"{base}/child"
        try:
            with pytest.raises(FileNotFoundError):
                await storage.mkdir(path, parents=False)
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(base)

    async def test_root_directory(self, storage: AbstractStorage):
        await storage.mkdir("/", exist_ok=True)
        await storage.mkdir("", exist_ok=True)
        with pytest.raises(FileExistsError):
            await storage.mkdir("/")


class TestIsDir:
    """is_dir() tests."""

    async def test_marker_directory(self, storage: AbstractStorage):
        path = f"test-isdir-{uid()}"
        try:
            await storage.mkdir(path)
            assert await storage.is_dir(path)
        finally:
            await storage.delete(path)

    async def test_root(self, storage: AbstractStorage):
        assert await storage.is_dir("/")
        assert await storage.is_dir("")

    async def test_nonexistent(self, storage: AbstractStorage):
        assert not await storage.is_dir(f"nonexistent-{uid()}")

    async def test_file_path(self, storage: AbstractStorage):
        path = f"test-isdir-file-{uid()}"
        try:
            await storage.upload_bytes(b"hello", path)
            assert not await storage.is_dir(path)
        finally:
            await storage.delete(path)


class TestStat:
    """stat() tests."""

    async def test_directory(self, storage: AbstractStorage):
        path = f"test-stat-dir-{uid()}"
        try:
            await storage.mkdir(path)
            info = await storage.stat(path)
            assert info.is_dir
            assert info.name == path
            assert info.size == 0
        finally:
            await storage.delete(path)

    async def test_file(self, storage: AbstractStorage):
        path = f"test-stat-file-{uid()}"
        try:
            await storage.upload_bytes(b"hello world", path)
            info = await storage.stat(path)
            assert not info.is_dir
            assert info.size == 11
        finally:
            await storage.delete(path)

    async def test_root(self, storage: AbstractStorage):
        info = await storage.stat("/")
        assert info.is_dir

    async def test_nonexistent_raises(self, storage: AbstractStorage):
        with pytest.raises(FileNotFoundError):
            await storage.stat(f"nonexistent-{uid()}")


class TestExists:
    """exists() tests."""

    async def test_file(self, storage: AbstractStorage):
        path = f"test-exists-file-{uid()}"
        try:
            await storage.upload_bytes(b"hello", path)
            assert await storage.exists(path)
        finally:
            await storage.delete(path)

    async def test_directory(self, storage: AbstractStorage):
        path = f"test-exists-dir-{uid()}"
        try:
            await storage.mkdir(path)
            assert await storage.exists(path)
        finally:
            await storage.delete(path)

    async def test_root(self, storage: AbstractStorage):
        assert await storage.exists("/")
        assert await storage.exists("")

    async def test_nonexistent(self, storage: AbstractStorage):
        assert not await storage.exists(f"nonexistent-{uid()}")


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
            with pytest.raises(OSError):  # noqa: PT011
                await storage.delete(path)
        finally:
            await storage.rmtree(path)

    async def test_file(self, storage: AbstractStorage):
        path = f"test-del-file-{uid()}"
        await storage.upload_bytes(b"hello", path)
        assert await storage.is_file(path)
        await storage.delete(path)
        assert not await storage.exists(path)

    async def test_nonexistent_silent(self, storage: AbstractStorage):
        # Some backends silently succeed (COS), others raise FileNotFoundError.
        # Both are acceptable per AbstractStorage.rmdir docstring.
        with contextlib.suppress(FileNotFoundError):
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
            with pytest.raises((IsADirectoryError, OSError)):
                await storage.unlink(path)
        finally:
            await storage.delete(path)

    async def test_nonexistent_missing_ok(self, storage: AbstractStorage):
        await storage.unlink(f"nonexistent-{uid()}", missing_ok=True)


class TestRmdir:
    """rmdir() tests."""

    async def test_empty_directory(self, storage: AbstractStorage):
        path = f"test-rmdir-dir-{uid()}"
        await storage.mkdir(path)
        assert await storage.is_dir(path)
        await storage.rmdir(path)
        assert not await storage.exists(path)

    async def test_nonempty_directory_raises(self, storage: AbstractStorage):
        path = f"test-rmdir-ned-{uid()}"
        try:
            await storage.mkdir(path)
            await storage.upload_bytes(b"hello", f"{path}/file.txt")
            with pytest.raises(OSError):  # noqa: PT011
                await storage.rmdir(path)
        finally:
            await storage.rmtree(path)

    async def test_file_raises_not_a_directory_error(self, storage: AbstractStorage):
        path = f"test-rmdir-file-{uid()}"
        try:
            await storage.upload_bytes(b"hello", path)
            with pytest.raises(NotADirectoryError):
                await storage.rmdir(path)
        finally:
            await storage.delete(path)


class TestRmtree:
    """rmtree() tests."""

    async def test_recursive_delete(self, storage: AbstractStorage):
        base = f"test-rmtree-{uid()}"
        try:
            await storage.mkdir(f"{base}/a/b", parents=True)
            await storage.upload_bytes(b"hello", f"{base}/file1.txt")
            await storage.upload_bytes(b"world", f"{base}/a/file2.txt")
            await storage.upload_bytes(b"!", f"{base}/a/b/file3.txt")
            assert await storage.is_dir(base)

            await storage.rmtree(base)
            assert not await storage.exists(base)
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(base)


class TestIterdir:
    """iterdir() tests."""

    async def test_files_and_marked_subdirs(self, storage: AbstractStorage):
        base = f"test-itd-{uid()}"
        try:
            await storage.mkdir(f"{base}/sub", parents=True)
            await storage.upload_bytes(b"a", f"{base}/a.txt")
            await storage.upload_bytes(b"b", f"{base}/b.txt")

            entries = [e async for e in storage.iterdir(base)]
            names = {e.name for e in entries}

            assert "sub" in names, f"sub not in {names}"
            assert "a.txt" in names, f"a.txt not in {names}"
            assert "b.txt" in names, f"b.txt not in {names}"
            assert len(entries) == 3, f"Expected 3 entries, got {len(entries)}"

            sub_info = next(e for e in entries if e.name == "sub")
            assert sub_info.is_dir
        finally:
            await storage.rmtree(base)


class TestWalk:
    """walk() + rmtree round-trip."""

    async def test_walk_rmtree_roundtrip(self, storage: AbstractStorage):
        base = f"test-walk-{uid()}"
        try:
            await storage.mkdir(f"{base}/d1", parents=True)
            await storage.mkdir(f"{base}/d2", parents=True)
            await storage.upload_bytes(b"x", f"{base}/f1.txt")
            await storage.upload_bytes(b"y", f"{base}/d1/f2.txt")
            await storage.upload_bytes(b"z", f"{base}/d2/f3.txt")

            all_files: set[str] = set()
            dir_count = 0
            async for _sp, _sd, sf in storage.walk(base):
                dir_count += 1
                all_files.update(f.name for f in sf)

            # Should have at least base + d1 + d2 directories
            assert dir_count >= 3
            assert "f1.txt" in all_files
            assert "f2.txt" in all_files
            assert "f3.txt" in all_files

            await storage.rmtree(base)
            assert not await storage.exists(base)
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(base)


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
