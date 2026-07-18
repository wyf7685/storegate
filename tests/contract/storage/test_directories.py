"""Shared AbstractStorage contract tests."""

import contextlib

import pytest

from app.storage import AbstractStorage, EntryKind, WalkEntry
from tests.support.ids import uid


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

    async def test_child_paths_are_absolute(self, storage: AbstractStorage):
        """iterdir() returns FileInfo.path as absolute POSIX paths."""
        base = f"test-itd-abs-{uid()}"
        try:
            await storage.mkdir(f"{base}/sub", parents=True)
            await storage.upload_bytes(b"a", f"{base}/a.txt")

            async for entry in storage.iterdir(base):
                assert entry.path.startswith("/"), (
                    f"Expected absolute path, got {entry.path!r} for entry {entry.name!r}"
                )
        finally:
            await storage.rmtree(base)

    async def test_root_directory(self, storage: AbstractStorage):
        """iterdir("/") lists root-level entries."""
        path = f"test-itd-root-{uid()}"
        try:
            await storage.upload_bytes(b"x", path)
            entries = [e async for e in storage.iterdir("/")]
            names = {e.name for e in entries}
            assert path in names, f"{path!r} not found in root listing: {names}"
        finally:
            await storage.delete(path)


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
            async for walk_entry in storage.walk(base):
                assert isinstance(walk_entry, WalkEntry)
                dir_count += 1
                all_files.update(entry.name for entry in walk_entry.entries if entry.kind is EntryKind.FILE)

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

    async def test_paths_are_absolute(self, storage: AbstractStorage):
        """walk() yields absolute paths for root and all child entries."""
        base = f"test-walk-abs-{uid()}"
        try:
            await storage.mkdir(f"{base}/sub", parents=True)
            await storage.upload_bytes(b"x", f"{base}/f.txt")

            async for walk_entry in storage.walk(base):
                assert isinstance(walk_entry, WalkEntry)
                assert walk_entry.path.startswith("/"), f"Walk root path must be absolute, got {walk_entry.path!r}"
                assert tuple(entry.path for entry in walk_entry.entries) == tuple(
                    sorted(entry.path for entry in walk_entry.entries)
                )
                for entry in walk_entry.entries:
                    assert entry.path.startswith("/"), f"Entry path must be absolute, got {entry.path!r}"
        finally:
            await storage.rmtree(base)

    async def test_root_directory(self, storage: AbstractStorage):
        """walk("/") yields absolute root and lists root-level children."""
        path = f"test-walk-root-{uid()}"
        try:
            await storage.mkdir(path)
            await storage.upload_bytes(b"x", f"{path}/f.txt")

            found = False
            async for walk_entry in storage.walk("/"):
                assert isinstance(walk_entry, WalkEntry)
                assert walk_entry.path.startswith("/"), f"Root path must be absolute, got {walk_entry.path!r}"
                for entry in walk_entry.entries:
                    if entry.name == path:
                        found = True
                        assert entry.kind is EntryKind.DIRECTORY
                        assert (entry.is_file, entry.is_dir, entry.is_symlink) == (False, True, False)
            assert found, f"{path!r} not found in root walk"
        finally:
            await storage.rmtree(path)


class TestList:
    """list_() tests."""

    async def test_list_directory(self, storage: AbstractStorage):
        base = f"test-list-dir-{uid()}"
        try:
            await storage.mkdir(base)
            await storage.upload_bytes(b"hello", f"{base}/a.txt")
            await storage.upload_bytes(b"world", f"{base}/b.txt")

            result = await storage.list_(base)
            names = {e.name for e in result}

            assert len(result) == 2, f"Expected 2 entries, got {len(result)}"
            assert "a.txt" in names
            assert "b.txt" in names
        finally:
            await storage.rmtree(base)

    async def test_list_empty_directory(self, storage: AbstractStorage):
        base = f"test-list-empty-{uid()}"
        try:
            await storage.mkdir(base)
            result = await storage.list_(base)
            assert len(result) == 0
        finally:
            await storage.delete(base)

    async def test_paths_are_absolute(self, storage: AbstractStorage):
        """list_() returns FileInfo.path as absolute POSIX paths."""
        base = f"test-list-abs-{uid()}"
        try:
            await storage.mkdir(base)
            await storage.upload_bytes(b"hello", f"{base}/a.txt")

            result = await storage.list_(base)
            for e in result:
                assert e.path.startswith("/"), f"Expected absolute path, got {e.path!r} for entry {e.name!r}"
        finally:
            await storage.rmtree(base)
