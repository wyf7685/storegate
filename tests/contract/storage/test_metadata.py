"""Shared AbstractStorage contract tests."""

from pathlib import PurePosixPath

import pytest

from app.storage import AbstractStorage, EntryKind, FileInfo
from tests.support.ids import uid


def _assert_kind(info: FileInfo, expected: EntryKind) -> None:
    assert info.kind is expected
    assert (info.is_file, info.is_dir, info.is_symlink) == {
        EntryKind.FILE: (True, False, False),
        EntryKind.DIRECTORY: (False, True, False),
        EntryKind.SYMLINK: (False, False, True),
    }[expected]


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

    async def test_dot(self, storage: AbstractStorage):
        """Dot is treated as root directory."""
        assert await storage.is_dir(".")

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
            _assert_kind(info, EntryKind.DIRECTORY)
            assert info.name == path
            assert info.size == 0
        finally:
            await storage.delete(path)

    async def test_file(self, storage: AbstractStorage):
        path = f"test-stat-file-{uid()}"
        try:
            await storage.upload_bytes(b"hello world", path)
            info = await storage.stat(path)
            _assert_kind(info, EntryKind.FILE)
            assert info.size == 11
        finally:
            await storage.delete(path)

    async def test_root(self, storage: AbstractStorage):
        info = await storage.stat("/")
        _assert_kind(info, EntryKind.DIRECTORY)

    async def test_nonexistent_raises(self, storage: AbstractStorage):
        with pytest.raises(FileNotFoundError):
            await storage.stat(f"nonexistent-{uid()}")

    async def test_path_is_absolute(self, storage: AbstractStorage):
        """stat() returns FileInfo.path as an absolute POSIX path."""
        path = f"test-stat-abs-{uid()}"
        try:
            await storage.upload_bytes(b"hello", path)
            info = await storage.stat(path)
            assert info.path.startswith("/"), f"Expected absolute path, got {info.path!r}"
        finally:
            await storage.delete(path)

    async def test_path_relative_absolute_equivalent(self, storage: AbstractStorage):
        """stat() returns the same FileInfo.path for relative and absolute input."""
        path = f"test-stat-equiv-{uid()}"
        try:
            await storage.upload_bytes(b"hello", path)
            info_rel = await storage.stat(path)
            info_abs = await storage.stat(f"/{path}")
            assert info_rel.path == info_abs.path, (
                f"Relative input gave {info_rel.path!r}, absolute gave {info_abs.path!r}"
            )
            assert info_rel.size == info_abs.size
        finally:
            await storage.delete(path)

    async def test_with_pure_posix_path(self, storage: AbstractStorage):
        """stat() accepts PurePosixPath as input."""

        path = f"test-stat-ppp-{uid()}"
        try:
            await storage.upload_bytes(b"data", path)
            info = await storage.stat(PurePosixPath(path))
            assert info.path.startswith("/")
            assert info.size == 4
        finally:
            await storage.delete(path)


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

    async def test_dot(self, storage: AbstractStorage):
        """Dot is treated as root directory."""
        assert await storage.exists(".")

    async def test_nonexistent(self, storage: AbstractStorage):
        assert not await storage.exists(f"nonexistent-{uid()}")


class TestPing:
    """ping() tests."""

    async def test_ping(self, storage: AbstractStorage):
        assert await storage.ping() is True
