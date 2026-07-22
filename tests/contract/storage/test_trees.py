"""Shared AbstractStorage contract tests."""

import contextlib

import pytest

from storegate.storage import AbstractStorage
from tests.support.ids import uid


class TestCopytree:
    """copytree() tests."""

    async def test_copytree_basic(self, storage: AbstractStorage):
        src = f"test-ct-src-{uid()}"
        dst = f"test-ct-dst-{uid()}"
        try:
            await storage.mkdir(f"{src}/sub", parents=True)
            await storage.upload_bytes(b"aaa", f"{src}/f1.txt")
            await storage.upload_bytes(b"bbb", f"{src}/sub/f2.txt")

            await storage.copytree(src, dst)

            assert await storage.is_dir(dst)
            assert await storage.is_file(f"{dst}/f1.txt")
            assert await storage.download_bytes(f"{dst}/f1.txt") == b"aaa"
            assert await storage.is_file(f"{dst}/sub/f2.txt")
            assert await storage.download_bytes(f"{dst}/sub/f2.txt") == b"bbb"
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(src)
            with contextlib.suppress(Exception):
                await storage.rmtree(dst)

    async def test_copytree_nonexistent_source(self, storage: AbstractStorage):
        with pytest.raises(NotADirectoryError):
            await storage.copytree(f"nonexistent-{uid()}", f"nonexistent-dst-{uid()}")

    async def test_copytree_overwrite_false(self, storage: AbstractStorage):
        src = f"test-ct-ow-src-{uid()}"
        dst = f"test-ct-ow-dst-{uid()}"
        try:
            await storage.mkdir(f"{src}/sub", parents=True)
            await storage.upload_bytes(b"aaa", f"{src}/f1.txt")
            await storage.upload_bytes(b"bbb", f"{src}/sub/f2.txt")

            await storage.mkdir(dst)
            await storage.upload_bytes(b"existing", f"{dst}/f1.txt")

            with pytest.raises(FileExistsError):
                await storage.copytree(src, dst, overwrite=False)
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(src)
            with contextlib.suppress(Exception):
                await storage.rmtree(dst)


class TestMovetree:
    """movetree() tests."""

    async def test_movetree_basic(self, storage: AbstractStorage):
        src = f"test-mt-src-{uid()}"
        dst = f"test-mt-dst-{uid()}"
        try:
            await storage.mkdir(f"{src}/sub", parents=True)
            await storage.upload_bytes(b"aaa", f"{src}/f1.txt")
            await storage.upload_bytes(b"bbb", f"{src}/sub/f2.txt")

            await storage.movetree(src, dst)

            assert not await storage.exists(src)
            assert await storage.is_dir(dst)
            assert await storage.is_file(f"{dst}/f1.txt")
            assert await storage.download_bytes(f"{dst}/f1.txt") == b"aaa"
            assert await storage.is_file(f"{dst}/sub/f2.txt")
            assert await storage.download_bytes(f"{dst}/sub/f2.txt") == b"bbb"
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(src)
            with contextlib.suppress(Exception):
                await storage.rmtree(dst)

    async def test_movetree_nonexistent_source(self, storage: AbstractStorage):
        with pytest.raises(NotADirectoryError):
            await storage.movetree(f"nonexistent-{uid()}", f"nonexistent-dst-{uid()}")
