"""Shared AbstractStorage contract tests."""

from __future__ import annotations

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
        with pytest.raises(FileNotFoundError):
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
        with pytest.raises(FileNotFoundError):
            await storage.movetree(f"nonexistent-{uid()}", f"nonexistent-dst-{uid()}")


class TestTreeOperationMatrix:
    @pytest.mark.parametrize("operation", ["copytree", "movetree"])
    async def test_same_path_overwrite_true_is_noop(self, storage: AbstractStorage, operation: str) -> None:
        path = f"test-tree-same-true-{uid()}"
        try:
            await storage.mkdir(path)
            await storage.upload_bytes(b"content", f"{path}/file.txt")
            await getattr(storage, operation)(path, path, overwrite=True)
            assert await storage.download_bytes(f"{path}/file.txt") == b"content"
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(path)

    @pytest.mark.parametrize("operation", ["copytree", "movetree"])
    async def test_same_path_overwrite_false_raises(self, storage: AbstractStorage, operation: str) -> None:
        path = f"test-tree-same-false-{uid()}"
        try:
            await storage.mkdir(path)
            await storage.upload_bytes(b"content", f"{path}/file.txt")
            with pytest.raises(FileExistsError):
                await getattr(storage, operation)(path, path, overwrite=False)
            assert await storage.download_bytes(f"{path}/file.txt") == b"content"
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(path)

    @pytest.mark.parametrize("operation", ["copytree", "movetree"])
    @pytest.mark.parametrize("overwrite", [False, True])
    async def test_same_path_missing_source_raises_file_not_found(
        self,
        storage: AbstractStorage,
        operation: str,
        overwrite: bool,
    ) -> None:
        path = f"test-tree-same-missing-{uid()}"
        with pytest.raises(FileNotFoundError):
            await getattr(storage, operation)(path, path, overwrite=overwrite)

    @pytest.mark.parametrize("operation", ["copytree", "movetree"])
    @pytest.mark.parametrize("overwrite", [False, True])
    async def test_file_source_is_not_a_tree(
        self,
        storage: AbstractStorage,
        operation: str,
        overwrite: bool,
    ) -> None:
        source = f"test-tree-file-source-{uid()}"
        destination = f"test-tree-file-destination-{uid()}"
        try:
            await storage.upload_bytes(b"content", source)
            with pytest.raises(NotADirectoryError):
                await getattr(storage, operation)(source, destination, overwrite=overwrite)
            assert await storage.download_bytes(source) == b"content"
        finally:
            with contextlib.suppress(Exception):
                await storage.delete(source)
            with contextlib.suppress(Exception):
                await storage.rmtree(destination)

    @pytest.mark.parametrize("operation", ["copytree", "movetree"])
    async def test_existing_destination_obeys_overwrite(self, storage: AbstractStorage, operation: str) -> None:
        source = f"test-tree-overwrite-source-{uid()}"
        destination = f"test-tree-overwrite-destination-{uid()}"
        try:
            await storage.mkdir(source)
            await storage.upload_bytes(b"new", f"{source}/new.txt")
            await storage.mkdir(destination)
            await storage.upload_bytes(b"keep", f"{destination}/keep.txt")

            with pytest.raises(FileExistsError):
                await getattr(storage, operation)(source, destination, overwrite=False)
            assert await storage.download_bytes(f"{source}/new.txt") == b"new"
            assert await storage.download_bytes(f"{destination}/keep.txt") == b"keep"

            await getattr(storage, operation)(source, destination, overwrite=True)
            assert await storage.download_bytes(f"{destination}/new.txt") == b"new"
            assert await storage.download_bytes(f"{destination}/keep.txt") == b"keep"
            if operation == "copytree":
                assert await storage.download_bytes(f"{source}/new.txt") == b"new"
            else:
                assert not await storage.exists(source)
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(source)
            with contextlib.suppress(Exception):
                await storage.rmtree(destination)
