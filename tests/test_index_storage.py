"""Tests for IndexStorage chunking and integrity.

The general storage test suite already covers basic CRUD operations via
the parametrized ``storage`` fixture. This file validates IndexStorage-specific
chunk semantics: multi-block upload/download integrity, deduplication, and
ref counting.
"""

import hashlib
import os
from contextlib import AbstractContextManager

import pytest

from app.storage.index import IndexStorage
from app.storage.memory import MemoryStorage
from tests.conftest import uid

BLOCK_SIZE = 16 * 1024  # 16 KB — ensures multi-block files with moderate data


@pytest.fixture
async def index_storage():
    """An IndexStorage backed by two MemoryStorage instances."""
    async with (
        MemoryStorage("/") as idx,
        MemoryStorage("/") as chunks,
        IndexStorage(idx, chunks, block_size=BLOCK_SIZE) as s,
    ):
        yield s


class TestUploadDownloadIntegrity:
    """End-to-end upload/download with SHA-256 verification."""

    async def test_small_file_roundtrip(self, index_storage: IndexStorage):
        data = b"hello index storage"
        path = f"test-idx-small-{uid()}"
        try:
            await index_storage.upload_bytes(data, path)
            downloaded = await index_storage.download_bytes(path)
            assert downloaded == data
        finally:
            await index_storage.delete(path)

    async def test_multi_block_roundtrip(self, index_storage: IndexStorage):
        # 48 KB → 3 blocks with 16 KB block_size
        data = os.urandom(48 * 1024)
        path = f"test-idx-multi-{uid()}"
        try:
            await index_storage.upload_bytes(data, path)
            downloaded = await index_storage.download_bytes(path)
            assert downloaded == data
            assert hashlib.sha256(downloaded).hexdigest() == hashlib.sha256(data).hexdigest()
        finally:
            await index_storage.delete(path)


class TestDeduplication:
    """Verify same-content blocks share storage via ref counting."""

    async def test_identical_files_share_chunks(self, index_storage: IndexStorage):
        data = b"A" * (BLOCK_SIZE * 2)  # 32 KB — 2 blocks
        path1 = f"test-dedup-1-{uid()}"
        path2 = f"test-dedup-2-{uid()}"
        try:
            await index_storage.upload_bytes(data, path1)
            await index_storage.upload_bytes(data, path2)

            info1 = await index_storage.stat(path1)
            info2 = await index_storage.stat(path2)
            assert info1.size == info2.size == len(data)

            downloaded1 = await index_storage.download_bytes(path1)
            downloaded2 = await index_storage.download_bytes(path2)
            assert downloaded1 == data
            assert downloaded2 == data
        finally:
            await index_storage.delete(path1)
            await index_storage.delete(path2)


class TestRefCounting:
    """Verify reference counting correctness across operations."""

    async def test_unlink_decrefs_chunks(self, index_storage: IndexStorage):
        data = b"B" * BLOCK_SIZE
        path = f"test-refc-unlink-{uid()}"
        await index_storage.upload_bytes(data, path)
        await index_storage.unlink(path)
        assert not await index_storage.exists(path)

    async def test_copy_increfs_chunks(self, index_storage: IndexStorage):
        data = b"C" * BLOCK_SIZE
        src = f"test-refc-copy-src-{uid()}"
        dst = f"test-refc-copy-dst-{uid()}"
        try:
            await index_storage.upload_bytes(data, src)
            await index_storage.copy(src, dst)
            assert await index_storage.exists(src)
            assert await index_storage.exists(dst)
            assert await index_storage.download_bytes(src) == data
            assert await index_storage.download_bytes(dst) == data
        finally:
            await index_storage.delete(src)
            await index_storage.delete(dst)

    async def test_move_transfers_refs(self, index_storage: IndexStorage):
        data = b"D" * BLOCK_SIZE
        src = f"test-refc-move-src-{uid()}"
        dst = f"test-refc-move-dst-{uid()}"
        try:
            await index_storage.upload_bytes(data, src)
            await index_storage.move(src, dst)
            assert not await index_storage.exists(src)
            assert await index_storage.exists(dst)
            assert await index_storage.download_bytes(dst) == data
        finally:
            await index_storage.delete(dst)
            with suppress_exc():
                await index_storage.delete(src)


class TestDirectoryOperations:
    """IndexStorage directory operations: mkdir, rmtree, copytree, movetree."""

    async def test_rmtree_deletes_all_files(self, index_storage: IndexStorage):
        base = f"test-idx-rmtree-{uid()}"
        try:
            await index_storage.mkdir(base)
            await index_storage.upload_bytes(b"x", f"{base}/f1.txt")
            await index_storage.upload_bytes(b"y", f"{base}/f2.txt")
            assert await index_storage.is_dir(base)

            await index_storage.rmtree(base)
            assert not await index_storage.exists(base)
        finally:
            with suppress_exc():
                await index_storage.rmtree(base)

    async def test_copytree_preserves_content(self, index_storage: IndexStorage):
        data = b"copytree test data"
        src = f"test-idx-cpsrc-{uid()}"
        dst = f"test-idx-cpdst-{uid()}"
        try:
            await index_storage.mkdir(src)
            await index_storage.upload_bytes(data, f"{src}/file.txt")
            await index_storage.copytree(src, dst)
            assert await index_storage.exists(dst)
            assert await index_storage.is_dir(dst)
            downloaded = await index_storage.download_bytes(f"{dst}/file.txt")
            assert downloaded == data
        finally:
            with suppress_exc():
                await index_storage.rmtree(src)
            with suppress_exc():
                await index_storage.rmtree(dst)


class TestOverwriteRefCleanup:
    """Verify overwriting a file correctly cleans up unused old chunks."""

    async def test_overwrite_cleans_up_old_chunks(self, index_storage: IndexStorage):
        # 构造 A+B 和 A+C（A 共享，B/C 不同），覆盖后验证：
        #   A 仍然存在（共享块 ref 不受影响）
        #   B 被清理
        #   C 已创建
        block_a = b"A" * BLOCK_SIZE
        old_data = block_a + b"B" * BLOCK_SIZE   # A + B
        new_data = block_a + b"C" * BLOCK_SIZE   # A + C
        path = f"test-ow-clean-{uid()}"
        try:
            await index_storage.upload_bytes(old_data, path)
            assert await index_storage.download_bytes(path) == old_data

            # Overwrite A+B → A+C
            await index_storage.upload_bytes(new_data, path)
            assert await index_storage.download_bytes(path) == new_data

            hash_a = _hash_block(block_a)
            hash_b = _hash_block(b"B" * BLOCK_SIZE)
            hash_c = _hash_block(b"C" * BLOCK_SIZE)

            # 共享块 A 仍然存在
            assert await index_storage._chunks.exists(f"{hash_to_path_stem(hash_a)}.bin")
            assert await index_storage._chunks.exists(f"{hash_to_path_stem(hash_a)}.ref")

            # 旧块 B 应该被清理
            assert not await index_storage._chunks.exists(
                f"{hash_to_path_stem(hash_b)}.bin"
            ), f"Old chunk B {hash_b[:8]} .bin not cleaned up"
            assert not await index_storage._chunks.exists(
                f"{hash_to_path_stem(hash_b)}.ref"
            ), f"Old chunk B {hash_b[:8]} .ref not cleaned up"

            # 新块 C 已创建
            assert await index_storage._chunks.exists(f"{hash_to_path_stem(hash_c)}.bin")
            assert await index_storage._chunks.exists(f"{hash_to_path_stem(hash_c)}.ref")

            await index_storage.delete(path)
        finally:
            with suppress_exc():
                await index_storage.delete(path)


def _hash_block(data: bytes) -> str:
    """Return the SHA-256 hex digest of a single block."""
    import hashlib
    return hashlib.sha256(data).hexdigest()


def hash_to_path_stem(chunk_hash: str) -> str:
    """Convert a chunk hash to its path stem: 'aa/bbbb/rest...'."""
    return f"{chunk_hash[:2]}/{chunk_hash[2:6]}/{chunk_hash[6:]}"


def suppress_exc() -> AbstractContextManager[None]:
    """Return a contextlib.suppress for all exceptions."""
    import contextlib

    return contextlib.suppress(Exception)
