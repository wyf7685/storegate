"""Tests for IndexStorage chunking and integrity.

The general storage test suite already covers basic CRUD operations via
the parametrized ``storage`` fixture. This file validates IndexStorage-specific
chunk semantics: multi-block upload/download integrity, deduplication, and
ref counting.
"""

import contextlib
import hashlib
import os
from contextlib import AbstractContextManager

import pytest
from pytest_mock import MockerFixture

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

            meta1 = await index_storage._get_file_meta(path1)
            meta2 = await index_storage._get_file_meta(path2)
            assert meta1 is not None
            assert meta2 is not None
            assert meta1.chunks == meta2.chunks, "Identical files must share the same chunk hashes"
            assert len(meta1.chunks) == 2

            # Verify each chunk .bin exists once and .ref lists both paths
            for chunk_hash in meta1.chunks:
                assert await index_storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.bin"), (
                    f"Chunk {chunk_hash[:8]} .bin should exist"
                )
                refs = await index_storage._chunk_load_refs(chunk_hash)
                assert refs is not None, f"Chunk {chunk_hash[:8]} .ref should exist"
                abs1 = f"/{path1}"
                abs2 = f"/{path2}"
                assert abs1 in refs, f"Chunk {chunk_hash[:8]} refs should include {abs1}"
                assert abs2 in refs, f"Chunk {chunk_hash[:8]} refs should include {abs2}"

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
        meta_before = await index_storage._get_file_meta(path)
        assert meta_before is not None
        chunk_hashes = meta_before.chunks[:]

        await index_storage.unlink(path)
        assert not await index_storage.exists(path)

        # Verify chunk .bin + .ref cleaned up (refs reached 0)
        for chunk_hash in chunk_hashes:
            assert not await index_storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.bin"), (
                f"Chunk {chunk_hash[:8]} .bin should be deleted after unlink"
            )
            assert not await index_storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.ref"), (
                f"Chunk {chunk_hash[:8]} .ref should be deleted after unlink"
            )

        # Verify FileMeta removed from index
        assert await index_storage._get_file_meta(path) is None

    async def test_copy_increfs_chunks(self, index_storage: IndexStorage):
        data = b"C" * BLOCK_SIZE
        src = f"test-refc-copy-src-{uid()}"
        dst = f"test-refc-copy-dst-{uid()}"
        try:
            await index_storage.upload_bytes(data, src)

            src_meta = await index_storage._get_file_meta(src)
            assert src_meta is not None
            refs_before: dict[str, set[str]] = {}
            for h in src_meta.chunks:
                r = await index_storage._chunk_load_refs(h)
                refs_before[h] = r or set()

            await index_storage.copy(src, dst)
            assert await index_storage.exists(src)
            assert await index_storage.exists(dst)

            dst_meta = await index_storage._get_file_meta(dst)
            assert dst_meta is not None
            assert dst_meta.chunks == src_meta.chunks, "Copy must preserve chunk hash list"

            # Verify incref: src + dst both in ref files, count increased by 1
            for chunk_hash in src_meta.chunks:
                assert await index_storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.bin"), (
                    f"Chunk {chunk_hash[:8]} .bin must exist after copy"
                )
                refs = await index_storage._chunk_load_refs(chunk_hash)
                assert refs is not None, f"Chunk {chunk_hash[:8]} .ref must exist"
                abs_src = f"/{src}"
                abs_dst = f"/{dst}"
                assert abs_src in refs, f"src path must remain in chunk {chunk_hash[:8]} refs"
                assert abs_dst in refs, f"dst path must be added to chunk {chunk_hash[:8]} refs"
                assert len(refs) == len(refs_before[chunk_hash]) + 1, (
                    f"Chunk {chunk_hash[:8]} ref count should increase by 1 after copy"
                )

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

            src_meta = await index_storage._get_file_meta(src)
            assert src_meta is not None
            refs_before: dict[str, set[str]] = {}
            for h in src_meta.chunks:
                r = await index_storage._chunk_load_refs(h)
                refs_before[h] = r or set()

            await index_storage.move(src, dst)
            assert not await index_storage.exists(src)
            assert await index_storage.exists(dst)

            dst_meta = await index_storage._get_file_meta(dst)
            assert dst_meta is not None
            assert dst_meta.chunks == src_meta.chunks, "Move must preserve chunk hash list"

            # Verify transref: src removed, dst added, count unchanged
            abs_src = f"/{src}"
            abs_dst = f"/{dst}"
            for chunk_hash in src_meta.chunks:
                assert await index_storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.bin"), (
                    f"Chunk {chunk_hash[:8]} .bin must exist after move"
                )
                refs = await index_storage._chunk_load_refs(chunk_hash)
                assert refs is not None, f"Chunk {chunk_hash[:8]} .ref must exist"
                assert abs_src not in refs, f"src path must be removed from chunk {chunk_hash[:8]} refs"
                assert abs_dst in refs, f"dst path must be added to chunk {chunk_hash[:8]} refs"
                assert len(refs) == len(refs_before[chunk_hash]), (
                    f"Chunk {chunk_hash[:8]} ref count unchanged after move (transfer, not create+destroy)"
                )

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

            # Collect chunk hashes before deletion
            all_chunks: list[str] = []
            for fname in ("f1.txt", "f2.txt"):
                meta = await index_storage._get_file_meta(f"{base}/{fname}")
                assert meta is not None
                all_chunks.extend(meta.chunks)

            await index_storage.rmtree(base)
            assert not await index_storage.exists(base)

            # Verify chunk .bin + .ref are cleaned up
            for chunk_hash in set(all_chunks):
                assert not await index_storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.bin"), (
                    f"Chunk {chunk_hash[:8]} .bin not cleaned up by rmtree"
                )
                assert not await index_storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.ref"), (
                    f"Chunk {chunk_hash[:8]} .ref not cleaned up by rmtree"
                )

            # Verify individual file metas removed from index
            assert await index_storage._get_file_meta(f"{base}/f1.txt") is None
            assert await index_storage._get_file_meta(f"{base}/f2.txt") is None
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

            # Verify chunk refs are shared between src and dst
            src_file = f"{src}/file.txt"
            dst_file = f"{dst}/file.txt"
            meta_src = await index_storage._get_file_meta(src_file)
            meta_dst = await index_storage._get_file_meta(dst_file)
            assert meta_src is not None, "Source file meta must exist after copytree"
            assert meta_dst is not None, "Destination file meta must exist after copytree"
            assert meta_src.chunks == meta_dst.chunks, "Source and destination must share chunk hashes after copytree"

            for chunk_hash in meta_src.chunks:
                refs = await index_storage._chunk_load_refs(chunk_hash)
                assert refs is not None, f"Chunk {chunk_hash[:8]} .ref must exist"
                abs_src_file = f"/{src_file}"
                abs_dst_file = f"/{dst_file}"
                assert abs_src_file in refs, f"src must retain ref to chunk {chunk_hash[:8]}"
                assert abs_dst_file in refs, f"dst must have ref to chunk {chunk_hash[:8]}"
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
        old_data = block_a + b"B" * BLOCK_SIZE  # A + B
        new_data = block_a + b"C" * BLOCK_SIZE  # A + C
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

            chunks = index_storage._chunks
            # 共享块 A 仍然存在
            assert await chunks.exists(f"{hash_to_path_stem(hash_a)}.bin")
            assert await chunks.exists(f"{hash_to_path_stem(hash_a)}.ref")

            # 旧块 B 应该被清理
            assert not await chunks.exists(f"{hash_to_path_stem(hash_b)}.bin"), (
                f"Old chunk B {hash_b[:8]} .bin not cleaned up"
            )
            assert not await chunks.exists(f"{hash_to_path_stem(hash_b)}.ref"), (
                f"Old chunk B {hash_b[:8]} .ref not cleaned up"
            )

            # 新块 C 已创建
            assert await chunks.exists(f"{hash_to_path_stem(hash_c)}.bin")
            assert await chunks.exists(f"{hash_to_path_stem(hash_c)}.ref")

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
    return contextlib.suppress(Exception)


class TestDownloadStreamOffset:
    """download_stream with offset > 0."""

    async def test_offset_middle(self, index_storage: IndexStorage):
        data = b"0123456789" * 100  # 1000 bytes
        path = f"test-dl-offset-{uid()}"
        try:
            await index_storage.upload_bytes(data, path)
            chunks = [chunk async for chunk in index_storage.download_stream(path, offset=500)]
            result = b"".join(chunks)
            assert result == data[500:]
        finally:
            await index_storage.delete(path)

    async def test_offset_beyond_file(self, index_storage: IndexStorage):
        data = b"short"
        path = f"test-dl-offbeyond-{uid()}"
        try:
            await index_storage.upload_bytes(data, path)
            chunks = [chunk async for chunk in index_storage.download_stream(path, offset=100)]
            assert b"".join(chunks) == b""
        finally:
            await index_storage.delete(path)

    async def test_offset_at_chunk_boundary(self, index_storage: IndexStorage):
        # upload 2+ blocks so offset falls at exact chunk boundary
        block_a = b"A" * BLOCK_SIZE
        block_b = b"B" * BLOCK_SIZE
        data = block_a + block_b
        path = f"test-dl-offchunk-{uid()}"
        try:
            await index_storage.upload_bytes(data, path)
            chunks = [chunk async for chunk in index_storage.download_stream(path, offset=BLOCK_SIZE)]
            result = b"".join(chunks)
            assert result == block_b
        finally:
            await index_storage.delete(path)


class TestSkipLocking:
    """IndexStorage with skip_locking=True."""

    async def test_upload_without_locking(self):
        async with (
            MemoryStorage("/") as idx,
            MemoryStorage("/") as chunks,
            IndexStorage(idx, chunks, block_size=BLOCK_SIZE, skip_locking=True) as s,
        ):
            path = f"test-nolock-{uid()}"
            try:
                await s.upload_bytes(b"hello", path)
                assert await s.download_bytes(path) == b"hello"
            finally:
                await s.delete(path)


class TestListDirectory:
    """list_() method."""

    async def test_list_mixed_entries(self, index_storage: IndexStorage):
        base = f"test-idx-list-{uid()}"
        try:
            await index_storage.mkdir(base)
            await index_storage.mkdir(f"{base}/sub", parents=True)
            await index_storage.upload_bytes(b"x", f"{base}/a.txt")
            await index_storage.upload_bytes(b"y", f"{base}/b.txt")

            entries = await index_storage.list_(base)
            names = {e.name for e in entries}
            assert "sub" in names
            assert "a.txt" in names
            assert "b.txt" in names
            assert len(entries) == 3
        finally:
            await index_storage.rmtree(base)


class TestCopyTreeNested:
    """copytree with nested subdirectories (exercises the directory creation loop)."""

    async def test_copytree_nested_dirs(self, index_storage: IndexStorage):
        src = f"test-idx-cpnest-src-{uid()}"
        dst = f"test-idx-cpnest-dst-{uid()}"
        try:
            await index_storage.mkdir(f"{src}/a/b", parents=True)
            await index_storage.upload_bytes(b"x", f"{src}/f1.txt")
            await index_storage.upload_bytes(b"y", f"{src}/a/f2.txt")
            await index_storage.upload_bytes(b"z", f"{src}/a/b/f3.txt")

            await index_storage.copytree(src, dst)
            assert await index_storage.exists(dst)
            assert await index_storage.exists(f"{dst}/f1.txt")
            assert await index_storage.exists(f"{dst}/a/f2.txt")
            assert await index_storage.exists(f"{dst}/a/b/f3.txt")
            assert await index_storage.download_bytes(f"{dst}/a/b/f3.txt") == b"z"
        finally:
            with contextlib.suppress(Exception):
                await index_storage.rmtree(src)
            with contextlib.suppress(Exception):
                await index_storage.rmtree(dst)


class TestRollback:
    """Verify rollback paths for copy, move, and upload_stream fault injection."""

    # ------------------------------------------------------------------
    # upload_stream
    # ------------------------------------------------------------------

    async def test_upload_stream_meta_failure_rolls_back_increfs(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ):
        """When meta write fails after all chunks incref'd, incref'd chunks are decref'd."""
        s = index_storage
        data = b"R" * (BLOCK_SIZE * 2)  # 32 KB → 2 blocks
        path = f"test-rollb-upload-{uid()}"

        original_upload = s._index.upload_bytes

        async def _fail_meta(data_bytes: bytes, remote_path: str, *, overwrite: bool = True) -> None:
            if str(remote_path) == f"/{path}":
                raise OSError("simulated meta write failure")
            await original_upload(data_bytes, remote_path, overwrite=overwrite)

        mocker.patch.object(s._index, "upload_bytes", _fail_meta)

        with pytest.raises(OSError, match="simulated meta write failure"):
            await s.upload_bytes(data, path)

        # File meta must not exist after rollback
        assert await s._get_file_meta(path) is None
        # Source file does not exist yet (first upload failed)

    # ------------------------------------------------------------------
    # copy() — incref failure → rollback_chunks
    # ------------------------------------------------------------------

    async def test_copy_incref_failure_rolls_back(self, index_storage: IndexStorage, mocker: MockerFixture):
        """When _chunk_incref raises during copy, rollback decrefs any incref'd chunks."""
        s = index_storage
        data = b"C" * (BLOCK_SIZE * 2)  # 32 KB → 2 blocks
        src = f"test-rollb-copyinc-src-{uid()}"
        dst = f"test-rollb-copyinc-dst-{uid()}"

        await s.upload_bytes(data, src)
        src_meta = await s._get_file_meta(src)
        assert src_meta is not None
        chunk_hashes = src_meta.chunks[:]

        # Fail _chunk_incref from the 2nd chunk onward
        call_count = 0
        original_incref = s._chunk_incref

        async def _failing_incref(chunk_hash: str, *remote_path: str) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise RuntimeError("simulated incref failure")
            await original_incref(chunk_hash, *remote_path)

        mocker.patch.object(s, "_chunk_incref", _failing_incref)

        with pytest.raises(BaseException) as _exc:  # noqa: PT011
            await s.copy(src, dst)

        # Destination file must not exist
        assert await s._get_file_meta(dst) is None
        # Source file is intact
        assert await s.exists(src)
        assert await s.download_bytes(src) == data
        # All chunk refs must NOT contain dst (rolled back)
        for chunk_hash in chunk_hashes:
            refs = await s._chunk_load_refs(chunk_hash)
            assert refs is not None
            assert f"/{dst}" not in refs, f"dst must be removed from chunk {chunk_hash[:8]} refs"

        with suppress_exc():
            await s.delete(dst)
        await s.delete(src)

    # ------------------------------------------------------------------
    # copy() — meta write failure → rollback_chunks
    # ------------------------------------------------------------------

    async def test_copy_meta_failure_rolls_back(self, index_storage: IndexStorage, mocker: MockerFixture):
        """When meta write fails after incref, all incref'd chunks are rolled back."""
        s = index_storage
        data = b"D" * BLOCK_SIZE
        src = f"test-rollb-copymeta-src-{uid()}"
        dst = f"test-rollb-copymeta-dst-{uid()}"

        await s.upload_bytes(data, src)
        src_meta = await s._get_file_meta(src)
        assert src_meta is not None
        chunk_hashes = src_meta.chunks[:]

        original_upload = s._index.upload_bytes

        async def _fail_meta(data_bytes: bytes, remote_path: str, *, overwrite: bool = True) -> None:
            if str(remote_path) == f"/{dst}":
                raise OSError("simulated meta write failure")
            await original_upload(data_bytes, remote_path, overwrite=overwrite)

        mocker.patch.object(s._index, "upload_bytes", _fail_meta)

        with pytest.raises(OSError, match="simulated meta write failure"):
            await s.copy(src, dst)

        # Destination file must not exist
        assert await s._get_file_meta(dst) is None
        # Source file is intact
        assert await s.download_bytes(src) == data
        # All chunk refs must NOT contain dst (rollback cleaned them)
        for chunk_hash in chunk_hashes:
            refs = await s._chunk_load_refs(chunk_hash)
            assert refs is not None
            assert f"/{dst}" not in refs, f"dst must be removed from chunk {chunk_hash[:8]} refs"

        with suppress_exc():
            await s.delete(dst)
        await s.delete(src)

    # ------------------------------------------------------------------
    # move() — transref failure → rollback_chunks
    # ------------------------------------------------------------------

    async def test_move_transref_failure_rolls_back(self, index_storage: IndexStorage, mocker: MockerFixture):
        """When _chunk_transref raises during move, rollback reverse-transrefs."""
        s = index_storage
        data = b"M" * BLOCK_SIZE
        src = f"test-rollb-mvtrans-src-{uid()}"
        dst = f"test-rollb-mvtrans-dst-{uid()}"

        await s.upload_bytes(data, src)
        src_meta = await s._get_file_meta(src)
        assert src_meta is not None
        chunk_hashes = src_meta.chunks[:]
        refs_before = {}
        for h in chunk_hashes:
            r = await s._chunk_load_refs(h)
            refs_before[h] = r or set()

        # Fail _chunk_transref for all chunks
        mocker.patch.object(s, "_chunk_transref", side_effect=RuntimeError("simulated transref failure"))

        with pytest.raises(BaseException) as _exc:  # noqa: PT011
            await s.move(src, dst)

        # Destination file must not exist
        assert await s._get_file_meta(dst) is None
        # Source file is intact
        assert await s.download_bytes(src) == data
        # Chunk refs must still point to src only (unchanged, rollback was a no-op
        # since transref never succeeded)
        for chunk_hash in chunk_hashes:
            refs = await s._chunk_load_refs(chunk_hash)
            assert refs is not None
            assert f"/{src}" in refs, f"src must remain in chunk {chunk_hash[:8]} refs"
            assert f"/{dst}" not in refs, f"dst must not be in chunk {chunk_hash[:8]} refs"

        await s.delete(src)

    # ------------------------------------------------------------------
    # move() — meta write failure → rollback_chunks (reverse transref)
    # ------------------------------------------------------------------

    async def test_move_meta_failure_rolls_back(self, index_storage: IndexStorage, mocker: MockerFixture):
        """When meta write fails after transref, chunks are reverse-transref'd back to src."""
        s = index_storage
        data = b"N" * BLOCK_SIZE
        src = f"test-rollb-mvmeta-src-{uid()}"
        dst = f"test-rollb-mvmeta-dst-{uid()}"

        await s.upload_bytes(data, src)
        src_meta = await s._get_file_meta(src)
        assert src_meta is not None
        chunk_hashes = src_meta.chunks[:]

        original_upload = s._index.upload_bytes

        async def _fail_meta(data_bytes: bytes, remote_path: str, *, overwrite: bool = True) -> None:
            if str(remote_path) == f"/{dst}":
                raise OSError("simulated meta write failure")
            await original_upload(data_bytes, remote_path, overwrite=overwrite)

        mocker.patch.object(s._index, "upload_bytes", _fail_meta)

        with pytest.raises(OSError, match="simulated meta write failure"):
            await s.move(src, dst)

        # Destination file must not exist
        assert await s._get_file_meta(dst) is None
        # Source file is intact
        assert await s.download_bytes(src) == data
        # Chunk refs must point back to src (rollback reversed the transref)
        for chunk_hash in chunk_hashes:
            refs = await s._chunk_load_refs(chunk_hash)
            assert refs is not None
            assert f"/{src}" in refs, f"src must be restored to chunk {chunk_hash[:8]} refs"
            assert f"/{dst}" not in refs, f"dst must not be in chunk {chunk_hash[:8]} refs"

        await s.delete(src)

    # ------------------------------------------------------------------
    # copytree — overwrite cleans up old chunks
    # ------------------------------------------------------------------

    async def test_copytree_overwrite_cleans_old_chunks(self, index_storage: IndexStorage):
        """copytree with overwrite decrefs old chunks in the destination."""
        s = index_storage
        old_data = b"OLD" * 4096  # 12 KB fits in one block
        new_data = b"NEW!" * 4096  # different content → different chunk hash
        src = f"test-rollb-cptree-src-{uid()}"
        dst = f"test-rollb-cptree-dst-{uid()}"

        try:
            # Pre-populate dst with old file
            await s.mkdir(dst, parents=True)
            await s.upload_bytes(old_data, f"{dst}/f.txt")
            old_meta = await s._get_file_meta(f"{dst}/f.txt")
            assert old_meta is not None
            old_chunks = set(old_meta.chunks)

            # Create src with new file (different content)
            await s.mkdir(src, parents=True)
            await s.upload_bytes(new_data, f"{src}/f.txt")
            new_meta = await s._get_file_meta(f"{src}/f.txt")
            assert new_meta is not None
            new_chunks = set(new_meta.chunks)

            # Old and new chunks must differ
            assert old_chunks != new_chunks, "Test data must produce different chunk hashes"

            await s.copytree(src, dst, overwrite=True)

            # Destination has new content
            assert await s.download_bytes(f"{dst}/f.txt") == new_data

            # Old chunks must be cleaned up (ref=0 → deleted)
            for h in old_chunks:
                assert not await s._chunks.exists(f"{hash_to_path_stem(h)}.bin"), (
                    f"Old chunk {h[:8]} must be cleaned up"
                )

            # New chunks must exist with refs for both src and dst
            for h in new_chunks:
                refs = await s._chunk_load_refs(h)
                assert refs is not None
                assert f"/{src}/f.txt" in refs
                assert f"/{dst}/f.txt" in refs
        finally:
            with suppress_exc():
                await s.rmtree(src)
            with suppress_exc():
                await s.rmtree(dst)

    # ------------------------------------------------------------------
    # movetree — overwrite cleans up old chunks
    # ------------------------------------------------------------------

    async def test_movetree_overwrite_cleans_old_chunks(self, index_storage: IndexStorage):
        """movetree with overwrite decrefs old chunks and transfers refs."""
        s = index_storage
        old_data = b"OLD!" * 4096  # 16 KB
        new_data = b"NEW!" * 4096  # 16 KB — different content
        src = f"test-rollb-mvtree-src-{uid()}"
        dst = f"test-rollb-mvtree-dst-{uid()}"

        try:
            # Pre-populate dst with old file
            await s.mkdir(dst, parents=True)
            await s.upload_bytes(old_data, f"{dst}/f.txt")
            old_meta = await s._get_file_meta(f"{dst}/f.txt")
            assert old_meta is not None
            old_chunks = set(old_meta.chunks)

            # Create src with new file (different content)
            await s.mkdir(src, parents=True)
            await s.upload_bytes(new_data, f"{src}/f.txt")
            new_meta = await s._get_file_meta(f"{src}/f.txt")
            assert new_meta is not None
            new_chunks = set(new_meta.chunks)

            assert old_chunks != new_chunks, "Test data must produce different chunk hashes"

            await s.movetree(src, dst, overwrite=True)

            # Source is gone
            assert not await s.exists(src)

            # Destination has new content
            assert await s.download_bytes(f"{dst}/f.txt") == new_data

            # Old chunks must be cleaned up
            for h in old_chunks:
                assert not await s._chunks.exists(f"{hash_to_path_stem(h)}.bin"), (
                    f"Old chunk {h[:8]} must be cleaned up"
                )

            # New chunks only ref dst (src is gone)
            for h in new_chunks:
                refs = await s._chunk_load_refs(h)
                assert refs is not None
                assert f"/{src}/f.txt" not in refs, f"src ref must be removed from chunk {h[:8]}"
                assert f"/{dst}/f.txt" in refs, f"dst ref must exist in chunk {h[:8]}"
        finally:
            with suppress_exc():
                await s.rmtree(src)
            with suppress_exc():
                await s.rmtree(dst)
