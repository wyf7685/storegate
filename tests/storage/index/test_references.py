"""IndexStorage behavior tests."""

from storegate.storage.index import IndexStorage
from tests.storage.index.helpers import BLOCK_SIZE, _hash_block, hash_to_path_stem, suppress_exc
from tests.support.ids import uid


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
                refs = await index_storage._refs.load_refs(chunk_hash)
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
                r = await index_storage._refs.load_refs(h)
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
                refs = await index_storage._refs.load_refs(chunk_hash)
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
                r = await index_storage._refs.load_refs(h)
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
                refs = await index_storage._refs.load_refs(chunk_hash)
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
