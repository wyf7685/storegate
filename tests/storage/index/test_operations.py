"""IndexStorage behavior tests."""

import contextlib

from storegate.storage.index import IndexStorage
from storegate.storage.memory import MemoryStorage
from tests.storage.index.helpers import BLOCK_SIZE, hash_to_path_stem, suppress_exc
from tests.support.ids import uid


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
                refs = await index_storage._refs.load_refs(chunk_hash)
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


class TestDisabledLocking:
    """IndexStorage with lock_mode='disabled'."""

    async def test_upload_without_locking(self):
        async with (
            MemoryStorage("/") as idx,
            MemoryStorage("/") as chunks,
            IndexStorage(idx, chunks, block_size=BLOCK_SIZE, lock_mode="disabled") as s,
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
