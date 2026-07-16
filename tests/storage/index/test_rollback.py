"""IndexStorage behavior tests."""

import pytest
from pytest_mock import MockerFixture

from app.storage.index import IndexStorage
from tests.storage.index.helpers import BLOCK_SIZE, hash_to_path_stem, suppress_exc
from tests.support.ids import uid


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
