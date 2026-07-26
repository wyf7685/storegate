"""IndexStorage behavior tests."""

import contextlib
import hashlib

import anyio
import pytest
from pytest_mock import MockerFixture

from storegate.storage.index import IndexStorage
from storegate.utils import flatten_exception_group
from tests.storage.index.helpers import BLOCK_SIZE, hash_to_path_stem, suppress_exc
from tests.support.ids import uid


def _hash_block(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def _chunk_bin_exists(storage: IndexStorage, chunk_hash: str) -> bool:
    return await storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.bin")


async def _chunk_ref_exists(storage: IndexStorage, chunk_hash: str) -> bool:
    return await storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.ref")


async def _assert_no_orphan_chunks(storage: IndexStorage, *chunk_hashes: str) -> None:
    for chunk_hash in chunk_hashes:
        assert not await _chunk_bin_exists(storage, chunk_hash), f"orphan bin for {chunk_hash[:8]}"
        assert not await _chunk_ref_exists(storage, chunk_hash), f"orphan ref for {chunk_hash[:8]}"


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
        await _assert_no_orphan_chunks(s, _hash_block(b"R" * BLOCK_SIZE))

    @pytest.mark.parametrize(
        "scenario",
        ["empty", "disjoint", "partial_shared", "identical"],
    )
    async def test_upload_overwrite_meta_failure_preserves_old_file(
        self,
        index_storage: IndexStorage,
        mocker: MockerFixture,
        scenario: str,
    ):
        payloads = {
            "empty": (b"", b"X" * BLOCK_SIZE),
            "disjoint": (b"A" * BLOCK_SIZE + b"B" * BLOCK_SIZE, b"C" * BLOCK_SIZE + b"D" * BLOCK_SIZE),
            "partial_shared": (b"A" * BLOCK_SIZE + b"B" * BLOCK_SIZE, b"A" * BLOCK_SIZE + b"C" * BLOCK_SIZE),
            "identical": (b"S" * BLOCK_SIZE + b"T" * BLOCK_SIZE, b"S" * BLOCK_SIZE + b"T" * BLOCK_SIZE),
        }
        old_data, new_data = payloads[scenario]
        s = index_storage
        path = f"test-upload-tx-{scenario}-{uid()}"
        await s.upload_bytes(old_data, path)
        old_meta = await s._get_file_meta(path)
        assert old_meta is not None
        old_hashes = list(old_meta.chunks)

        original_upload = s._index.upload_bytes

        async def _fail_meta(data_bytes: bytes, remote_path: str, *, overwrite: bool = True) -> None:
            if str(remote_path) == f"/{path}":
                raise OSError(f"simulated meta write failure ({scenario})")
            await original_upload(data_bytes, remote_path, overwrite=overwrite)

        mocker.patch.object(s._index, "upload_bytes", _fail_meta)

        with pytest.raises(OSError, match="simulated meta write failure"):
            await s.upload_bytes(new_data, path)

        assert await s.download_bytes(path) == old_data
        restored = await s._get_file_meta(path)
        assert restored is not None
        assert restored.chunks == old_hashes

        for chunk_hash in old_hashes:
            assert await _chunk_bin_exists(s, chunk_hash)
            refs = await s._refs.load_refs(chunk_hash)
            assert refs is not None
            assert f"/{path}" in refs
            assert not any(ref.startswith("$rollback-") for ref in refs)

        # New-only chunks must not remain as orphans after rollback.
        new_hashes = (
            [_hash_block(new_data[i : i + BLOCK_SIZE]) for i in range(0, len(new_data), BLOCK_SIZE)] if new_data else []
        )
        for chunk_hash in set(new_hashes) - set(old_hashes):
            await _assert_no_orphan_chunks(s, chunk_hash)

        await s.delete(path)

    async def test_upload_rollback_failure_is_primary_first(self, index_storage: IndexStorage, mocker: MockerFixture):
        s = index_storage
        data = b"U" * BLOCK_SIZE
        path = f"test-upload-primary-first-{uid()}"
        original_upload = s._index.upload_bytes
        original_decref = s._refs.decref

        async def _fail_meta(data_bytes: bytes, remote_path: str, *, overwrite: bool = True) -> None:
            if str(remote_path) == f"/{path}":
                raise OSError("primary meta failure")
            await original_upload(data_bytes, remote_path, overwrite=overwrite)

        async def _fail_decref(chunk_hash: str, *remote_path: str) -> None:
            if any(str(p) == f"/{path}" for p in remote_path):
                raise RuntimeError("cleanup decref failure")
            await original_decref(chunk_hash, *remote_path)

        mocker.patch.object(s._index, "upload_bytes", _fail_meta)
        mocker.patch.object(s._refs, "decref", _fail_decref)

        with pytest.raises(BaseExceptionGroup) as caught:
            await s.upload_bytes(data, path)

        flattened = list(flatten_exception_group(caught.value))
        assert len(flattened) >= 2
        assert isinstance(flattened[0], OSError)
        assert "primary meta failure" in str(flattened[0])
        assert any(isinstance(exc, RuntimeError) and "cleanup decref failure" in str(exc) for exc in flattened[1:])

    async def test_chunk_write_error_after_commit_removes_staged_bin(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ) -> None:
        s = index_storage
        data = b"W" * BLOCK_SIZE
        path = f"test-chunk-commit-error-{uid()}"
        chunk_hash = _hash_block(data)
        original_upload = s._chunks.upload_bytes

        async def _commit_then_fail(data_bytes: bytes, remote_path: str, *, overwrite: bool = True) -> None:
            await original_upload(data_bytes, remote_path, overwrite=overwrite)
            if str(remote_path).endswith(".bin"):
                raise OSError("chunk write committed before error")

        mocker.patch.object(s._chunks, "upload_bytes", _commit_then_fail)

        with pytest.raises(OSError, match="chunk write committed before error"):
            await s.upload_bytes(data, path)

        await _assert_no_orphan_chunks(s, chunk_hash)
        assert await s._get_file_meta(path) is None

    async def test_meta_write_error_after_commit_keeps_new_file_and_cleans_old_refs(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ) -> None:
        s = index_storage
        path = f"test-meta-commit-error-{uid()}"
        old_data = b"A" * BLOCK_SIZE + b"B" * BLOCK_SIZE
        new_data = b"C" * BLOCK_SIZE + b"D" * BLOCK_SIZE
        await s.upload_bytes(old_data, path)
        old_meta = await s._get_file_meta(path)
        assert old_meta is not None
        old_hashes = set(old_meta.chunks)
        original_upload = s._index.upload_bytes

        async def _commit_then_fail(data_bytes: bytes, remote_path: str, *, overwrite: bool = True) -> None:
            await original_upload(data_bytes, remote_path, overwrite=overwrite)
            if str(remote_path) == f"/{path}":
                raise OSError("metadata write committed before error")

        mocker.patch.object(s._index, "upload_bytes", _commit_then_fail)

        with pytest.raises(OSError, match="metadata write committed before error"):
            await s.upload_bytes(new_data, path)

        assert await s.download_bytes(path) == new_data
        new_meta = await s._get_file_meta(path)
        assert new_meta is not None
        new_hashes = set(new_meta.chunks)
        for chunk_hash in old_hashes - new_hashes:
            await _assert_no_orphan_chunks(s, chunk_hash)
        for chunk_hash in new_hashes:
            assert await s._refs.load_refs(chunk_hash) == {f"/{path}"}
        await s.delete(path)

    @pytest.mark.parametrize("guard_release_fails", [False, True])
    async def test_post_commit_decref_failure_always_releases_guards(
        self,
        index_storage: IndexStorage,
        mocker: MockerFixture,
        *,
        guard_release_fails: bool,
    ) -> None:
        s = index_storage
        path = f"test-post-commit-cleanup-{uid()}"
        old_data = b"O" * BLOCK_SIZE
        new_data = b"N" * BLOCK_SIZE
        old_hash = _hash_block(old_data)
        new_hash = _hash_block(new_data)
        await s.upload_bytes(old_data, path)
        original_decref = s._refs.decref
        original_release = s._refs.release_rollback_guards

        async def _decref_then_fail(chunk_hash: str, *remote_path: str) -> None:
            await original_decref(chunk_hash, *remote_path)
            if chunk_hash == old_hash and any(str(value) == f"/{path}" for value in remote_path):
                raise OSError("old-only decref committed before error")

        async def _release_then_fail(guards: dict[str, str]) -> None:
            await original_release(guards)
            if guard_release_fails:
                raise RuntimeError("guard release committed before error")

        mocker.patch.object(s._refs, "decref", _decref_then_fail)
        mocker.patch.object(s._refs, "release_rollback_guards", _release_then_fail)

        with pytest.raises((OSError, BaseExceptionGroup)) as caught:
            await s.upload_bytes(new_data, path)

        flattened = (
            list(flatten_exception_group(caught.value))
            if isinstance(caught.value, BaseExceptionGroup)
            else [caught.value]
        )
        assert "old-only decref committed before error" in str(flattened[0])
        if guard_release_fails:
            assert "guard release committed before error" in str(flattened[-1])
        assert await s.download_bytes(path) == new_data
        await _assert_no_orphan_chunks(s, old_hash)
        assert await s._refs.load_refs(new_hash) == {f"/{path}"}
        await s.delete(path)

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
        original_incref = s._refs.incref

        async def _failing_incref(chunk_hash: str, *remote_path: str) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise RuntimeError("simulated incref failure")
            await original_incref(chunk_hash, *remote_path)

        mocker.patch.object(s._refs, "incref", _failing_incref)

        with pytest.raises(BaseException) as _exc:  # noqa: PT011
            await s.copy(src, dst)

        # Destination file must not exist
        assert await s._get_file_meta(dst) is None
        # Source file is intact
        assert await s.exists(src)
        assert await s.download_bytes(src) == data
        # All chunk refs must NOT contain dst (rolled back)
        for chunk_hash in chunk_hashes:
            refs = await s._refs.load_refs(chunk_hash)
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
            refs = await s._refs.load_refs(chunk_hash)
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
            r = await s._refs.load_refs(h)
            refs_before[h] = r or set()

        # Fail _chunk_transref for all chunks
        mocker.patch.object(s._refs, "transref", side_effect=RuntimeError("simulated transref failure"))

        with pytest.raises(BaseException) as _exc:  # noqa: PT011
            await s.move(src, dst)

        # Destination file must not exist
        assert await s._get_file_meta(dst) is None
        # Source file is intact
        assert await s.download_bytes(src) == data
        # Chunk refs must still point to src only (unchanged, rollback was a no-op
        # since transref never succeeded)
        for chunk_hash in chunk_hashes:
            refs = await s._refs.load_refs(chunk_hash)
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
            refs = await s._refs.load_refs(chunk_hash)
            assert refs is not None
            assert f"/{src}" in refs, f"src must be restored to chunk {chunk_hash[:8]} refs"
            assert f"/{dst}" not in refs, f"dst must not be in chunk {chunk_hash[:8]} refs"

        await s.delete(src)

    # ------------------------------------------------------------------
    @pytest.mark.parametrize("operation", ["copy", "move"])
    async def test_overwrite_decref_failure_preserves_old_chunks(
        self, index_storage: IndexStorage, mocker: MockerFixture, operation: str
    ):
        s = index_storage
        old_data = b"O" * BLOCK_SIZE + b"P" * BLOCK_SIZE
        new_data = b"N" * BLOCK_SIZE + b"Q" * BLOCK_SIZE
        src = f"test-rollb-overwrite-src-{uid()}"
        dst = f"test-rollb-overwrite-dst-{uid()}"
        await s.upload_bytes(old_data, dst)
        await s.upload_bytes(new_data, src)
        old_meta = await s._get_file_meta(dst)
        assert old_meta is not None
        assert len(old_meta.chunks) >= 2
        old_chunks = set(old_meta.chunks)
        original_decref = s._refs.decref
        old_decrefs = 0

        async def fail_second_old_decref(chunk_hash: str, *remote_path: str) -> None:
            nonlocal old_decrefs
            if chunk_hash in old_chunks and any(str(path) == f"/{dst}" for path in remote_path):
                old_decrefs += 1
                if old_decrefs == 2:
                    raise RuntimeError("simulated second old-chunk decref failure")
            await original_decref(chunk_hash, *remote_path)

        mocker.patch.object(s._refs, "decref", fail_second_old_decref)
        with pytest.raises(RuntimeError, match="second old-chunk decref"):
            await getattr(s, operation)(src, dst, overwrite=True)

        assert await s.download_bytes(dst) == old_data
        assert await s.download_bytes(src) == new_data
        for chunk_hash in old_chunks:
            assert await s._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.bin")

        await s.delete(src)
        await s.delete(dst)

    async def test_move_src_unlink_failure_rolls_back_instead_of_stranding_src(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ):
        """Removing the source is the commit point, so its failure must roll the move back.

        Regression: the src unlink used to sit outside every rollback arm. A failure there
        left src readable while all of its chunks had been transref'd to dst, so a later
        unlink(dst) decref'd them to zero and destroyed data src still pointed at.
        """
        s = index_storage
        data = b"U" * BLOCK_SIZE
        src = f"test-rollb-mvsrc-src-{uid()}"
        dst = f"test-rollb-mvsrc-dst-{uid()}"

        await s.upload_bytes(data, src)
        src_meta = await s._get_file_meta(src)
        assert src_meta is not None
        chunk_hashes = src_meta.chunks[:]

        original_unlink = s._index.unlink

        async def _fail_src_unlink(remote_path: str, *, missing_ok: bool = False) -> None:
            if str(remote_path) == f"/{src}":
                raise OSError("simulated src unlink failure")
            await original_unlink(remote_path, missing_ok=missing_ok)

        mocker.patch.object(s._index, "unlink", _fail_src_unlink)

        with pytest.raises(OSError, match="simulated src unlink failure"):
            await s.move(src, dst)

        mocker.stopall()

        # The move is fully undone: src is readable and dst never materialised.
        assert await s.download_bytes(src) == data
        assert await s._get_file_meta(dst) is None
        # Chunks belong to src alone, so unlinking dst cannot reap them.
        for chunk_hash in chunk_hashes:
            refs = await s._refs.load_refs(chunk_hash)
            assert refs is not None
            assert refs == {f"/{src}"}

        await s.delete(src)

    async def test_overwrite_cancellation_releases_rollback_guards(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ):
        s = index_storage
        old_data = b"O" * BLOCK_SIZE + b"P" * BLOCK_SIZE
        new_data = b"N" * BLOCK_SIZE + b"Q" * BLOCK_SIZE
        src = f"test-rollb-cancel-src-{uid()}"
        dst = f"test-rollb-cancel-dst-{uid()}"
        await s.upload_bytes(old_data, dst)
        await s.upload_bytes(new_data, src)
        old_meta = await s._get_file_meta(dst)
        assert old_meta is not None
        old_chunks = set(old_meta.chunks)
        original_decref = s._refs.decref
        old_decrefs = 0
        cancel_scope: anyio.CancelScope | None = None

        async def cancel_on_old_decref(chunk_hash: str, *remote_path: str) -> None:
            nonlocal old_decrefs
            if chunk_hash in old_chunks and any(str(path) == f"/{dst}" for path in remote_path):
                old_decrefs += 1
                if old_decrefs == 1:
                    assert cancel_scope is not None
                    cancel_scope.cancel()
                    raise RuntimeError("simulated cancellation during decref")
            await original_decref(chunk_hash, *remote_path)

        mocker.patch.object(s._refs, "decref", cancel_on_old_decref)
        with anyio.CancelScope() as active_scope:
            cancel_scope = active_scope
            with pytest.raises(RuntimeError, match="simulated cancellation"):
                await s.copy(src, dst, overwrite=True)

        assert await s.download_bytes(dst) == old_data
        refs = [await s._refs.load_refs(chunk_hash) for chunk_hash in old_chunks]
        for refs_for_chunk in refs:
            assert refs_for_chunk is not None
            assert not any(ref.startswith("$rollback-") for ref in refs_for_chunk)
        await s.delete(src)
        await s.delete(dst)

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
                refs = await s._refs.load_refs(h)
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
                refs = await s._refs.load_refs(h)
                assert refs is not None
                assert f"/{src}/f.txt" not in refs, f"src ref must be removed from chunk {h[:8]}"
                assert f"/{dst}/f.txt" in refs, f"dst ref must exist in chunk {h[:8]}"
        finally:
            with suppress_exc():
                await s.rmtree(src)
            with suppress_exc():
                await s.rmtree(dst)


class TestTreeTransactionRollback:
    async def test_copytree_failure_restores_overwritten_files_and_refs(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ) -> None:
        storage = index_storage
        src = f"test-tree-copy-src-{uid()}"
        dst = f"test-tree-copy-dst-{uid()}"
        try:
            await storage.upload_bytes(b"new-one", f"{src}/one.txt")
            await storage.upload_bytes(b"new-two", f"{src}/two.txt")
            await storage.upload_bytes(b"old-one", f"{dst}/one.txt")
            await storage.upload_bytes(b"old-two", f"{dst}/two.txt")
            old_one = await storage._get_file_meta(f"{dst}/one.txt")
            old_two = await storage._get_file_meta(f"{dst}/two.txt")
            assert old_one is not None
            assert old_two is not None

            original_copy = storage.copy

            async def fail_second(source: str, destination: str, *, overwrite: bool = True) -> None:
                if str(destination).endswith("/two.txt"):
                    raise OSError("injected tree copy failure")
                await original_copy(source, destination, overwrite=overwrite)

            mocker.patch.object(storage, "copy", fail_second)
            with pytest.raises(OSError, match="injected tree copy failure"):
                await storage.copytree(src, dst, overwrite=True)

            assert await storage.download_bytes(f"{dst}/one.txt") == b"old-one"
            assert await storage.download_bytes(f"{dst}/two.txt") == b"old-two"
            for meta, path in ((old_one, f"/{dst}/one.txt"), (old_two, f"/{dst}/two.txt")):
                for chunk_hash in meta.chunks:
                    refs = await storage._refs.load_refs(chunk_hash)
                    assert refs is not None
                    assert path in refs
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(src)
            with contextlib.suppress(Exception):
                await storage.rmtree(dst)

    async def test_movetree_failure_restores_source_and_destination(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ) -> None:
        storage = index_storage
        src = f"test-tree-move-src-{uid()}"
        dst = f"test-tree-move-dst-{uid()}"
        try:
            await storage.upload_bytes(b"new-one", f"{src}/one.txt")
            await storage.upload_bytes(b"new-two", f"{src}/two.txt")
            await storage.upload_bytes(b"old-one", f"{dst}/one.txt")
            await storage.upload_bytes(b"old-two", f"{dst}/two.txt")

            original_move = storage.move

            async def fail_second(source: str, destination: str, *, overwrite: bool = True) -> None:
                if str(destination).endswith("/two.txt"):
                    raise OSError("injected tree move failure")
                await original_move(source, destination, overwrite=overwrite)

            mocker.patch.object(storage, "move", fail_second)
            with pytest.raises(OSError, match="injected tree move failure"):
                await storage.movetree(src, dst, overwrite=True)

            assert await storage.download_bytes(f"{src}/one.txt") == b"new-one"
            assert await storage.download_bytes(f"{src}/two.txt") == b"new-two"
            assert await storage.download_bytes(f"{dst}/one.txt") == b"old-one"
            assert await storage.download_bytes(f"{dst}/two.txt") == b"old-two"
        finally:
            with contextlib.suppress(Exception):
                await storage.rmtree(src)
            with contextlib.suppress(Exception):
                await storage.rmtree(dst)
