"""Real AnyIO cancellation coverage for IndexStorage upload transactions."""

from collections.abc import AsyncIterator

import anyio
import anyio.lowlevel
import pytest
from pytest_mock import MockerFixture

from storegate.storage.index import IndexStorage
from tests.storage.index.helpers import BLOCK_SIZE, hash_to_path_stem
from tests.support.ids import uid


def _hash_block(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


async def _bin_exists(storage: IndexStorage, chunk_hash: str) -> bool:
    return await storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.bin")


async def _ref_exists(storage: IndexStorage, chunk_hash: str) -> bool:
    return await storage._chunks.exists(f"{hash_to_path_stem(chunk_hash)}.ref")


async def _assert_clean_path(storage: IndexStorage, path: str, *chunk_hashes: str) -> None:
    assert await storage._get_file_meta(path) is None
    for chunk_hash in chunk_hashes:
        assert not await _bin_exists(storage, chunk_hash), f"orphan bin for {chunk_hash[:8]}"
        assert not await _ref_exists(storage, chunk_hash), f"orphan ref for {chunk_hash[:8]}"


async def _cancel_and_checkpoint(cancel_scope: anyio.CancelScope) -> None:
    cancel_scope.cancel()
    # Real cancellation is delivered at the next awaitable boundary.
    await anyio.lowlevel.checkpoint()


class TestUploadCancellation:
    async def test_cancel_during_input_stream_leaves_no_orphans(self, index_storage: IndexStorage) -> None:
        s = index_storage
        path = f"cancel-input-{uid()}"
        block_x = b"X" * BLOCK_SIZE
        block_y = b"Y" * BLOCK_SIZE
        hash_x = _hash_block(block_x)
        hash_y = _hash_block(block_y)
        cancel_scope: anyio.CancelScope | None = None

        async def stream() -> AsyncIterator[bytes]:
            yield block_x[: BLOCK_SIZE // 2]
            assert cancel_scope is not None
            await _cancel_and_checkpoint(cancel_scope)
            yield block_x[BLOCK_SIZE // 2 :] + block_y

        with anyio.CancelScope() as scope:
            cancel_scope = scope
            with pytest.raises(anyio.get_cancelled_exc_class()):
                await s.upload_stream(stream(), path)

        await _assert_clean_path(s, path, hash_x, hash_y)

    async def test_cancel_during_chunk_upload_leaves_no_orphans(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ) -> None:
        s = index_storage
        path = f"cancel-chunk-{uid()}"
        block_x = b"X" * BLOCK_SIZE
        block_y = b"Y" * BLOCK_SIZE
        hash_x = _hash_block(block_x)
        hash_y = _hash_block(block_y)
        original_upload = s._chunks.upload_bytes
        cancel_scope: anyio.CancelScope | None = None

        async def cancel_on_bin(data: bytes, remote_path: str, *, overwrite: bool = True) -> None:
            await original_upload(data, remote_path, overwrite=overwrite)
            if str(remote_path).endswith(".bin"):
                assert cancel_scope is not None
                await _cancel_and_checkpoint(cancel_scope)

        mocker.patch.object(s._chunks, "upload_bytes", cancel_on_bin)

        with anyio.CancelScope() as scope:
            cancel_scope = scope
            with pytest.raises(anyio.get_cancelled_exc_class()):
                await s.upload_bytes(block_x + block_y, path)

        await _assert_clean_path(s, path, hash_x, hash_y)

    async def test_cancel_during_incref_leaves_no_orphans(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ) -> None:
        s = index_storage
        path = f"cancel-ref-{uid()}"
        block_x = b"X" * BLOCK_SIZE
        block_y = b"Y" * BLOCK_SIZE
        hash_x = _hash_block(block_x)
        hash_y = _hash_block(block_y)
        original_incref = s._refs.incref
        cancel_scope: anyio.CancelScope | None = None
        calls = 0

        async def cancel_on_incref(chunk_hash: str, *remote_path: str) -> bool:
            nonlocal calls
            calls += 1
            if calls == 1:
                assert cancel_scope is not None
                await _cancel_and_checkpoint(cancel_scope)
            return await original_incref(chunk_hash, *remote_path)

        mocker.patch.object(s._refs, "incref", cancel_on_incref)

        with anyio.CancelScope() as scope:
            cancel_scope = scope
            with pytest.raises(anyio.get_cancelled_exc_class()):
                await s.upload_bytes(block_x + block_y, path)

        await _assert_clean_path(s, path, hash_x, hash_y)

    async def test_cancel_after_meta_commit_keeps_new_file_and_cleans_old_refs(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ) -> None:
        s = index_storage
        path = f"cancel-meta-{uid()}"
        old_data = b"A" * BLOCK_SIZE + b"B" * BLOCK_SIZE
        new_data = b"X" * BLOCK_SIZE + b"Y" * BLOCK_SIZE
        await s.upload_bytes(old_data, path)
        old_meta = await s._get_file_meta(path)
        assert old_meta is not None
        old_hashes = set(old_meta.chunks)
        original_upload = s._index.upload_bytes
        cancel_scope: anyio.CancelScope | None = None

        async def cancel_after_meta(data: bytes, remote_path: str, *, overwrite: bool = True) -> None:
            await original_upload(data, remote_path, overwrite=overwrite)
            if str(remote_path) == f"/{path}":
                assert cancel_scope is not None
                await _cancel_and_checkpoint(cancel_scope)

        mocker.patch.object(s._index, "upload_bytes", cancel_after_meta)

        with anyio.CancelScope() as scope:
            cancel_scope = scope
            with pytest.raises(anyio.get_cancelled_exc_class()):
                await s.upload_bytes(new_data, path)

        assert await s.download_bytes(path) == new_data
        new_meta = await s._get_file_meta(path)
        assert new_meta is not None
        new_hashes = set(new_meta.chunks)
        for chunk_hash in old_hashes - new_hashes:
            assert not await _bin_exists(s, chunk_hash)
            assert not await _ref_exists(s, chunk_hash)
        for chunk_hash in new_hashes:
            refs = await s._refs.load_refs(chunk_hash)
            assert refs == {f"/{path}"}
        await s.delete(path)

    async def test_instance_reusable_after_cancellation(
        self, index_storage: IndexStorage, mocker: MockerFixture
    ) -> None:
        s = index_storage
        fail_path = f"cancel-reuse-fail-{uid()}"
        ok_path = f"cancel-reuse-ok-{uid()}"
        block = b"Z" * BLOCK_SIZE
        original_upload = s._index.upload_bytes
        cancel_scope: anyio.CancelScope | None = None

        async def cancel_on_meta(data: bytes, remote_path: str, *, overwrite: bool = True) -> None:
            await original_upload(data, remote_path, overwrite=overwrite)
            if str(remote_path) == f"/{fail_path}":
                assert cancel_scope is not None
                await _cancel_and_checkpoint(cancel_scope)

        mocker.patch.object(s._index, "upload_bytes", cancel_on_meta)

        with anyio.CancelScope() as scope:
            cancel_scope = scope
            with pytest.raises(anyio.get_cancelled_exc_class()):
                await s.upload_bytes(block, fail_path)

        mocker.stopall()
        assert await s.download_bytes(fail_path) == block
        await s.upload_bytes(block, ok_path)
        assert await s.download_bytes(ok_path) == block
        await s.delete(fail_path)
        await s.delete(ok_path)
