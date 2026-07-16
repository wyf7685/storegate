"""CachedStorage behavior tests."""

import contextlib

import pytest

from app.storage.cached import CachedStorage
from tests.support.ids import uid


class TestCacheInvalidation:
    """Verify write operations backfill ``False`` into relevant caches."""

    async def test_unlink_backfills_false(self, cached: CachedStorage):
        path = f"test-inv-unlink-{uid()}"
        await cached.upload_bytes(b"hello", path)

        snap = cached.dump_cache()
        assert snap.get("exists", {}).get(path) is True

        await cached.unlink(path)

        snap = cached.dump_cache()
        assert snap.get("exists", {}).get(path) is False, "exists should be backfilled False after unlink"
        assert snap.get("is_file", {}).get(path) is False, "is_file should be backfilled False after unlink"
        assert snap.get("is_dir", {}).get(path) is False, "is_dir should be backfilled False after unlink"

    async def test_rmdir_backfills_false(self, cached: CachedStorage):
        path = f"test-inv-rmdir-{uid()}"
        await cached.mkdir(path)

        await cached.rmdir(path)

        snap = cached.dump_cache()
        assert snap.get("exists", {}).get(path) is False, "exists should be backfilled False after rmdir"
        assert snap.get("is_dir", {}).get(path) is False, "is_dir should be backfilled False after rmdir"

    async def test_delete_many_invalidates_after_partial_failure(self, cached: CachedStorage):
        root = f"test-inv-delete-many-{uid()}"
        deleted = f"{root}/deleted.txt"
        nonempty = f"{root}/nonempty"
        child = f"{nonempty}/child.txt"
        try:
            await cached.mkdir(root)
            await cached.mkdir(nonempty)
            await cached.upload_bytes(b"deleted", deleted)
            await cached.upload_bytes(b"child", child)

            await cached.stat(deleted)
            await cached.stat(nonempty)
            await cached.download_bytes(deleted)
            entries = [entry async for entry in cached.iterdir(root)]
            assert len(entries) == 2

            with pytest.raises(OSError, match="Directory not empty"):
                await cached.delete_many(deleted, nonempty)

            snap = cached.dump_cache()
            for namespace in ("exists", "is_file", "is_dir", "stat", "download"):
                assert deleted not in snap.get(namespace, {})
            for namespace in ("exists", "is_file", "is_dir", "stat"):
                assert nonempty not in snap.get(namespace, {})
            assert root not in snap.get("iterdir", {})
            assert not await cached.exists(deleted)
            assert await cached.exists(nonempty)
        finally:
            await cached.rmtree(root)

    async def test_move_backfills_src_false_dst_true(self, cached: CachedStorage):
        src = f"test-inv-move-src-{uid()}"
        dst = f"test-inv-move-dst-{uid()}"
        try:
            await cached.upload_bytes(b"hello", src)

            await cached.move(src, dst)

            snap = cached.dump_cache()
            assert snap.get("exists", {}).get(src) is False, "src exists should be False after move"
            assert snap.get("exists", {}).get(dst) is True, "dst exists should be True after move"
        finally:
            await cached.delete(dst)
            with contextlib.suppress(Exception):
                await cached.delete(src)
