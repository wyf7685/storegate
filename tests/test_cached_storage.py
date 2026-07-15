"""Tests for CachedStorage cache behavior — with verified cache state.

Every test uses :meth:`CachedStorage.dump_cache` to *prove* whether a
result came from cache or from the underlying storage, rather than just
assuming the cache behaved as documented.
"""

import contextlib

import pytest

from app.storage.cached import CachedStorage
from app.storage.memory import MemoryStorage
from tests.conftest import uid


@pytest.fixture
async def cached():
    """A CachedStorage wrapping MemoryStorage for fast, deterministic tests."""
    async with MemoryStorage("/") as inner, CachedStorage(inner) as s:
        yield s


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


async def _clear_path(cached: CachedStorage, path: str) -> None:
    """Remove all cache entries for *path* (simulates a completely cold cache)."""
    await cached._cache.mdelete(
        ("exists", path),
        ("is_file", path),
        ("is_dir", path),
        ("stat", path),
        ("download", path),
        ("iterdir", path),
    )


# ---------------------------------------------------------------------------
# TestCacheHit
# ---------------------------------------------------------------------------


class TestCacheHit:
    """Verify that each read operation's result is actually stored in cache."""

    async def test_exists_backfilled_after_upload(self, cached: CachedStorage):
        path = f"test-cexists-{uid()}"
        try:
            await cached.upload_bytes(b"hello", path)
            snap = cached.dump_cache()
            assert snap.get("exists", {}).get(path) is True, "exists should be backfilled after upload"
        finally:
            await cached.delete(path)

    async def test_is_file_backfilled_after_upload(self, cached: CachedStorage):
        path = f"test-cisfile-{uid()}"
        try:
            await cached.upload_bytes(b"hello", path)
            snap = cached.dump_cache()
            assert snap.get("is_file", {}).get(path) is True, "is_file should be backfilled after upload"
        finally:
            await cached.delete(path)

    async def test_is_dir_backfilled_after_mkdir(self, cached: CachedStorage):
        path = f"test-cisdir-{uid()}"
        try:
            await cached.mkdir(path)
            snap = cached.dump_cache()
            assert snap.get("is_dir", {}).get(path) is True, "is_dir should be backfilled after mkdir"
        finally:
            await cached.delete(path)

    async def test_stat_backfilled_on_miss(self, cached: CachedStorage):
        path = f"test-cstat-{uid()}"
        try:
            await cached.upload_bytes(b"hello world", path)

            # Upload does NOT backfill stat — clear + force miss
            await _clear_path(cached, path)

            info = await cached.stat(path)  # miss → backfills stat + exists + is_file + is_dir
            assert info.size == 11

            snap = cached.dump_cache()
            assert path in snap.get("stat", {}), "stat should be backfilled after miss"
        finally:
            await cached.delete(path)


# ---------------------------------------------------------------------------
# TestCacheInvalidation
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# TestCrossBackfill
# ---------------------------------------------------------------------------


class TestCrossBackfill:
    """Verify that a cache *miss* on one operation backfills related namespaces."""

    async def test_is_file_miss_backfills_exists_and_is_dir(self, cached: CachedStorage):
        path = f"test-bf-exists-{uid()}"
        try:
            await cached.upload_bytes(b"hello", path)

            # Clear ALL caches for this path → force a true miss
            await _clear_path(cached, path)

            snap = cached.dump_cache()
            assert path not in snap.get("exists", {}), "exists should be cold"
            assert path not in snap.get("is_file", {}), "is_file should be cold"

            assert await cached.is_file(path)  # miss → backfills

            snap = cached.dump_cache()
            assert snap.get("exists", {}).get(path) is True, "exists should be backfilled True"
            assert snap.get("is_dir", {}).get(path) is False, "is_dir should be backfilled False"
            assert snap.get("is_file", {}).get(path) is True, "is_file should be backfilled True"
        finally:
            await cached.delete(path)

    async def test_is_dir_miss_backfills_exists_and_is_file(self, cached: CachedStorage):
        path = f"test-bf-isdir-{uid()}"
        try:
            await cached.mkdir(path)

            # Clear ALL caches → force miss
            await _clear_path(cached, path)

            assert await cached.is_dir(path)  # miss → backfills

            snap = cached.dump_cache()
            assert snap.get("exists", {}).get(path) is True, "exists should be backfilled True"
            assert snap.get("is_file", {}).get(path) is False, "is_file should be backfilled False"
            assert snap.get("is_dir", {}).get(path) is True, "is_dir should be backfilled True"
        finally:
            await cached.delete(path)

    async def test_stat_file_miss_backfills_all(self, cached: CachedStorage):
        path = f"test-bf-statf-{uid()}"
        try:
            await cached.upload_bytes(b"hello world", path)

            # Clear ALL caches → force stat miss
            await _clear_path(cached, path)

            info = await cached.stat(path)  # miss → backfills
            assert not info.is_dir

            snap = cached.dump_cache()
            assert snap.get("exists", {}).get(path) is True, "exists should be backfilled True"
            assert snap.get("is_file", {}).get(path) is True, "is_file should be backfilled True"
            assert snap.get("is_dir", {}).get(path) is False, "is_dir should be backfilled False"
            assert path in snap.get("stat", {}), "stat should be backfilled"
        finally:
            await cached.delete(path)

    async def test_stat_dir_miss_backfills_all(self, cached: CachedStorage):
        path = f"test-bf-statd-{uid()}"
        try:
            await cached.mkdir(path)

            # Clear ALL caches → force stat miss
            await _clear_path(cached, path)

            info = await cached.stat(path)  # miss → backfills
            assert info.is_dir

            snap = cached.dump_cache()
            assert snap.get("exists", {}).get(path) is True, "exists should be backfilled True"
            assert snap.get("is_file", {}).get(path) is False, "is_file should be backfilled False"
            assert snap.get("is_dir", {}).get(path) is True, "is_dir should be backfilled True"
            assert path in snap.get("stat", {}), "stat should be backfilled"
        finally:
            await cached.delete(path)


# ---------------------------------------------------------------------------
# TestIsFileCacheMiss
# ---------------------------------------------------------------------------


class TestIsFileCacheMiss:
    """Verify is_file cache is populated even when download cache is skipped."""

    async def test_is_file_backfilled_when_download_exceeds_threshold(self, cached: CachedStorage):
        data = b"X" * 20000  # > default 16 KB threshold
        path = f"test-isfile-miss-{uid()}"
        try:
            await cached.upload_bytes(data, path)

            snap = cached.dump_cache()
            # is_file/exists/is_dir always backfilled by upload, regardless of size
            assert snap.get("is_file", {}).get(path) is True
            assert snap.get("exists", {}).get(path) is True
            # download cache skipped because data exceeds threshold
            assert path not in snap.get("download", {}), "download must NOT be cached (above threshold)"
        finally:
            await cached.delete(path)


# ---------------------------------------------------------------------------
# TestIterdirCacheHit
# ---------------------------------------------------------------------------


class TestIterdirCacheHit:
    """Verify iterdir results are cached on first miss and reused on second call."""

    async def test_iterdir_cached_on_miss(self, cached: CachedStorage):
        dirpath = f"test-itd-hit-{uid()}"
        filepath = f"{dirpath}/a.txt"
        try:
            await cached.mkdir(dirpath)
            await cached.upload_bytes(b"a", filepath)

            # First call: miss → populates cache
            entries = [e async for e in cached.iterdir(dirpath)]
            assert len(entries) == 1
            assert entries[0].name == "a.txt"

            snap = cached.dump_cache()
            assert dirpath in snap.get("iterdir", {}), "iterdir result should be cached after miss"

            # Second call: uses cache
            entries2 = [e async for e in cached.iterdir(dirpath)]
            assert len(entries2) == 1
            assert entries2[0].name == "a.txt"
        finally:
            await cached.rmtree(dirpath)


# ---------------------------------------------------------------------------
# TestListBackfill
# ---------------------------------------------------------------------------


class TestListBackfill:
    """Verify list_ backfills individual per-entry caches.

    ``list_`` **always** queries the underlying storage (bypassing the
    iterdir cache), but backfills stat/exists/is_file/is_dir for each
    returned entry.
    """

    async def test_list_backfills_individual_entries(self, cached: CachedStorage):
        dirpath = f"test-list-bf-{uid()}"
        filepath = f"{dirpath}/file.txt"
        try:
            await cached.mkdir(dirpath)
            await cached.upload_bytes(b"hello", filepath)

            # Clear all entry-level caches → start cold
            await _clear_path(cached, filepath)

            result = await cached.list_(dirpath)
            assert len(result) == 1
            assert result[0].name == "file.txt"
            assert not result[0].is_dir

            snap = cached.dump_cache()
            assert snap.get("is_file", {}).get(filepath) is True, "is_file should be backfilled by list_"
            assert snap.get("exists", {}).get(filepath) is True, "exists should be backfilled by list_"
            # list_ does NOT populate the iterdir cache (it bypasses it)
            assert dirpath not in snap.get("iterdir", {}), "list_ should not populate iterdir cache"
        finally:
            await cached.rmtree(dirpath)


# ---------------------------------------------------------------------------
# TestDownloadCacheThreshold
# ---------------------------------------------------------------------------


class TestDownloadCacheThreshold:
    """Verify download cache is NOT populated above threshold."""

    async def test_large_file_download_not_cached(self, cached: CachedStorage):
        data = b"L" * 20000  # > 16 KB
        path = f"test-dl-thresh-{uid()}"
        try:
            await cached.upload_bytes(data, path)

            # Upload skipped download due to threshold
            snap = cached.dump_cache()
            assert path not in snap.get("download", {}), "download not cached after large upload"

            # Download still works (miss path)
            result = await cached.download_bytes(path)
            assert result == data

            # Still not cached after download (miss path also checks threshold)
            snap = cached.dump_cache()
            assert path not in snap.get("download", {}), "download not cached even after download"
        finally:
            await cached.delete(path)


# ---------------------------------------------------------------------------
# TestDownloadCache
# ---------------------------------------------------------------------------


class TestDownloadCache:
    """Verify download cache IS populated for small files."""

    async def test_small_file_download_is_cached(self, cached: CachedStorage):
        path = f"test-dlcache-{uid()}"
        data = b"hello download cache test"  # < 16 KB
        try:
            await cached.upload_bytes(data, path)

            snap = cached.dump_cache()
            assert path in snap.get("download", {}), "download should be cached after small upload"
            assert snap["download"][path] == data

            result1 = await cached.download_bytes(path)
            assert result1 == data

            # Cache persists
            snap = cached.dump_cache()
            assert path in snap.get("download", {})
        finally:
            await cached.delete(path)
