"""CachedStorage behavior tests."""

from app.storage.cached import CachedStorage
from tests.storage.cached.helpers import _clear_path
from tests.support.ids import uid


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
