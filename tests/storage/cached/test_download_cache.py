"""CachedStorage behavior tests."""

import pytest

from storegate.storage.cached import CachedStorage
from tests.support.ids import uid


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


class TestDownloadOffsetParity:
    async def test_negative_offset_rejected_on_hit_and_miss(self, cached: CachedStorage) -> None:
        path = f"test-dlcache-negative-{uid()}"
        data = b"cached-content"
        try:
            await cached.upload_bytes(data, path)
            with pytest.raises(ValueError, match="non-negative"):
                await anext(cached.download_stream(path, offset=-1))

            await cached._cache.delete("download", path)
            with pytest.raises(ValueError, match="non-negative"):
                await anext(cached.download_stream(path, offset=-1))
        finally:
            await cached.delete(path)

    async def test_at_or_beyond_size_is_empty_on_hit_and_miss(self, cached: CachedStorage) -> None:
        path = f"test-dlcache-empty-{uid()}"
        data = b"cached-content"
        try:
            await cached.upload_bytes(data, path)
            for offset in (len(data), len(data) + 1):
                assert [chunk async for chunk in cached.download_stream(path, offset=offset)] == []

            await cached._cache.delete("download", path)
            for offset in (len(data), len(data) + 1):
                assert [chunk async for chunk in cached.download_stream(path, offset=offset)] == []
        finally:
            await cached.delete(path)
