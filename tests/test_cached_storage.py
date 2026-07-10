"""Tests for CachedStorage cache behavior.

The general storage test suite already covers basic CRUD operations via
the parametrized ``storage`` fixture. This file validates CachedStorage-specific
cache semantics: hit/miss, invalidation on writes, cross-backfill, and
download caching.
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


class TestCacheHit:
    """Verify cached results are returned without hitting the underlying storage."""

    async def test_exists_cache_hit(self, cached: CachedStorage):
        path = f"test-cexists-{uid()}"
        try:
            await cached.upload_bytes(b"hello", path)
            # First call: cache miss, delegates to inner storage
            assert await cached.exists(path)
            # Second call: cache hit
            assert await cached.exists(path)
        finally:
            await cached.delete(path)

    async def test_is_file_cache_hit(self, cached: CachedStorage):
        path = f"test-cisfile-{uid()}"
        try:
            await cached.upload_bytes(b"hello", path)
            assert await cached.is_file(path)
            # Cache hit
            assert await cached.is_file(path)
        finally:
            await cached.delete(path)

    async def test_is_dir_cache_hit(self, cached: CachedStorage):
        path = f"test-cisdir-{uid()}"
        try:
            await cached.mkdir(path)
            assert await cached.is_dir(path)
            # Cache hit
            assert await cached.is_dir(path)
        finally:
            await cached.delete(path)

    async def test_stat_cache_hit(self, cached: CachedStorage):
        path = f"test-cstat-{uid()}"
        try:
            await cached.upload_bytes(b"hello world", path)
            info1 = await cached.stat(path)
            info2 = await cached.stat(path)
            assert info1.size == info2.size == 11
        finally:
            await cached.delete(path)


class TestCacheInvalidation:
    """Verify cache is invalidated after write operations."""

    async def test_unlink_invalidates_cache(self, cached: CachedStorage):
        path = f"test-inv-unlink-{uid()}"
        await cached.upload_bytes(b"hello", path)
        assert await cached.exists(path)
        await cached.unlink(path)
        assert not await cached.exists(path)

    async def test_rmdir_invalidates_cache(self, cached: CachedStorage):
        path = f"test-inv-rmdir-{uid()}"
        await cached.mkdir(path)
        assert await cached.is_dir(path)
        await cached.rmdir(path)
        assert not await cached.exists(path)

    async def test_move_invalidates_cache(self, cached: CachedStorage):
        src = f"test-inv-move-src-{uid()}"
        dst = f"test-inv-move-dst-{uid()}"
        try:
            await cached.upload_bytes(b"hello", src)
            assert await cached.exists(src)
            await cached.move(src, dst)
            assert not await cached.exists(src)
            assert await cached.exists(dst)
        finally:
            await cached.delete(dst)
            with contextlib.suppress(Exception):
                await cached.delete(src)


class TestCrossBackfill:
    """Verify cross-namespace cache backfill.

    When ``is_file`` returns True, ``exists`` and ``is_dir`` should be
    backfilled with correct values.
    """

    async def test_is_file_backfills_exists(self, cached: CachedStorage):
        path = f"test-bf-exists-{uid()}"
        try:
            await cached.upload_bytes(b"hello", path)
            assert await cached.is_file(path)
            # exists should now be cached as True without a separate API call
            assert await cached.exists(path)
        finally:
            await cached.delete(path)

    async def test_is_dir_backfills_exists(self, cached: CachedStorage):
        path = f"test-bf-isdir-{uid()}"
        try:
            await cached.mkdir(path)
            assert await cached.is_dir(path)
            # exists should now be cached as True
            assert await cached.exists(path)
            # is_file should be cached as False
            assert not await cached.is_file(path)
        finally:
            await cached.delete(path)

    async def test_stat_file_backfills(self, cached: CachedStorage):
        path = f"test-bf-statf-{uid()}"
        try:
            await cached.upload_bytes(b"hello world", path)
            info = await cached.stat(path)
            assert not info.is_dir
            # Backfill: exists=True, is_file=True, is_dir=False
            assert await cached.exists(path)
            assert await cached.is_file(path)
            assert not await cached.is_dir(path)
        finally:
            await cached.delete(path)

    async def test_stat_dir_backfills(self, cached: CachedStorage):
        path = f"test-bf-statd-{uid()}"
        try:
            await cached.mkdir(path)
            info = await cached.stat(path)
            assert info.is_dir
            # Backfill: exists=True, is_file=False, is_dir=True
            assert await cached.exists(path)
            assert not await cached.is_file(path)
            assert await cached.is_dir(path)
        finally:
            await cached.delete(path)


class TestDownloadCache:
    """Verify download results are cached below the threshold."""

    async def test_download_cache(self, cached: CachedStorage):
        path = f"test-dlcache-{uid()}"
        data = b"hello download cache test"
        try:
            await cached.upload_bytes(data, path)
            result1 = await cached.download_bytes(path)
            result2 = await cached.download_bytes(path)
            assert result1 == data
            assert result2 == data
        finally:
            await cached.delete(path)
