from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from storegate.storage.abstract import EntryKind, FileInfo
from storegate.storage.cached import CachedStorage
from storegate.storage.cached.backend.base import DOWNLOAD, EXISTS, STAT
from storegate.storage.cached.backend.memory import MemoryCacheBackend
from storegate.storage.memory import MemoryStorage

_EIGHT_NAMESPACES = frozenset(
    {
        "exists",
        "is_file",
        "is_dir",
        "is_symlink",
        "stat",
        "lstat",
        "iterdir",
        "download",
    }
)


async def test_child_connect_failure_closes_cache_and_allows_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    child = MemoryStorage("/")
    cached = CachedStorage(child)
    cache_connect = AsyncMock()
    cache_close = AsyncMock()
    monkeypatch.setattr(cached._cache, "connect", cache_connect)
    monkeypatch.setattr(cached._cache, "close", cache_close)
    child_connect = AsyncMock(side_effect=[RuntimeError("child failed"), None])
    monkeypatch.setattr(child, "connect", child_connect)

    with pytest.raises(RuntimeError, match="child failed"):
        await cached.connect()
    cache_connect.assert_awaited_once()
    cache_close.assert_awaited_once()
    await cached.connect()
    assert child_connect.await_count == 2
    await cached.close()


async def test_child_and_cache_connect_rollback_failures_are_preserved_and_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    child = MemoryStorage("/")
    cached = CachedStorage(child)
    cache_connect = AsyncMock()
    cache_close = AsyncMock(side_effect=[OSError("cache cleanup failed"), None])
    child_connect = AsyncMock(side_effect=[RuntimeError("child failed"), None])
    monkeypatch.setattr(cached._cache, "connect", cache_connect)
    monkeypatch.setattr(cached._cache, "close", cache_close)
    monkeypatch.setattr(child, "connect", child_connect)

    with pytest.raises(BaseExceptionGroup) as caught:
        await cached.connect()

    assert [str(error) for error in caught.value.exceptions] == ["child failed", "cache cleanup failed"]
    assert cached._lifecycle_state == "NEW"
    await cached.connect()
    assert child_connect.await_count == 2
    assert cache_close.await_count == 1
    await cached.close()
    assert cache_close.await_count == 2


async def test_two_async_with_lifecycles_preserve_namespaces_without_stale_values() -> None:
    child = MemoryStorage("/")
    cached = CachedStorage(child)
    path = "reconnect.txt"

    async with cached:
        await cached.upload_bytes(b"first-life", path)
        assert await cached.exists(path) is True
        snapshot = cached.dump_cache()
        assert set(snapshot) == _EIGHT_NAMESPACES
        assert snapshot["exists"].get(path) is True
        assert any(values for values in snapshot.values())

    # close() clears values but keeps the eight configured namespaces.
    closed_snapshot = cached.dump_cache()
    assert set(closed_snapshot) == _EIGHT_NAMESPACES
    assert all(values == {} for values in closed_snapshot.values())

    async with cached:
        second_snapshot = cached.dump_cache()
        assert set(second_snapshot) == _EIGHT_NAMESPACES
        assert all(values == {} for values in second_snapshot.values())
        assert await cached.exists(path) is True
        assert cached.dump_cache()["exists"].get(path) is True


async def test_memory_configure_namespace_is_idempotent_and_rejects_conflicts() -> None:
    backend = MemoryCacheBackend(capacity=8)
    backend.configure_namespace(EXISTS, 30)
    backend.configure_namespace(EXISTS, 30)  # identical config is a no-op
    with pytest.raises(ValueError, match="already configured"):
        backend.configure_namespace(EXISTS, 60)
    with pytest.raises(ValueError, match="already configured"):
        backend.configure_namespace(EXISTS, 30, capacity=4)


def test_cached_storage_rejects_invalid_configuration() -> None:
    with pytest.raises(ValueError, match="ttl must be > 0"):
        CachedStorage(MemoryStorage("/"), ttl=0)
    with pytest.raises(ValueError, match="ttl must be > 0"):
        CachedStorage(MemoryStorage("/"), ttl=-1)
    with pytest.raises(ValueError, match="capacity must be >= 4"):
        CachedStorage(MemoryStorage("/"), capacity=3)
    with pytest.raises(ValueError, match="capacity must be >= 4"):
        CachedStorage(MemoryStorage("/"), capacity=0)
    with pytest.raises(ValueError, match="download_cache_threshold must be None or >= 0"):
        CachedStorage(MemoryStorage("/"), download_cache_threshold=-1)


def test_download_cache_threshold_none_and_zero_are_valid() -> None:
    CachedStorage(MemoryStorage("/"), download_cache_threshold=None)
    CachedStorage(MemoryStorage("/"), download_cache_threshold=0)


async def test_compare_exchange_proxies_capability_and_invalidates_caches() -> None:
    child = MemoryStorage("/")
    async with CachedStorage(child) as cached:
        assert cached.capabilities is child.capabilities
        assert cached.capabilities.compare_exchange is True

        created = await cached.compare_exchange("cas.txt", expected_token=None, data=b"v1")
        assert created is not None
        assert created.data == b"v1"
        snap = cached.dump_cache()
        assert snap["exists"].get("cas.txt") is True
        assert snap["is_file"].get("cas.txt") is True
        assert snap["download"].get("cas.txt") == b"v1"

        # Seed a stale download/metadata view that CAS must replace.
        await cached._cache.set(DOWNLOAD.entry("cas.txt", b"stale"))
        stale_info = FileInfo(path="cas.txt", name="cas.txt", kind=EntryKind.FILE, size=99)
        await cached._cache.set(STAT.entry("cas.txt", stale_info))

        updated = await cached.compare_exchange("cas.txt", expected_token=created.token, data=b"v2")
        assert updated is not None
        assert updated.data == b"v2"
        snap = cached.dump_cache()
        assert snap["download"].get("cas.txt") == b"v2"
        assert "stat" not in snap or "cas.txt" not in snap.get("stat", {})
        assert snap["exists"].get("cas.txt") is True

        conflict = await cached.compare_exchange("cas.txt", expected_token=created.token, data=b"v3")
        assert conflict is None
        # Conflict leaves previous successful cache facts intact.
        assert cached.dump_cache()["download"].get("cas.txt") == b"v2"

        versioned = await cached.read_versioned("cas.txt")
        assert versioned is not None
        assert versioned.data == b"v2"
        assert versioned.token == updated.token


async def test_cas_methods_reject_invalid_paths_before_wrapped_io(monkeypatch: pytest.MonkeyPatch) -> None:
    child = MemoryStorage("/")
    cached = CachedStorage(child)
    read_versioned = AsyncMock(return_value=None)
    compare_exchange = AsyncMock(return_value=None)
    monkeypatch.setattr(child, "read_versioned", read_versioned)
    monkeypatch.setattr(child, "compare_exchange", compare_exchange)

    with pytest.raises(ValueError, match="segments"):
        await cached.read_versioned("../escape")
    with pytest.raises(ValueError, match="NUL"):
        await cached.read_versioned("bad\x00path")
    with pytest.raises(ValueError, match="segments"):
        await cached.compare_exchange("../escape", expected_token=None, data=b"x")
    with pytest.raises(ValueError, match="NUL"):
        await cached.compare_exchange("bad\x00path", expected_token=None, data=b"x")

    read_versioned.assert_not_awaited()
    compare_exchange.assert_not_awaited()
