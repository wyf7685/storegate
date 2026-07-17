from unittest.mock import AsyncMock

import pytest

from app.storage.cached import CachedStorage
from app.storage.memory import MemoryStorage


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
