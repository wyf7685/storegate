"""RedisCacheBackend tests."""

import fnmatch
from pathlib import Path
from typing import Any, Self, cast

import pytest
from pydantic import SecretStr

from app.storage.cached import CachedStorage, RedisCacheBackend
from app.storage.dav.client import DavConfig
from app.storage.local import LocalStorage
from app.storage.s3.client import S3Config


class FakePipeline:
    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._operations: list[tuple[str, tuple[Any, ...]]] = []

    def get(self, key: str) -> Self:
        self._operations.append(("get", (key,)))
        return self

    def set(self, key: str, value: bytes, *, ex: int) -> Self:
        self._operations.append(("set", (key, value, ex)))
        return self

    def delete(self, key: str) -> Self:
        self._operations.append(("delete", (key,)))
        return self

    async def execute(self) -> list[Any]:
        results: list[Any] = []
        for name, args in self._operations:
            if name == "get":
                results.append(await self._redis.get(*args))
            elif name == "set":
                results.append(await self._redis.set(*args[:2], ex=args[2]))
            elif name == "delete":
                results.append(await self._redis.delete(*args))
            else:
                raise AssertionError(f"Unsupported pipeline operation: {name}")
        return results


class FakeRedis:
    def __init__(self, values: dict[str, bytes] | None = None) -> None:
        self.values: dict[str, bytes] = {} if values is None else values
        self.scan_matches: list[str] = []
        self.closed = False

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        self.closed = True

    async def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    async def set(self, key: str, value: bytes, *, ex: int) -> bool:  # noqa: ARG002
        self.values[key] = value
        return True

    async def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            if key in self.values:
                del self.values[key]
                removed += 1
        return removed

    async def scan(self, cursor: int, *, match: str, count: int) -> tuple[int, list[str]]:
        del cursor, count
        self.scan_matches.append(match)
        keys = [key for key in self.values if fnmatch.fnmatchcase(key, match)]
        return 0, keys

    def pipeline(self, *, transaction: bool) -> FakePipeline:
        assert not transaction
        return FakePipeline(self)


def _install_fake_client(backend: RedisCacheBackend, redis: FakeRedis) -> None:
    cast("Any", backend)._redis = redis


def _backend(redis: FakeRedis, identity: str) -> RedisCacheBackend:
    backend = RedisCacheBackend()
    backend.bind_storage(identity)
    backend.configure_namespace("exists", 30)
    _install_fake_client(backend, redis)
    return backend


def _s3_config(*, secret_access_key: str, endpoint_url: str) -> S3Config:
    return S3Config(
        access_key_id=SecretStr("access-key"),
        secret_access_key=SecretStr(secret_access_key),
        region="test-region",
        bucket="test-bucket",
        endpoint_url=endpoint_url,
        path_style=True,
    )


def _dav_config(*, password: str, root_prefix: str) -> DavConfig:
    return DavConfig(
        base_url="https://dav.example.test/webdav",
        username="user",
        password=SecretStr(password),
        root_prefix=root_prefix,
    )


async def test_auto_prefix_isolates_shared_redis() -> None:
    redis = FakeRedis()
    first = _backend(redis, '{"kind":"local","root":"/first"}')
    second = _backend(redis, '{"kind":"local","root":"/second"}')

    await first.set("exists", "same.txt", True)
    await second.set("exists", "same.txt", False)
    await first.mset(("exists", "batch.txt", True))
    await second.mset(("exists", "batch.txt", False))

    assert await first.get("exists", "same.txt") is True
    assert await second.get("exists", "same.txt") is False
    assert await first.mget(("exists", "batch.txt")) == [True]
    assert await second.mget(("exists", "batch.txt")) == [False]

    await first.mdelete(("exists", "batch.txt"))
    await first.clear()

    assert await first.get("exists", "same.txt") is None
    assert await second.get("exists", "same.txt") is False
    assert await first.get("exists", "batch.txt") is None
    assert await second.get("exists", "batch.txt") is False
    assert redis.scan_matches == [f"{first._scope_prefix()}:*"]


async def test_cached_storage_close_preserves_redis_entries(tmp_path: Path) -> None:
    values: dict[str, bytes] = {}
    first_backend = RedisCacheBackend()
    _install_fake_client(first_backend, FakeRedis(values))
    first = CachedStorage(LocalStorage(tmp_path), cache=first_backend)
    await first_backend.set("exists", "cached.txt", True)

    await first.close()
    assert first_backend._redis is None

    second_backend = RedisCacheBackend()
    _install_fake_client(second_backend, FakeRedis(values))
    CachedStorage(LocalStorage(tmp_path), cache=second_backend)

    assert await second_backend.get("exists", "cached.txt") is True


def test_auto_prefix_rejects_unknown_or_conflicting_storage() -> None:
    backend = RedisCacheBackend()
    with pytest.raises(ValueError, match="stable cache identity"):
        backend.bind_storage(None)

    backend.bind_storage('{"kind":"local","root":"/first"}')
    with pytest.raises(ValueError, match="multiple storage identities"):
        backend.bind_storage('{"kind":"local","root":"/second"}')


def test_explicit_key_prefix_is_validated_and_compatible() -> None:
    backend = RedisCacheBackend(key_prefix="storegate")
    assert backend._rk("exists", "file.txt") == "storegate:exists:file.txt"

    with pytest.raises(ValueError, match="key_prefix"):
        RedisCacheBackend(key_prefix="invalid*")
