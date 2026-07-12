import contextlib
import json
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any, final, override

from app.storage.abstract import FileInfo

from .base import CacheBackend

if TYPE_CHECKING:
    import redis.asyncio as aioredis


def _bool_to_bytes(v: bool) -> bytes:
    return b"1" if v else b"0"


def _bytes_to_bool(v: bytes | None) -> bool | None:
    if v is None:
        return None
    return v == b"1"


def _file_info_to_json(info: FileInfo) -> bytes:
    data: dict[str, object] = {
        "path": info.path,
        "name": info.name,
        "is_dir": info.is_dir,
        "size": info.size,
    }
    if info.modified is not None:
        data["modified"] = info.modified.isoformat()
    if info.created is not None:
        data["created"] = info.created.isoformat()
    return json.dumps(data, separators=(",", ":")).encode()


def _json_to_file_info(raw: bytes) -> FileInfo:
    obj = json.loads(raw.decode())
    modified = datetime.fromisoformat(obj["modified"]) if "modified" in obj else None
    created = datetime.fromisoformat(obj["created"]) if "created" in obj else None
    return FileInfo(
        path=obj.get("path", ""),
        name=obj.get("name", ""),
        is_dir=obj.get("is_dir", False),
        size=obj.get("size", 0),
        modified=modified,
        created=created,
    )


def _file_info_list_to_json(infos: list[FileInfo]) -> bytes:
    return json.dumps([_file_info_to_json_plain(fi) for fi in infos], separators=(",", ":")).encode()


def _file_info_to_json_plain(info: FileInfo) -> dict[str, object]:
    data: dict[str, object] = {
        "path": info.path,
        "name": info.name,
        "is_dir": info.is_dir,
        "size": info.size,
    }
    if info.modified is not None:
        data["modified"] = info.modified.isoformat()
    if info.created is not None:
        data["created"] = info.created.isoformat()
    return data


def _json_to_file_info_list(raw: bytes) -> list[FileInfo]:
    items = json.loads(raw.decode())
    return [_json_to_file_info_from_dict(item) for item in items]


def _json_to_file_info_from_dict(obj: dict[str, Any]) -> FileInfo:
    modified = datetime.fromisoformat(obj["modified"]) if "modified" in obj else None
    created = datetime.fromisoformat(obj["created"]) if "created" in obj else None
    return FileInfo(
        path=obj.get("path", ""),
        name=obj.get("name", ""),
        is_dir=obj.get("is_dir", False),
        size=obj.get("size", 0),
        modified=modified,
        created=created,
    )


@final
class RedisCacheBackend(CacheBackend):
    """Redis-backed cache backend using :mod:`redis.asyncio`.

    Each namespace is stored under the Redis key prefix
    ``storegate:{namespace}:{key}``.  Per-key TTL is set via
    ``SET … EX``.

    Parameters
    ----------
    url:
        Redis connection URL (e.g. ``"redis://localhost:6379/0"``).
    **kw:
        Extra keyword arguments forwarded to :func:`redis.asyncio.from_url`.
    """

    _PREFIX = "storegate"

    def __init__(self, url: str = "redis://localhost:6379/0", **kw: Any) -> None:
        super().__init__()
        self._url = url
        self._kw = kw
        self._redis: aioredis.Redis | None = None
        self._ttls: dict[str, int] = {}
        self._serializers: dict[str, Callable[[Any], bytes]] = {}
        self._deserializers: dict[str, Callable[[bytes], Any]] = {}

    def _ensure_client(self) -> aioredis.Redis:
        if self._redis is None:
            raise RuntimeError("Redis client not connected. Call connect() first.")
        return self._redis

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @override
    async def connect(self) -> None:
        if self._redis is not None:
            return  # idempotent

        import redis.asyncio as aioredis

        client: aioredis.Redis = await aioredis.from_url(self._url, **self._kw)
        await client.ping()
        self._redis = client

    @override
    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    @override
    async def ping(self) -> bool:
        try:
            return await self._ensure_client().ping()
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Namespace configuration
    # ------------------------------------------------------------------

    @override
    def configure_namespace(self, name: str, ttl: int, **opts: Any) -> None:
        self._ttls[name] = ttl
        self._serializers[name], self._deserializers[name] = _serializers_for(name)

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _rk(self, namespace: str, key: str) -> str:
        return f"{self._PREFIX}:{namespace}:{key}"

    # ------------------------------------------------------------------
    # Single-key operations
    # ------------------------------------------------------------------

    @override
    async def get(self, namespace: str, key: str) -> Any:
        try:
            raw: str | bytes | None = await self._ensure_client().get(self._rk(namespace, key))
        except Exception:
            return None
        if raw is None:
            return None
        if isinstance(raw, str):
            raw = raw.encode()
        return self._deserializers[namespace](raw)

    @override
    async def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> None:
        rk = self._rk(namespace, key)
        raw = self._serializers[namespace](value)
        ex = ttl if ttl is not None else self._ttls[namespace]
        with contextlib.suppress(Exception):
            await self._ensure_client().set(rk, raw, ex=ex)

    @override
    async def delete(self, namespace: str, key: str) -> bool:
        try:
            return await self._ensure_client().delete(self._rk(namespace, key)) > 0
        except Exception:
            return False

    @override
    async def clear(self, namespace: str | None = None) -> None:
        r = self._ensure_client()
        pattern = f"{self._PREFIX}:{namespace}:*" if namespace is not None else f"{self._PREFIX}:*"
        cursor = 0
        with contextlib.suppress(Exception):
            while True:
                cursor, keys = await r.scan(cursor, match=pattern, count=100)
                if keys:
                    await r.delete(*keys)
                if cursor == 0:
                    break

    # ------------------------------------------------------------------
    # Batch / pipeline operations
    # ------------------------------------------------------------------

    @override
    async def mget(self, *keys: tuple[str, str]) -> list[Any]:
        pipe = self._ensure_client().pipeline(transaction=False)
        for ns, key in keys:
            pipe.get(self._rk(ns, key))
        try:
            raws: list[bytes | None] = await pipe.execute()
        except Exception:
            return [None] * len(keys)
        return [
            self._deserializers[ns](raw) if raw is not None else None for (ns, _), raw in zip(keys, raws, strict=True)
        ]

    @override
    async def mset(self, *entries: tuple[str, str, Any]) -> None:
        pipe = self._ensure_client().pipeline(transaction=False)
        for ns, key, value in entries:
            rk = self._rk(ns, key)
            raw = self._serializers[ns](value)
            pipe.set(rk, raw, ex=self._ttls[ns])
        with contextlib.suppress(Exception):
            await pipe.execute()

    @override
    async def mdelete(self, *keys: tuple[str, str]) -> int:
        pipe = self._ensure_client().pipeline(transaction=False)
        for ns, key in keys:
            pipe.delete(self._rk(ns, key))
        try:
            results: list[int] = await pipe.execute()
        except Exception:
            return 0
        return sum(1 for r in results if r > 0)


def _serializers_for(ns: str) -> tuple[Callable[[Any], bytes], Callable[[bytes], Any]]:
    """Return ``(serializer, deserializer)`` for *ns*."""
    if ns in ("exists", "is_file", "is_dir"):
        return _bool_to_bytes, _bytes_to_bool
    if ns == "stat":
        return _file_info_to_json, _json_to_file_info
    if ns == "iterdir":
        return _file_info_list_to_json, _json_to_file_info_list
    if ns == "download":
        return lambda v: v, lambda v: v
    raise ValueError(f"Unknown namespace: {ns!r}")
