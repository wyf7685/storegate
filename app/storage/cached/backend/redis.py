import contextlib
import hashlib
import json
import re
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

    Each namespace is stored below a storage-specific prefix.  With the
    default ``key_prefix="auto"``, :meth:`bind_storage` derives
    ``storegate:v1:{sha256}:{namespace}:{key}`` from the wrapped storage's
    stable, non-secret cache identity.  Per-key TTL is set via ``SET … EX``.

    Parameters
    ----------
    url:
        Redis connection URL (e.g. ``"redis://localhost:6379/0"``).
    key_prefix:
        ``"auto"`` (default) derives an isolated prefix from the bound
        storage.  A validated explicit prefix is available for storage
        implementations that cannot provide a stable identity.
    **kw:
        Extra keyword arguments forwarded to :func:`redis.asyncio.from_url`.
    """

    _PREFIX = "storegate"
    _PREFIX_RE = re.compile(r"[A-Za-z0-9:_-]+")

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        key_prefix: str = "auto",
        **kw: Any,
    ) -> None:
        super().__init__()
        if key_prefix != "auto" and self._PREFIX_RE.fullmatch(key_prefix) is None:
            raise ValueError("key_prefix must contain only letters, digits, ':', '_' or '-'")
        self._url = url
        self._key_prefix = key_prefix
        self._instance_prefix: str | None = None if key_prefix == "auto" else key_prefix
        self._bound_identity: str | None = None
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
    # Storage binding / namespace configuration
    # ------------------------------------------------------------------

    @override
    def bind_storage(self, identity: str | None) -> None:
        if self._key_prefix != "auto":
            return
        if identity is None:
            raise ValueError("RedisCacheBackend requires a stable cache identity when key_prefix='auto'")

        instance_prefix = f"{self._PREFIX}:v1:{hashlib.sha256(identity.encode()).hexdigest()}"
        if self._bound_identity is not None and self._bound_identity != identity:
            raise ValueError("RedisCacheBackend cannot be bound to multiple storage identities")
        self._bound_identity = identity
        self._instance_prefix = instance_prefix

    @override
    def configure_namespace(self, name: str, ttl: int, **opts: Any) -> None:
        self._ttls[name] = ttl
        self._serializers[name], self._deserializers[name] = _serializers_for(name)

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _scope_prefix(self) -> str:
        if self._instance_prefix is None:
            raise RuntimeError("Redis cache backend is not bound to a storage identity")
        return self._instance_prefix

    def _rk(self, namespace: str, key: str) -> str:
        return f"{self._scope_prefix()}:{namespace}:{key}"

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
        scope_prefix = self._scope_prefix()
        pattern = f"{scope_prefix}:{namespace}:*" if namespace is not None else f"{scope_prefix}:*"
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
