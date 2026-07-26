import contextlib
import hashlib
import re
from typing import TYPE_CHECKING, Any, final, override

from storegate.utils import requires_extra

from .base import CacheBackend, CacheEntry, Namespace

if TYPE_CHECKING:
    import redis.asyncio as aioredis


@final
class RedisCacheBackend(CacheBackend):
    """Redis-backed cache backend using :mod:`redis.asyncio`.

    Each namespace is stored below a storage-specific prefix. With the default
    ``key_prefix="auto"``, :meth:`bind_storage` derives
    ``storegate:v3:{sha256}:{namespace}:{key}`` from the wrapped storage's
    stable, non-secret namespace identity. Per-key TTL is set via ``SET … EX``.

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
        requires_extra("redis", extra_name="redis")
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
            raise ValueError("RedisCacheBackend requires a stable namespace identity when key_prefix='auto'")

        instance_prefix = f"{self._PREFIX}:v3:{hashlib.sha256(identity.encode()).hexdigest()}"
        if self._bound_identity is not None and self._bound_identity != identity:
            raise ValueError("RedisCacheBackend cannot be bound to multiple storage identities")
        self._bound_identity = identity
        self._instance_prefix = instance_prefix

    @override
    def configure_namespace(self, namespace: Namespace[Any], ttl: int, **opts: Any) -> None:
        del opts  # Redis namespaces ignore capacity and other memory-only options.
        if ttl <= 0:
            raise ValueError("ttl must be > 0")
        name = namespace.name
        existing = self._ttls.get(name)
        if existing is not None:
            if existing == ttl:
                return
            raise ValueError(
                f"namespace {name!r} already configured with ttl={existing}; cannot reconfigure with ttl={ttl}"
            )
        self._ttls[name] = ttl

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _scope_prefix(self) -> str:
        if self._instance_prefix is None:
            raise RuntimeError("Redis cache backend is not bound to a storage identity")
        return self._instance_prefix

    def _rk(self, namespace: Namespace[Any], key: str) -> str:
        return f"{self._scope_prefix()}:{namespace.name}:{key}"

    # ------------------------------------------------------------------
    # Single-key operations
    # ------------------------------------------------------------------

    @override
    async def get[T](self, namespace: Namespace[T], key: str) -> T | None:
        try:
            raw: str | bytes | None = await self._ensure_client().get(self._rk(namespace, key))
        except Exception:
            return None
        if raw is None:
            return None
        if isinstance(raw, str):
            raw = raw.encode()
        return namespace.loads(raw)

    @override
    async def set[T](self, entry: CacheEntry[T], ttl: int | None = None) -> None:
        namespace = entry.namespace
        rk = self._rk(namespace, entry.key)
        raw = namespace.dumps(entry.value)
        ex = ttl if ttl is not None else self._ttls[namespace.name]
        with contextlib.suppress(Exception):
            await self._ensure_client().set(rk, raw, ex=ex)

    @override
    async def delete(self, namespace: Namespace[Any], key: str) -> bool:
        # Deliberately unguarded: a dropped delete leaves a stale positive
        # entry readable until TTL, so the caller must see the failure.
        return await self._ensure_client().delete(self._rk(namespace, key)) > 0

    @override
    async def clear(self, namespace: Namespace[Any] | None = None) -> None:
        r = self._ensure_client()
        scope_prefix = self._scope_prefix()
        pattern = f"{scope_prefix}:{namespace.name}:*" if namespace is not None else f"{scope_prefix}:*"
        cursor = 0
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
    async def mget(self, *keys: tuple[Namespace[Any], str]) -> list[Any]:
        pipe = self._ensure_client().pipeline(transaction=False)
        for ns, key in keys:
            pipe.get(self._rk(ns, key))
        try:
            raws: list[str | bytes | None] = await pipe.execute()
        except Exception:
            return [None] * len(keys)
        # Mirrors ``get``: a client configured with decode_responses=True hands
        # back ``str``, which the deserialisers cannot consume.
        return [
            ns.loads(raw.encode() if isinstance(raw, str) else raw) if raw is not None else None
            for (ns, _), raw in zip(keys, raws, strict=True)
        ]

    @override
    async def mset(self, *entries: CacheEntry[Any]) -> None:
        pipe = self._ensure_client().pipeline(transaction=False)
        for ns, key, value in entries:
            rk = self._rk(ns, key)
            raw = ns.dumps(value)
            pipe.set(rk, raw, ex=self._ttls[ns.name])
        with contextlib.suppress(Exception):
            await pipe.execute()

    @override
    async def mdelete(self, *keys: tuple[Namespace[Any], str]) -> int:
        pipe = self._ensure_client().pipeline(transaction=False)
        for ns, key in keys:
            pipe.delete(self._rk(ns, key))
        # Unguarded for the same reason as ``delete``.
        results: list[int] = await pipe.execute()
        return sum(1 for r in results if r > 0)
