from typing import Any, final, override

from expiringdictx import ExpiringDict

from .base import CacheBackend, Namespace


@final
class MemoryCacheBackend(CacheBackend):
    """In-memory cache backend backed by :class:`~expiringdictx.ExpiringDict`.

    All methods are ``async`` in signature but execute synchronously
    (there is no I/O).

    Values are held as live objects, so the namespace codecs are unused here;
    only the namespace name is needed to select a cache.

    Namespace configuration and cache objects survive :meth:`close`. Closing
    only clears each namespace's contents so a later reconnect reuses the same
    configured namespaces without stale values.

    Parameters
    ----------
    capacity:
        Default max entries per namespace when not overridden via
        ``configure_namespace``.
    """

    def __init__(self, capacity: int = 1000) -> None:
        super().__init__()
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._default_capacity = capacity
        self._caches: dict[str, ExpiringDict[str, Any]] = {}
        self._namespace_configs: dict[str, tuple[int, int]] = {}

    # ------------------------------------------------------------------
    # Namespace configuration
    # ------------------------------------------------------------------

    @override
    def configure_namespace(self, namespace: Namespace[Any], ttl: int, **opts: Any) -> None:
        if ttl <= 0:
            raise ValueError("ttl must be > 0")
        capacity = opts.get("capacity", self._default_capacity)
        if not isinstance(capacity, int) or isinstance(capacity, bool):
            raise TypeError("capacity must be an int")
        if capacity < 1:
            raise ValueError("capacity must be >= 1")

        name = namespace.name
        config = (ttl, capacity)
        existing = self._namespace_configs.get(name)
        if existing is not None:
            if existing == config:
                return
            raise ValueError(
                f"namespace {name!r} already configured with ttl={existing[0]}, "
                f"capacity={existing[1]}; cannot reconfigure with ttl={ttl}, capacity={capacity}"
            )

        self._namespace_configs[name] = config
        self._caches[name] = ExpiringDict(capacity=capacity, default_age=ttl)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @override
    async def connect(self) -> None:
        pass  # no persistent connection to open

    @override
    async def close(self) -> None:
        # Preserve namespace configuration and objects; drop only values.
        for cache in self._caches.values():
            cache.clear()

    @override
    async def ping(self) -> bool:
        return True  # always alive

    # ------------------------------------------------------------------
    # Single-key operations
    # ------------------------------------------------------------------

    @override
    async def get[T](self, namespace: Namespace[T], key: str) -> T | None:
        value: T | None = self._caches[namespace.name].get(key)
        return value

    @override
    async def set[T](
        self,
        namespace: Namespace[T],
        key: str,
        value: T,
        ttl: int | None = None,
    ) -> None:
        # ttl is ignored: ExpiringDict has per-dict default_age, not per-key
        self._caches[namespace.name][key] = value

    @override
    async def delete(self, namespace: Namespace[Any], key: str) -> bool:
        return self._caches[namespace.name].pop(key, None) is not None

    @override
    async def clear(self, namespace: Namespace[Any] | None = None) -> None:
        if namespace is None:
            for cache in self._caches.values():
                cache.clear()
        else:
            self._caches[namespace.name].clear()

    # ------------------------------------------------------------------
    # Batch / pipeline operations
    # ------------------------------------------------------------------

    @override
    async def mget(self, *keys: tuple[Namespace[Any], str]) -> list[Any]:
        return [self._caches[ns.name].get(key) for ns, key in keys]

    @override
    async def mset(self, *entries: tuple[Namespace[Any], str, Any]) -> None:
        for ns, key, value in entries:
            self._caches[ns.name][key] = value

    @override
    async def mdelete(self, *keys: tuple[Namespace[Any], str]) -> int:
        count = 0
        for ns, key in keys:
            if self._caches[ns.name].pop(key, None) is not None:
                count += 1
        return count

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @override
    def snapshot(self) -> dict[str, dict[str, object]]:
        """Return a complete snapshot of all namespaces."""
        return {ns: dict(cache.items()) for ns, cache in self._caches.items()}
