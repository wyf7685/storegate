from abc import ABC, abstractmethod
from typing import Any


class CacheBackend(ABC):
    """Abstract asynchronous cache backend for :class:`CachedStorage`.

    Each backend manages multiple *namespaces* (e.g. ``"exists"``,
    ``"is_file"``, ``"download"``).  Every operation is scoped to a
    single namespace.

    Implementations are free to choose their own storage engine,
    eviction policy, and serialisation strategy.
    """

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """Open any persistent connections needed by the backend.

        Called by :meth:`CachedStorage.connect`.  Must be idempotent.
        """
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Release any resources held by the backend.

        Called by :meth:`CachedStorage.close`.
        """
        raise NotImplementedError

    @abstractmethod
    async def ping(self) -> bool:
        """Check whether the backend is healthy.

        Returns ``True`` if the backend is reachable and operational.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Namespace configuration
    # ------------------------------------------------------------------

    @abstractmethod
    def configure_namespace(self, name: str, ttl: int, **opts: Any) -> None:
        """Configure a namespace with *ttl* (seconds) as its default TTL.

        Backend implementations may accept additional keyword arguments
        (e.g. ``capacity`` for :class:`MemoryCacheBackend`) and should
        ignore unknown options.

        Must be called before any data operation uses *name*.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Single-key operations
    # ------------------------------------------------------------------

    @abstractmethod
    async def get(self, namespace: str, key: str) -> Any:
        """Return the cached value for *key* in *namespace*, or ``None``.

        Returns ``None`` for a cache miss or expired entry --
        indistinguishable from a cached ``None``, so callers should
        not store ``None`` as a meaningful value.
        """
        raise NotImplementedError

    @abstractmethod
    async def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> None:
        """Store *value* under *key* in *namespace*.

        Parameters
        ----------
        ttl:
            Per-entry TTL in seconds.  ``None`` means use the
            namespace default (implementation-defined).
        """
        raise NotImplementedError

    @abstractmethod
    async def delete(self, namespace: str, key: str) -> bool:
        """Remove *key* from *namespace*.

        Returns ``True`` if the key existed and was removed, ``False``
        if it was not present.
        """
        raise NotImplementedError

    @abstractmethod
    async def clear(self, namespace: str | None = None) -> None:
        """Clear cached entries.

        - ``namespace is None`` -- clear **all** namespaces.
        - *namespace* is a string -- clear only that namespace.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Batch / pipeline operations
    # ------------------------------------------------------------------

    @abstractmethod
    async def mget(self, *keys: tuple[str, str]) -> list[Any]:
        """Batch get.

        Each element of *keys* is a ``(namespace, key)`` pair.
        Results are returned in the same order as *keys*, with
        ``None`` for cache misses or expired entries.
        """
        raise NotImplementedError

    @abstractmethod
    async def mset(self, *entries: tuple[str, str, Any]) -> None:
        """Batch set.

        Each element of *entries* is a ``(namespace, key, value)``
        triple.  All entries use their namespace's default TTL.
        """
        raise NotImplementedError

    @abstractmethod
    async def mdelete(self, *keys: tuple[str, str]) -> int:
        """Batch delete.

        Each element of *keys* is a ``(namespace, key)`` pair.
        Returns the total number of keys that existed and were removed.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Introspection (optional — defaults return empty)
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, dict[str, object]]:
        """Return a complete snapshot of all namespaces for debugging/testing.

        Returns ``{namespace: {key: value}}``.  The default implementation
        returns an empty dict; backends that support introspection should
        override this method.
        """
        return {}
