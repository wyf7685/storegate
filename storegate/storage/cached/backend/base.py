from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from storegate.storage.abstract import FileInfo

from .codec import (
    bool_from_bytes,
    bool_to_bytes,
    file_info_from_json,
    file_info_list_from_json,
    file_info_list_to_json,
    file_info_to_json,
    raw_bytes,
)


@dataclass(frozen=True, slots=True)
class Namespace[T]:
    """A cache namespace and the value type it stores.

    Addressing a namespace by this descriptor rather than by its bare name
    lets the type checker infer what :meth:`CacheBackend.get` returns, so a
    caller can no longer silently claim the wrong type for a cached value.
    The serialiser pair travels with the namespace as well, which keeps the
    wire format for a namespace defined in exactly one place.
    """

    name: str
    dumps: Callable[[T], bytes]
    loads: Callable[[bytes], T]


EXISTS = Namespace[bool]("exists", bool_to_bytes, bool_from_bytes)
IS_FILE = Namespace[bool]("is_file", bool_to_bytes, bool_from_bytes)
IS_DIR = Namespace[bool]("is_dir", bool_to_bytes, bool_from_bytes)
IS_SYMLINK = Namespace[bool]("is_symlink", bool_to_bytes, bool_from_bytes)
STAT = Namespace[FileInfo]("stat", file_info_to_json, file_info_from_json)
LSTAT = Namespace[FileInfo]("lstat", file_info_to_json, file_info_from_json)
ITERDIR = Namespace[list[FileInfo]]("iterdir", file_info_list_to_json, file_info_list_from_json)
DOWNLOAD = Namespace[bytes]("download", raw_bytes, raw_bytes)


class CacheBackend(ABC):
    """Abstract asynchronous cache backend for :class:`CachedStorage`.

    Each backend manages multiple *namespaces* (:data:`EXISTS`, :data:`STAT`,
    :data:`DOWNLOAD` and friends). Every operation is scoped to a single
    namespace, addressed by its :class:`Namespace` descriptor rather than by
    name so the value type survives into the caller.

    Implementations are free to choose their own storage engine and eviction
    policy. A backend that must serialise values uses the codec carried by the
    namespace, so the wire format for a namespace is the same everywhere.
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
    # Storage binding / namespace configuration
    # ------------------------------------------------------------------

    def bind_storage(self, identity: str | None) -> None:  # noqa: B027
        """Bind this backend to a storage's stable namespace identity.

        Backends that do not need persistent instance scoping may ignore this.
        """

    @abstractmethod
    def configure_namespace(self, namespace: Namespace[Any], ttl: int, **opts: Any) -> None:
        """Configure *namespace* with *ttl* (seconds) as its default TTL.

        Backend implementations may accept additional keyword arguments
        (e.g. ``capacity`` for :class:`MemoryCacheBackend`) and should
        ignore unknown options.

        Must be called before any data operation uses *namespace*. Repeated
        calls with identical configuration are idempotent; conflicting
        configuration for an existing namespace must fail explicitly.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Single-key operations
    # ------------------------------------------------------------------

    @abstractmethod
    async def get[T](self, namespace: Namespace[T], key: str) -> T | None:
        """Return the cached value for *key* in *namespace*, or ``None``.

        Returns ``None`` for a cache miss or expired entry --
        indistinguishable from a cached ``None``, so callers should
        not store ``None`` as a meaningful value.
        """
        raise NotImplementedError

    @abstractmethod
    async def set[T](
        self,
        namespace: Namespace[T],
        key: str,
        value: T,
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
    async def delete(self, namespace: Namespace[Any], key: str) -> bool:
        """Remove *key* from *namespace*.

        Returns ``True`` if the key existed and was removed, ``False``
        if it was not present.

        Backend failures must be raised, never swallowed. A silently dropped
        delete leaves a stale positive entry readable until its TTL expires,
        which is a correctness failure rather than a degraded cache. This is
        the opposite of :meth:`get` and :meth:`set`, where a failure only costs
        a miss and may safely degrade.
        """
        raise NotImplementedError

    @abstractmethod
    async def clear(self, namespace: Namespace[Any] | None = None) -> None:
        """Clear cached entries.

        - ``namespace is None`` -- clear **all** namespaces.
        - *namespace* is a :class:`Namespace` -- clear only that namespace.

        Backend failures must be raised, for the same reason as :meth:`delete`.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Batch / pipeline operations
    # ------------------------------------------------------------------

    @abstractmethod
    async def mget(self, *keys: tuple[Namespace[Any], str]) -> list[Any]:
        """Batch get.

        Each element of *keys* is a ``(namespace, key)`` pair.
        Results are returned in the same order as *keys*, with
        ``None`` for cache misses or expired entries.

        Unlike :meth:`get`, the result is not typed per entry: one call may
        span namespaces with different value types.
        """
        raise NotImplementedError

    @abstractmethod
    async def mset(self, *entries: tuple[Namespace[Any], str, Any]) -> None:
        """Batch set.

        Each element of *entries* is a ``(namespace, key, value)``
        triple.  All entries use their namespace's default TTL.
        """
        raise NotImplementedError

    @abstractmethod
    async def mdelete(self, *keys: tuple[Namespace[Any], str]) -> int:
        """Batch delete.

        Each element of *keys* is a ``(namespace, key)`` pair.
        Returns the total number of keys that existed and were removed.

        Backend failures must be raised, for the same reason as :meth:`delete`.
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
