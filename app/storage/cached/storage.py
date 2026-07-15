import itertools
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator
from pathlib import PurePosixPath
from typing import Literal, final, override

from app.log import escape_tag

from ..abstract import AbstractStorage, BytesLike, FileInfo, PathLike
from .backend import CacheBackend
from .backend.memory import MemoryCacheBackend


@final
class CachedStorage(AbstractStorage):
    """Cache metadata and download queries of an :class:`AbstractStorage`.

    Wraps an existing storage backend and caches the results of
    ``exists``, ``is_file``, ``is_dir``, ``stat``, ``iterdir`` and
    ``download_stream`` via a pluggable :class:`~.backend.CacheBackend`.

    **Cross-caching** — each read operation that returns a positive result
    also backfills related caches so that subsequent queries for the same
    path can be served without hitting the underlying storage:

    - ``is_file`` → ``True``: also writes ``exists=True`` and ``is_dir=False``.
    - ``is_dir`` → ``True``: also writes ``exists=True`` and ``is_file=False``.
    - ``exists`` → ``False``: also writes ``is_file=False`` and ``is_dir=False``.
    - ``stat`` success: also writes ``exists=True``, ``is_file`` and ``is_dir``.
    - ``download_stream`` success: also writes ``exists=True``, ``is_file=True``
      and ``is_dir=False``.

    **Write-backfill** — after a successful write operation, semantically
    certain values are written into the cache immediately, avoiding a
    subsequent round-trip to the underlying storage:

    - ``upload_stream``: ``exists=True, is_file=True, is_dir=False`` plus the
      uploaded bytes cached for ``download_stream`` when ≤ threshold.
    - ``unlink`` / ``rmdir`` / ``delete``: ``exists=False, is_file=False,
      is_dir=False``.
    - ``move``: source → ``exists=False, is_file=False, is_dir=False``;
      destination → ``exists=True``, with ``is_file`` / ``is_dir`` inferred
      from the source's cached type when available.
    - ``copy``: destination → ``exists=True`` + type inferred from source cache.
    - ``mkdir``: ``exists=True, is_file=False, is_dir=True``.
    - ``rmtree``: all caches cleared (too broad to backfill precisely).

    Parameters
    ----------
    storage:
        The underlying storage to wrap.
    ttl:
        TTL (seconds) for cached entries.  Default 30 s.
    capacity:
        Maximum number of entries per cache.  Default 1000.
    download_cache_threshold:
        Maximum file size (bytes) to cache for ``download_stream``.
        ``None`` to disable caching.  Default 16 KB.
    cache:
        Cache backend.  Pass ``"memory"`` (default) for the built-in
        in-process :class:`~.backend.MemoryCacheBackend`, or any
        :class:`~.backend.CacheBackend` instance (e.g. to use Redis).
    """

    def __init__(
        self,
        storage: AbstractStorage,
        *,
        ttl: int = 30,
        capacity: int = 1000,
        download_cache_threshold: int | None = 16 * 1024,  # 16 KB
        cache: Literal["memory"] | CacheBackend = "memory",
    ) -> None:
        super().__init__()
        self._storage = storage
        self._ttl = ttl
        self._capacity = capacity
        self._download_cache_threshold = download_cache_threshold
        if download_cache_threshold is not None and download_cache_threshold < 0:
            raise ValueError("download_cache_threshold must be None or >= 0")

        if cache == "memory":
            self._cache = MemoryCacheBackend(capacity=capacity)
        else:
            self._cache = cache

        self._cache.bind_storage(self._storage.cache_identity)
        self._cache.configure_namespace("exists", ttl)
        self._cache.configure_namespace("is_file", ttl)
        self._cache.configure_namespace("is_dir", ttl)
        self._cache.configure_namespace("stat", ttl)
        self._cache.configure_namespace("iterdir", ttl)
        self._cache.configure_namespace("download", ttl * 2, capacity=capacity // 4)

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    @override
    def id(self) -> str:
        return self._storage.id

    @property
    @override
    def cache_identity(self) -> str | None:
        return self._storage.cache_identity

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @override
    async def connect(self) -> None:
        await self._cache.connect()
        await self._storage.connect()
        self.log.info(
            f"Connected (backend=<le>{type(self._cache).__name__}</>, "
            f"ttl=<g>{self._ttl}s</g>, capacity=<g>{self._capacity}</g>)"
        )

    @override
    async def close(self) -> None:
        await self._storage.close()
        await self._cache.close()
        self.log.debug("Disconnected")

    @override
    async def ping(self) -> bool:
        return await self._cache.ping() and await self._storage.ping()

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize(path: PathLike) -> str:
        """Normalise *path* into a cache key.

        Strips leading ``/`` and collapses ``"."`` to ``""``,
        consistent with :meth:`S3Storage._remote_path_to_key`.
        """
        p = PurePosixPath(path)
        if p.is_absolute():
            p = p.relative_to("/")
        result = str(p)
        return "" if result == "." else result

    @staticmethod
    def _parent(normalized: str) -> str:
        """Return the normalised parent directory of *normalized*."""
        if not normalized:
            return ""
        parent = str(PurePosixPath(normalized).parent)
        return "" if parent == "." else parent

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    async def _invalidate_path(
        self,
        path: PathLike,
        *,
        exists: bool | None = None,
        is_file: bool | None = None,
        is_dir: bool | None = None,
        download: bytes | None = None,
    ) -> None:
        """Remove all cached entries for *path* and its parent's ``iterdir``.

        Optionally backfill known post-write values.  Pass ``None`` (default)
        for any field whose value is uncertain -- it will not be written.
        """
        np = self._normalize(path)

        # Batch delete all 6 caches for this path
        deleted = await self._cache.mdelete(
            ("exists", np),
            ("is_file", np),
            ("is_dir", np),
            ("stat", np),
            ("iterdir", self._parent(np)),
            ("download", np),
        )
        removed = deleted > 0

        # Batch backfill
        entries: list[tuple[str, str, object]] = []
        if exists is not None:
            entries.append(("exists", np, exists))
        if is_file is not None:
            entries.append(("is_file", np, is_file))
        if is_dir is not None:
            entries.append(("is_dir", np, is_dir))
        if download is not None:
            entries.append(("download", np, download))
        if entries:
            await self._cache.mset(*entries)
            backfilled = True
        else:
            backfilled = False

        if backfilled:
            self.log.debug(f"Cache backfilled: <y>{escape_tag(np)}</y>")
        elif removed:
            self.log.debug(f"Cache invalidated: <y>{escape_tag(np)}</y>")

    async def _clear_all_caches(self) -> None:
        await self._cache.clear()
        self.log.debug("All caches cleared")

    # ------------------------------------------------------------------
    # Introspection (for testing)
    # ------------------------------------------------------------------

    def dump_cache(self) -> dict[str, dict[str, object]]:
        """Return a complete snapshot of all cache namespaces.

        Returns ``{namespace: {key: value}}``.  Intended for debugging
        and tests; not part of the storage API contract.
        """
        return self._cache.snapshot()

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        buffer = bytearray() if self._download_cache_threshold is not None else None
        threshold = self._download_cache_threshold or 0

        async def _tracked_stream() -> AsyncGenerator[BytesLike]:
            nonlocal buffer
            async for chunk in stream:
                if buffer is not None:
                    buffer.extend(chunk)
                    if len(buffer) > threshold:
                        buffer = None
                yield chunk

        await self._storage.upload_stream(_tracked_stream(), remote_path, overwrite=overwrite)

        await self._invalidate_path(
            remote_path,
            exists=True,
            is_file=True,
            is_dir=False,
            download=bytes(buffer) if buffer is not None else None,
        )

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    @override
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncIterator[bytes]:
        np = self._normalize(remote_path)

        # 缓存中存储的是一定是完整文件内容 → 切片后即可服务 Range 请求
        if self._download_cache_threshold is not None and (cached := await self._cache.get("download", np)) is not None:
            length = len(cached)
            self.log.trace(f"Cache hit: <le>download_stream</>(<y>{escape_tag(np)}</y>) → <g>{length} bytes</g>")
            if offset:
                yield cached[offset:]
            else:
                yield cached
            return

        # offset > 0 时不写入缓存（下载的是片段）
        buffer = bytearray() if (offset == 0 and self._download_cache_threshold is not None) else None
        threshold = self._download_cache_threshold or 0
        async for chunk in self._storage.download_stream(remote_path, offset=offset):
            if buffer is not None:
                buffer.extend(chunk)
                if len(buffer) > threshold:
                    buffer = None
            yield chunk

        # Backfill metadata（无论 offset 都写）
        await self._cache.mset(
            ("exists", np, True),
            ("is_file", np, True),
            ("is_dir", np, False),
        )

        if buffer is not None:
            await self._cache.set("download", np, bytes(buffer))
            self.log.debug(f"Cached: <le>download_stream</>(<y>{escape_tag(np)}</y>) → <g>{len(buffer)} bytes</g>")

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    @override
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        await self._storage.unlink(path, missing_ok=missing_ok)
        await self._invalidate_path(path, exists=False, is_file=False, is_dir=False)

    @override
    async def rmdir(self, path: PathLike) -> None:
        await self._storage.rmdir(path)
        await self._invalidate_path(path, exists=False, is_file=False, is_dir=False)

    @override
    async def delete(self, path: PathLike) -> None:
        await self._storage.delete(path)
        await self._invalidate_path(path, exists=False, is_file=False, is_dir=False)

    @override
    async def delete_many(self, *paths: PathLike) -> None:
        try:
            await self._storage.delete_many(*paths)
        finally:
            for path in paths:
                await self._invalidate_path(path)

    @override
    async def move(self, src: PathLike, dst: PathLike) -> None:
        await self._storage.move(src, dst)

        # Attempt to infer dst type from src cache (may be expired -> None)
        src_np = self._normalize(src)
        [dst_is_file, dst_is_dir] = await self._cache.mget(
            ("is_file", src_np),
            ("is_dir", src_np),
        )

        await self._invalidate_path(src, exists=False, is_file=False, is_dir=False)
        await self._invalidate_path(dst, exists=True, is_file=dst_is_file, is_dir=dst_is_dir)

    @override
    async def copy(self, src: PathLike, dst: PathLike) -> None:
        await self._storage.copy(src, dst)

        # Attempt to infer dst type from src cache (may be expired -> None)
        src_np = self._normalize(src)
        [dst_is_file, dst_is_dir] = await self._cache.mget(
            ("is_file", src_np),
            ("is_dir", src_np),
        )

        await self._invalidate_path(dst, exists=True, is_file=dst_is_file, is_dir=dst_is_dir)

    # ------------------------------------------------------------------
    # Directory
    # ------------------------------------------------------------------

    @override
    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        await self._storage.mkdir(path, parents=parents, exist_ok=exist_ok)
        if parents:
            np = self._normalize(path)
            parts = PurePosixPath(np).parts if np else ()
            for i in range(len(parts) + 1):
                ancestor = str(PurePosixPath(*parts[:i])) if i > 0 else ""
                if i == len(parts):
                    # Final target: backfill known directory state
                    await self._invalidate_path(ancestor, exists=True, is_file=False, is_dir=True)
                else:
                    # Intermediate ancestors: pure invalidation (can't infer full listing)
                    await self._invalidate_path(ancestor)
        else:
            await self._invalidate_path(path, exists=True, is_file=False, is_dir=True)

    @override
    async def rmtree(self, path: PathLike) -> None:
        await self._storage.rmtree(path)
        self.log.debug("RmTree — clearing all caches")
        await self._clear_all_caches()

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        await self._storage.copytree(src, dst, overwrite=overwrite)
        self.log.debug("CopyTree — clearing all caches")
        await self._clear_all_caches()

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        await self._storage.movetree(src, dst, overwrite=overwrite)
        self.log.debug("MoveTree — clearing all caches")
        await self._clear_all_caches()

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @override
    async def exists(self, path: PathLike) -> bool:
        np = self._normalize(path)
        cached: bool | None = await self._cache.get("exists", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>exists</>(<y>{escape_tag(np)}</y>) = <g>{cached}</g>")
            return cached
        result = await self._storage.exists(path)
        if result:
            await self._cache.set("exists", np, True)
        else:
            # Does not exist → definitely not a file or directory either
            await self._cache.mset(
                ("exists", np, False),
                ("is_file", np, False),
                ("is_dir", np, False),
            )
        self.log.debug(f"Cache miss: <le>exists</>(<y>{escape_tag(np)}</y>) = <g>{result}</g>")
        return result

    @override
    async def is_file(self, path: PathLike) -> bool:
        np = self._normalize(path)
        cached: bool | None = await self._cache.get("is_file", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>is_file</>(<y>{escape_tag(np)}</y>) = <g>{cached}</g>")
            return cached
        result = await self._storage.is_file(path)
        if result:
            # Is a file → exists and is not a directory
            await self._cache.mset(
                ("is_file", np, True),
                ("exists", np, True),
                ("is_dir", np, False),
            )
        else:
            await self._cache.set("is_file", np, False)
        self.log.debug(f"Cache miss: <le>is_file</>(<y>{escape_tag(np)}</y>) = <g>{result}</g>")
        return result

    @override
    async def is_dir(self, path: PathLike) -> bool:
        np = self._normalize(path)
        cached: bool | None = await self._cache.get("is_dir", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>is_dir</>(<y>{escape_tag(np)}</y>) = <g>{cached}</g>")
            return cached
        result = await self._storage.is_dir(path)
        if result:
            # Is a directory → exists and is not a file
            await self._cache.mset(
                ("is_dir", np, True),
                ("exists", np, True),
                ("is_file", np, False),
            )
        else:
            await self._cache.set("is_dir", np, False)
        self.log.debug(f"Cache miss: <le>is_dir</>(<y>{escape_tag(np)}</y>) = <g>{result}</g>")
        return result

    @staticmethod
    def _info_to_cache_entries(np: str, info: FileInfo) -> list[tuple[str, str, object]]:
        return [
            ("stat", np, info),
            ("exists", np, True),
            ("is_file", np, not info.is_dir),
            ("is_dir", np, info.is_dir),
        ]

    @override
    async def stat(self, path: PathLike) -> FileInfo:
        np = self._normalize(path)
        cached: FileInfo | None = await self._cache.get("stat", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>stat</>(<y>{escape_tag(np)}</y>)")
            return cached
        result = await self._storage.stat(path)  # may raise FileNotFoundError
        # stat is the most complete metadata source
        await self._cache.mset(*self._info_to_cache_entries(np, result))
        self.log.debug(f"Cache miss: <le>stat</>(<y>{escape_tag(np)}</y>)")
        return result

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    @override
    async def iterdir(self, path: PathLike) -> AsyncIterator[FileInfo]:
        np = self._normalize(path)
        cached: list[FileInfo] | None = await self._cache.get("iterdir", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>iterdir</>(<y>{escape_tag(np)}</y>) → <g>{len(cached)}</g> entries")
            for info in cached:
                yield info
            return
        entries: list[FileInfo] = []
        async for info in self._storage.iterdir(path):
            entries.append(info)
            await self._cache.mset(*self._info_to_cache_entries(self._normalize(info.path), info))
            yield info
        await self._cache.set("iterdir", np, entries.copy())
        self.log.debug(f"Cache miss: <le>iterdir</>(<y>{escape_tag(np)}</y>) → <g>{len(entries)}</g> entries")

    @override
    async def walk(self, path: PathLike) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
        dirs: list[FileInfo] = []
        files: list[FileInfo] = []
        async for entry in self.iterdir(path):
            (dirs if entry.is_dir else files).append(entry)
        yield self.normalize_path(path).as_posix(), dirs, files
        for d in dirs:
            async for result in self.walk(d.path):
                yield result

    @override
    async def list_(self, path: PathLike) -> list[FileInfo]:
        infos: list[FileInfo] = await self._storage.list_(path)
        await self._cache.mset(
            *itertools.chain.from_iterable(
                self._info_to_cache_entries(self._normalize(entry.path), entry) for entry in infos
            )
        )
        return infos
