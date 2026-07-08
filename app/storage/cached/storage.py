from collections.abc import AsyncIterable, AsyncIterator
from pathlib import PurePosixPath
from typing import final, override

from expiringdictx import ExpiringDict

from app.log import escape_tag

from ..abstract import AbstractStorage, BytesLike, FileInfo


@final
class CachedStorage(AbstractStorage):
    """Cache metadata queries of an :class:`AbstractStorage`.

    Wraps an existing storage backend and caches the results of
    ``exists``, ``is_file``, ``is_dir``, ``stat`` and ``iterdir``
    using :class:`~expiringdictx.ExpiringDict` instances.

    Parameters
    ----------
    storage:
        The underlying storage to wrap.
    ttl:
        TTL (seconds) for cached entries.  Default 5 s.
    capacity:
        Maximum number of entries per cache.  Default 500.
    download_cache_threshold:
        Minimum file size (bytes) to cache for ``download_stream``. None to disable caching.  Default 16 KB.
    """

    def __init__(
        self,
        storage: AbstractStorage,
        *,
        ttl: int = 30,
        capacity: int = 1000,
        download_cache_threshold: int | None = 16 * 1024,  # 16 KB
    ) -> None:
        super().__init__()
        self._storage = storage
        self._ttl = ttl
        self._capacity = capacity
        self._download_cache_threshold = download_cache_threshold
        if download_cache_threshold is not None and download_cache_threshold < 0:
            raise ValueError("download_cache_threshold must be None or >= 0")

        self._cache_exists: ExpiringDict[str, bool] = ExpiringDict(capacity=capacity, default_age=ttl)
        self._cache_is_file: ExpiringDict[str, bool] = ExpiringDict(capacity=capacity, default_age=ttl)
        self._cache_is_dir: ExpiringDict[str, bool] = ExpiringDict(capacity=capacity, default_age=ttl)
        self._cache_stat: ExpiringDict[str, FileInfo] = ExpiringDict(capacity=capacity, default_age=ttl)
        self._cache_iterdir: ExpiringDict[str, list[FileInfo]] = ExpiringDict(capacity=capacity, default_age=ttl)
        self._cache_download: ExpiringDict[str, bytes] = ExpiringDict(capacity=capacity // 4, default_age=ttl * 2)

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @override
    @property
    def id(self) -> str:
        return self._storage.id

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @override
    async def connect(self) -> None:
        await self._storage.connect()
        self.log.info(f"Connected (ttl=<g>{self._ttl}s</g>, capacity=<g>{self._capacity}</g>)")

    @override
    async def close(self) -> None:
        self._clear_all_caches()
        await self._storage.close()
        self.log.debug("Disconnected")

    @override
    async def ping(self) -> bool:
        return await self._storage.ping()

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize(path: str) -> str:
        """Normalise *path* into a cache key.

        Strips leading ``/`` and collapses ``"."`` to ``""``,
        consistent with :meth:`CosStorage._remote_path_to_key`.
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

    def _invalidate_path(self, path: str) -> None:
        """Remove all cached entries for *path* and its parent's ``iterdir``."""
        np = self._normalize(path)
        removed = False

        if self._cache_exists.pop(np, None) is not None:
            removed = True
        if self._cache_is_file.pop(np, None) is not None:
            removed = True
        if self._cache_is_dir.pop(np, None) is not None:
            removed = True
        if self._cache_stat.pop(np, None) is not None:
            removed = True
        parent = self._parent(np)
        if self._cache_iterdir.pop(parent, None) is not None:
            removed = True
        if self._cache_download.pop(np, None) is not None:
            removed = True

        if removed:
            self.log.debug(f"Cache invalidated: <y>{escape_tag(np)}</y>")

    def _clear_all_caches(self) -> None:
        self._cache_exists.clear()
        self._cache_is_file.clear()
        self._cache_is_dir.clear()
        self._cache_stat.clear()
        self._cache_iterdir.clear()
        self._cache_download.clear()
        self.log.debug("All caches cleared")

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: str,
        *,
        overwrite: bool = True,
    ) -> None:
        await self._storage.upload_stream(stream, remote_path, overwrite=overwrite)
        self._invalidate_path(remote_path)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    @override
    async def download_stream(
        self,
        remote_path: str,
    ) -> AsyncIterator[bytes]:
        if self._download_cache_threshold is not None and (cached := self._cache_download.get(remote_path)) is not None:
            self.log.trace(
                f"Cache hit: <le>download_stream</>(<y>{escape_tag(remote_path)}</y>) → <g>{len(cached)} bytes</g>"
            )
            yield cached
            return

        buffer = bytearray() if self._download_cache_threshold is not None else None
        threshold = self._download_cache_threshold or 0
        async for chunk in self._storage.download_stream(remote_path):
            if buffer is not None:
                buffer.extend(chunk)
                if len(buffer) > threshold:
                    buffer = None
            yield chunk

        if buffer is not None:
            self._cache_download[remote_path] = bytes(buffer)
            self.log.debug(
                f"Cached: <le>download_stream</>(<y>{escape_tag(remote_path)}</y>) → <g>{len(buffer)} bytes</g>"
            )

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    @override
    async def unlink(self, path: str, *, missing_ok: bool = False) -> None:
        await self._storage.unlink(path, missing_ok=missing_ok)
        self._invalidate_path(path)

    @override
    async def rmdir(self, path: str) -> None:
        await self._storage.rmdir(path)
        self._invalidate_path(path)

    @override
    async def delete(self, path: str) -> None:
        await self._storage.delete(path)
        self._invalidate_path(path)

    @override
    async def delete_many(self, *paths: str) -> None:
        await self._storage.delete_many(*paths)
        for path in paths:
            self._invalidate_path(path)

    @override
    async def move(self, src: str, dst: str) -> None:
        await self._storage.move(src, dst)
        self._invalidate_path(src)
        self._invalidate_path(dst)

    @override
    async def copy(self, src: str, dst: str) -> None:
        await self._storage.copy(src, dst)
        self._invalidate_path(dst)

    # ------------------------------------------------------------------
    # Directory
    # ------------------------------------------------------------------

    @override
    async def mkdir(
        self,
        path: str,
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
                self._invalidate_path(ancestor)
        else:
            self._invalidate_path(path)

    @override
    async def rmtree(self, path: str) -> None:
        await self._storage.rmtree(path)
        self.log.debug("RmTree — clearing all caches")
        self._clear_all_caches()

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @override
    async def exists(self, path: str) -> bool:
        np = self._normalize(path)
        cached: bool | None = self._cache_exists.get(np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>exists</>(<y>{escape_tag(np)}</y>) = <g>{cached}</g>")
            return cached
        result = await self._storage.exists(path)
        self._cache_exists[np] = result
        self.log.debug(f"Cache miss: <le>exists</>(<y>{escape_tag(np)}</y>) = <g>{result}</g>")
        return result

    @override
    async def is_file(self, path: str) -> bool:
        np = self._normalize(path)
        cached: bool | None = self._cache_is_file.get(np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>is_file</>(<y>{escape_tag(np)}</y>) = <g>{cached}</g>")
            return cached
        result = await self._storage.is_file(path)
        self._cache_is_file[np] = result
        self.log.debug(f"Cache miss: <le>is_file</>(<y>{escape_tag(np)}</y>) = <g>{result}</g>")
        return result

    @override
    async def is_dir(self, path: str) -> bool:
        np = self._normalize(path)
        cached: bool | None = self._cache_is_dir.get(np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>is_dir</>(<y>{escape_tag(np)}</y>) = <g>{cached}</g>")
            return cached
        result = await self._storage.is_dir(path)
        self._cache_is_dir[np] = result
        self.log.debug(f"Cache miss: <le>is_dir</>(<y>{escape_tag(np)}</y>) = <g>{result}</g>")
        return result

    @override
    async def stat(self, path: str) -> FileInfo:
        np = self._normalize(path)
        cached: FileInfo | None = self._cache_stat.get(np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>stat</>(<y>{escape_tag(np)}</y>)")
            return cached
        result = await self._storage.stat(path)  # may raise FileNotFoundError
        self._cache_stat[np] = result
        self.log.debug(f"Cache miss: <le>stat</>(<y>{escape_tag(np)}</y>)")
        return result

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    @override
    async def iterdir(self, path: str) -> AsyncIterator[FileInfo]:
        np = self._normalize(path)
        cached: list[FileInfo] | None = self._cache_iterdir.get(np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>iterdir</>(<y>{escape_tag(np)}</y>) → <g>{len(cached)}</g> entries")
            for entry in cached:
                yield entry
            return
        entries: list[FileInfo] = []
        async for entry in self._storage.iterdir(path):
            entries.append(entry)
            yield entry
        self._cache_iterdir[np] = entries.copy()
        self.log.debug(f"Cache miss: <le>iterdir</>(<y>{escape_tag(np)}</y>) → <g>{len(entries)}</g> entries")

    @override
    async def walk(self, path: str) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
        dirs: list[FileInfo] = []
        files: list[FileInfo] = []
        async for entry in self.iterdir(path):
            (dirs if entry.is_dir else files).append(entry)
        yield path, dirs, files
        for d in dirs:
            async for result in self.walk(d.path):
                yield result
