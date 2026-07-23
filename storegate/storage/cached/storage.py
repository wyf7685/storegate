from collections.abc import AsyncGenerator, AsyncIterable
from pathlib import PurePosixPath
from typing import Literal, final, override

import anyio

from storegate.log import escape_tag

from ..abstract import (
    AbstractStorage,
    BytesLike,
    EntryKind,
    FileInfo,
    PathLike,
    StorageCapabilities,
    VersionedBytes,
    WalkEntry,
    validate_download_offset,
)
from .backend import CacheBackend
from .backend.memory import MemoryCacheBackend


@final
class CachedStorage(AbstractStorage):
    """Cache metadata and downloads while preserving lexical symlink identity.

    Follow namespaces (``stat``, ``exists``, ``is_file``, ``is_dir`` and
    ``download``) are distinct from lexical namespaces (``lstat`` and
    ``is_symlink``). A lexical symlink may cache only its lexical metadata;
    follow results are always read directly from the wrapped storage so target
    mutations through another alias cannot leave a stale cached result.

    Directory discovery caches snapshots without changing their order. Regular
    files and directories cross-fill both lexical and follow metadata, while a
    discovered symlink fills only ``lstat`` and ``is_symlink``.

    Parameters
    ----------
    storage:
        The underlying storage to wrap.
    ttl:
        TTL (seconds) for cached entries. Must be ``> 0``. Default 30 s.
    capacity:
        Maximum number of entries per metadata cache. Must be ``>= 4`` so the
        download namespace can keep at least one entry. Default 1000.
    download_cache_threshold:
        Maximum file size (bytes) cached for ``download_stream``. ``None``
        disables download caching; otherwise must be ``>= 0``. Default 16 KiB.
    cache:
        Cache backend, or ``"memory"`` for the built-in in-process backend.
    """

    def __init__(
        self,
        storage: AbstractStorage,
        *,
        ttl: int = 30,
        capacity: int = 1000,
        download_cache_threshold: int | None = 16 * 1024,
        cache: Literal["memory"] | CacheBackend = "memory",
    ) -> None:
        super().__init__()
        if ttl <= 0:
            raise ValueError("ttl must be > 0")
        if capacity < 4:
            raise ValueError("capacity must be >= 4")
        if download_cache_threshold is not None and download_cache_threshold < 0:
            raise ValueError("download_cache_threshold must be None or >= 0")

        download_capacity = capacity // 4
        if download_capacity < 1:
            raise ValueError("download namespace capacity must be >= 1")

        self._storage = storage
        self._ttl = ttl
        self._capacity = capacity
        self._download_cache_threshold = download_cache_threshold

        self._cache: CacheBackend = MemoryCacheBackend(capacity=capacity) if cache == "memory" else cache
        self._cache.bind_storage(self._storage.namespace_identity)
        self._cache.configure_namespace("exists", ttl)
        self._cache.configure_namespace("is_file", ttl)
        self._cache.configure_namespace("is_dir", ttl)
        self._cache.configure_namespace("is_symlink", ttl)
        self._cache.configure_namespace("stat", ttl)
        self._cache.configure_namespace("lstat", ttl)
        self._cache.configure_namespace("iterdir", ttl)
        self._cache.configure_namespace("download", ttl * 2, capacity=download_capacity)

    @property
    @override
    def display_id(self) -> str:
        return self._storage.display_id

    @property
    @override
    def namespace_identity(self) -> str:
        return self._storage.namespace_identity

    @property
    @override
    def capabilities(self) -> StorageCapabilities:
        # Proxy CAS only when the wrapped storage implements it; otherwise keep
        # the existing capability object so non-CAS backends stay allocation-free.
        return self._storage.capabilities

    @override
    async def connect(self) -> None:
        await self._cache.connect()
        try:
            await self._storage.connect()
        except BaseException as primary:
            cleanup_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                try:
                    await self._cache.close()
                except BaseException as secondary:
                    cleanup_error = secondary
            if cleanup_error is not None:
                raise BaseExceptionGroup(
                    "Cached storage connection rollback failed", [primary, cleanup_error]
                ) from None
            raise
        self.log.info(
            f"Connected (backend=<le>{type(self._cache).__name__}</>, "
            f"ttl=<g>{self._ttl}s</g>, capacity=<g>{self._capacity}</g>)"
        )

    @override
    async def close(self) -> None:
        try:
            await self._storage.close()
        finally:
            await self._cache.close()
        self.log.debug("Disconnected")

    @override
    async def ping(self) -> bool:
        return await self._cache.ping() and await self._storage.ping()

    @override
    async def read_versioned(self, path: PathLike) -> VersionedBytes | None:
        # Validate before any wrapped CAS I/O; keep the original path for
        # backend semantics after normalization succeeds.
        self._normalize(path)
        return await self._storage.read_versioned(path)

    @override
    async def compare_exchange(
        self,
        path: PathLike,
        *,
        expected_token: str | None,
        data: BytesLike,
    ) -> VersionedBytes | None:
        # Validate before any wrapped CAS I/O so invalid paths never mutate.
        self._normalize(path)
        result = await self._storage.compare_exchange(
            path,
            expected_token=expected_token,
            data=data,
        )
        if result is not None:
            await self._invalidate_path(
                path,
                exists=True,
                is_file=True,
                is_dir=False,
                is_symlink=False,
                download=result.data
                if self._download_cache_threshold is not None and len(result.data) <= self._download_cache_threshold
                else None,
            )
        return result

    @staticmethod
    def _normalize(path: PathLike) -> str:
        # Validate caller-supplied logical paths before any cache key derivation.
        absolute = AbstractStorage.normalize_path(path)
        relative = absolute.relative_to("/")
        result = relative.as_posix()
        return "" if result == "." else result

    @staticmethod
    def _parent(normalized: str) -> str:
        if not normalized:
            return ""
        parent = str(PurePosixPath(normalized).parent)
        return "" if parent == "." else parent

    @staticmethod
    def _lexical_cache_entries(np: str, info: FileInfo) -> list[tuple[str, str, object]]:
        if info.kind is EntryKind.SYMLINK:
            return [("lstat", np, info), ("is_symlink", np, True)]
        return [
            ("lstat", np, info),
            ("stat", np, info),
            ("is_symlink", np, False),
            ("exists", np, True),
            ("is_file", np, info.kind is EntryKind.FILE),
            ("is_dir", np, info.kind is EntryKind.DIRECTORY),
        ]

    @staticmethod
    def _follow_cache_entries(np: str, info: FileInfo) -> list[tuple[str, str, object]]:
        return [
            ("stat", np, info),
            ("exists", np, True),
            ("is_file", np, info.kind is EntryKind.FILE),
            ("is_dir", np, info.kind is EntryKind.DIRECTORY),
        ]

    async def _cache_lexical_info(self, info: FileInfo) -> None:
        await self._cache.mset(*self._lexical_cache_entries(self._normalize(info.path), info))

    async def _cache_lexical_missing(self, np: str) -> None:
        await self._cache.mset(
            ("is_symlink", np, False),
            ("exists", np, False),
            ("is_file", np, False),
            ("is_dir", np, False),
        )

    async def _is_lexical_symlink(self, path: PathLike, np: str) -> bool:
        cached: bool | None = await self._cache.get("is_symlink", np)
        if cached is not None:
            return cached
        try:
            info = await self._storage.lstat(path)
        except FileNotFoundError:
            await self._cache_lexical_missing(np)
            return False
        await self._cache.mset(*self._lexical_cache_entries(np, info))
        return info.kind is EntryKind.SYMLINK

    async def _invalidate_path(
        self,
        path: PathLike,
        *,
        exists: bool | None = None,
        is_file: bool | None = None,
        is_dir: bool | None = None,
        is_symlink: bool | None = None,
        download: bytes | None = None,
    ) -> None:
        """Invalidate a path, its listing, and its parent listing.

        Optional values are post-operation facts. ``None`` means the value is
        uncertain and must remain absent rather than being inferred.
        """
        np = self._normalize(path)
        parent = self._parent(np)
        keys = [
            ("exists", np),
            ("is_file", np),
            ("is_dir", np),
            ("is_symlink", np),
            ("stat", np),
            ("lstat", np),
            ("download", np),
            ("iterdir", np),
            ("iterdir", parent),
        ]
        deleted = await self._cache.mdelete(*keys)

        entries: list[tuple[str, str, object]] = []
        if exists is not None:
            entries.append(("exists", np, exists))
        if is_file is not None:
            entries.append(("is_file", np, is_file))
        if is_dir is not None:
            entries.append(("is_dir", np, is_dir))
        if is_symlink is not None:
            entries.append(("is_symlink", np, is_symlink))
        if download is not None:
            entries.append(("download", np, download))
        if entries:
            await self._cache.mset(*entries)
            self.log.debug(f"Cache backfilled: <y>{escape_tag(np)}</y>")
        elif deleted > 0:
            self.log.debug(f"Cache invalidated: <y>{escape_tag(np)}</y>")

    async def _invalidate_destination_ancestry(self, path: PathLike) -> None:
        """Invalidate every destination ancestor and listing it can change."""
        np = self._normalize(path)
        parts = PurePosixPath(np).parts if np else ()
        keys: list[tuple[str, str]] = []
        for index in range(len(parts) + 1):
            ancestor = str(PurePosixPath(*parts[:index])) if index else ""
            keys.extend(
                (
                    ("exists", ancestor),
                    ("is_file", ancestor),
                    ("is_dir", ancestor),
                    ("is_symlink", ancestor),
                    ("stat", ancestor),
                    ("lstat", ancestor),
                    ("download", ancestor),
                    ("iterdir", ancestor),
                )
            )
        if await self._cache.mdelete(*keys) > 0:
            self.log.debug(f"Destination ancestry invalidated: <y>{escape_tag(np)}</y>")

    async def _invalidate_as_missing(self, path: PathLike) -> None:
        await self._invalidate_path(
            path,
            exists=False,
            is_file=False,
            is_dir=False,
            is_symlink=False,
        )

    async def _clear_all_caches(self) -> None:
        await self._cache.clear()
        self.log.debug("All caches cleared")

    def dump_cache(self) -> dict[str, dict[str, object]]:
        """Return ``{namespace: {key: value}}`` for debugging and tests."""
        return self._cache.snapshot()

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        remote_path = self.normalize_path(remote_path)
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
            is_symlink=False,
            download=bytes(buffer) if buffer is not None else None,
        )

    @override
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
        offset = validate_download_offset(offset)
        remote_path = self.normalize_path(remote_path)
        np = self._normalize(remote_path)
        if await self._is_lexical_symlink(remote_path, np):
            async for chunk in self._storage.download_stream(remote_path, offset=offset):
                yield chunk
            return

        if self._download_cache_threshold is not None and (cached := await self._cache.get("download", np)) is not None:
            self.log.trace(f"Cache hit: <le>download_stream</>(<y>{escape_tag(np)}</y>) → <g>{len(cached)} bytes</g>")
            if offset >= len(cached):
                return
            yield cached[offset:] if offset else cached
            return

        buffer = bytearray() if (offset == 0 and self._download_cache_threshold is not None) else None
        threshold = self._download_cache_threshold or 0
        async for chunk in self._storage.download_stream(remote_path, offset=offset):
            if buffer is not None:
                buffer.extend(chunk)
                if len(buffer) > threshold:
                    buffer = None
            yield chunk

        await self._cache.mset(
            ("exists", np, True),
            ("is_file", np, True),
            ("is_dir", np, False),
        )
        if buffer is not None:
            await self._cache.set("download", np, bytes(buffer))
            self.log.debug(f"Cached: <le>download_stream</>(<y>{escape_tag(np)}</y>) → <g>{len(buffer)} bytes</g>")

    @override
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        path = self.normalize_path(path)
        await self._storage.unlink(path, missing_ok=missing_ok)
        await self._invalidate_as_missing(path)

    @override
    async def rmdir(self, path: PathLike) -> None:
        path = self.normalize_path(path)
        await self._storage.rmdir(path)
        await self._invalidate_as_missing(path)

    @override
    async def delete(self, path: PathLike) -> None:
        path = self.normalize_path(path)
        await self._storage.delete(path)
        await self._invalidate_as_missing(path)

    @override
    async def delete_many(self, *paths: PathLike) -> None:
        normalized = tuple(self.normalize_path(path) for path in paths)
        try:
            await self._storage.delete_many(*normalized)
        finally:
            for path in normalized:
                await self._invalidate_path(path)

    async def _backfill_copied_kind(self, path: PathLike, kind: EntryKind) -> None:
        np = self._normalize(path)
        if kind is EntryKind.SYMLINK:
            await self._cache.set("is_symlink", np, True)
            return
        await self._cache.mset(
            ("exists", np, True),
            ("is_file", np, kind is EntryKind.FILE),
            ("is_dir", np, kind is EntryKind.DIRECTORY),
            ("is_symlink", np, False),
        )

    @override
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        src_np = self._normalize(src)
        dst_np = self._normalize(dst)
        source = await self.lstat(src)
        try:
            await self._storage.move(src, dst, overwrite=overwrite)
        except BaseException:
            await self._invalidate_path(src)
            if dst_np != src_np:
                await self._invalidate_destination_ancestry(dst)
            raise
        if dst_np == src_np:
            await self._invalidate_path(src)
            return
        await self._invalidate_as_missing(src)
        await self._invalidate_destination_ancestry(dst)
        await self._backfill_copied_kind(dst, source.kind)

    @override
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        source = await self.lstat(src)
        try:
            await self._storage.copy(src, dst, overwrite=overwrite)
        except BaseException:
            await self._invalidate_destination_ancestry(dst)
            raise
        await self._invalidate_destination_ancestry(dst)
        await self._backfill_copied_kind(dst, source.kind)

    @override
    async def symlink(
        self,
        target: PathLike,
        link_path: PathLike,
        *,
        target_is_directory: bool = False,
        overwrite: bool = False,
    ) -> None:
        link_path = self.normalize_path(link_path)
        try:
            await self._storage.symlink(
                target,
                link_path,
                target_is_directory=target_is_directory,
                overwrite=overwrite,
            )
        except BaseException:
            await self._invalidate_path(link_path)
            raise
        await self._invalidate_path(link_path, is_symlink=True)

    @override
    async def readlink(self, path: PathLike) -> str:
        path = self.normalize_path(path)
        return await self._storage.readlink(path)

    @override
    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        path = self.normalize_path(path)
        await self._storage.mkdir(path, parents=parents, exist_ok=exist_ok)
        if parents:
            np = self._normalize(path)
            parts = PurePosixPath(np).parts if np else ()
            for index in range(len(parts) + 1):
                ancestor = str(PurePosixPath(*parts[:index])) if index else ""
                if index == len(parts):
                    await self._invalidate_path(
                        ancestor,
                        exists=True,
                        is_file=False,
                        is_dir=True,
                        is_symlink=False,
                    )
                else:
                    await self._invalidate_path(ancestor)
        else:
            await self._invalidate_path(
                path,
                exists=True,
                is_file=False,
                is_dir=True,
                is_symlink=False,
            )

    @override
    async def rmtree(self, path: PathLike) -> None:
        path = self.normalize_path(path)
        try:
            await self._storage.rmtree(path)
        finally:
            await self._clear_all_caches()

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        try:
            await self._storage.copytree(src, dst, overwrite=overwrite)
        finally:
            await self._clear_all_caches()

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src = self.normalize_path(src)
        dst = self.normalize_path(dst)
        try:
            await self._storage.movetree(src, dst, overwrite=overwrite)
        finally:
            await self._clear_all_caches()

    @override
    async def exists(self, path: PathLike) -> bool:
        np = self._normalize(path)
        if await self._is_lexical_symlink(path, np):
            return await self._storage.exists(path)
        cached: bool | None = await self._cache.get("exists", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>exists</>(<y>{escape_tag(np)}</y>) = <g>{cached}</g>")
            return cached
        result = await self._storage.exists(path)
        await self._cache.set("exists", np, result)
        self.log.debug(f"Cache miss: <le>exists</>(<y>{escape_tag(np)}</y>) = <g>{result}</g>")
        return result

    @override
    async def is_file(self, path: PathLike) -> bool:
        np = self._normalize(path)
        if await self._is_lexical_symlink(path, np):
            return await self._storage.is_file(path)
        cached: bool | None = await self._cache.get("is_file", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>is_file</>(<y>{escape_tag(np)}</y>) = <g>{cached}</g>")
            return cached
        result = await self._storage.is_file(path)
        if result:
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
        if await self._is_lexical_symlink(path, np):
            return await self._storage.is_dir(path)
        cached: bool | None = await self._cache.get("is_dir", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>is_dir</>(<y>{escape_tag(np)}</y>) = <g>{cached}</g>")
            return cached
        result = await self._storage.is_dir(path)
        if result:
            await self._cache.mset(
                ("is_dir", np, True),
                ("exists", np, True),
                ("is_file", np, False),
            )
        else:
            await self._cache.set("is_dir", np, False)
        self.log.debug(f"Cache miss: <le>is_dir</>(<y>{escape_tag(np)}</y>) = <g>{result}</g>")
        return result

    @override
    async def is_symlink(self, path: PathLike) -> bool:
        np = self._normalize(path)
        cached: bool | None = await self._cache.get("is_symlink", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>is_symlink</>(<y>{escape_tag(np)}</y>) = <g>{cached}</g>")
            return cached
        try:
            info = await self.lstat(path)
        except FileNotFoundError:
            return False
        return info.kind is EntryKind.SYMLINK

    @override
    async def stat(self, path: PathLike) -> FileInfo:
        np = self._normalize(path)
        if await self._is_lexical_symlink(path, np):
            return await self._storage.stat(path)
        cached: FileInfo | None = await self._cache.get("stat", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>stat</>(<y>{escape_tag(np)}</y>)")
            return cached
        result = await self._storage.stat(path)
        await self._cache.mset(*self._follow_cache_entries(np, result))
        self.log.debug(f"Cache miss: <le>stat</>(<y>{escape_tag(np)}</y>)")
        return result

    @override
    async def lstat(self, path: PathLike) -> FileInfo:
        np = self._normalize(path)
        cached: FileInfo | None = await self._cache.get("lstat", np)
        if cached is not None:
            self.log.trace(f"Cache hit: <le>lstat</>(<y>{escape_tag(np)}</y>)")
            return cached
        try:
            result = await self._storage.lstat(path)
        except FileNotFoundError:
            await self._cache_lexical_missing(np)
            raise
        await self._cache.mset(*self._lexical_cache_entries(np, result))
        self.log.debug(f"Cache miss: <le>lstat</>(<y>{escape_tag(np)}</y>)")
        return result

    async def _backfill_discovery(self, infos: tuple[FileInfo, ...] | list[FileInfo]) -> None:
        entries: list[tuple[str, str, object]] = []
        for info in infos:
            entries.extend(self._lexical_cache_entries(self._normalize(info.path), info))
        if entries:
            await self._cache.mset(*entries)

    @override
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        np = self._normalize(path)
        cached: list[FileInfo] | None = await self._cache.get("iterdir", np)
        if cached is not None:
            await self._backfill_discovery(cached)
            self.log.trace(f"Cache hit: <le>iterdir</>(<y>{escape_tag(np)}</y>) → <g>{len(cached)}</g> entries")
            for info in cached:
                yield info
            return

        entries: list[FileInfo] = []
        async for info in self._storage.iterdir(path):
            entries.append(info)
            await self._cache.mset(*self._lexical_cache_entries(self._normalize(info.path), info))
            yield info
        await self._cache.set("iterdir", np, entries.copy())
        self.log.debug(f"Cache miss: <le>iterdir</>(<y>{escape_tag(np)}</y>) → <g>{len(entries)}</g> entries")

    @override
    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        path = self.normalize_path(path)
        async for snapshot in self._storage.walk(path):
            await self._backfill_discovery(snapshot.entries)
            yield snapshot

    @override
    async def list_(self, path: PathLike) -> list[FileInfo]:
        path = self.normalize_path(path)
        infos = await self._storage.list_(path)
        await self._backfill_discovery(infos)
        return infos
