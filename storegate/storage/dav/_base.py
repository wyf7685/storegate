from __future__ import annotations

import errno
from pathlib import Path, PurePosixPath
from typing import override
from urllib.parse import urlparse

import anyio

from storegate.log import escape_tag
from storegate.storage.abstract import (
    AbstractStorage,
    EntryKind,
    FileInfo,
    PathLike,
    StorageCapabilities,
    UnsupportedOperationError,
    WalkEntry,
    make_namespace_identity,
)
from storegate.utils import ExceptionTranslator

from .client import AsyncDavClient, DavClientError, DavConfig, DavHttpStatusError, DavResource
from .utils import dav_resource_to_file_info, href_to_storage_path

translator = ExceptionTranslator(
    bypass=OSError,
    catch=DavClientError,
    default=OSError,
)

_DAV_CAPABILITIES = StorageCapabilities()
_UNSUPPORTED_ERRNO = getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)
# Statuses that make a server-side Depth:infinity COPY/MOVE fall back to the
# explicit walk-and-copy path: 403/405/501 refuse the recursive operation and
# 409 reports a missing intermediate collection (RFC 4918 §9.8.5). The fallback
# creates every destination collection itself, so it resolves all four.
_RECURSIVE_OP_FALLBACK_STATUSES = frozenset({403, 405, 409, 501})


def _unsupported_entry(path: PathLike) -> UnsupportedOperationError:
    return UnsupportedOperationError(_UNSUPPORTED_ERRNO, f"Unsupported WebDAV resource: {path}")


@translator.handles(DavHttpStatusError)
def _(exc: DavHttpStatusError, msg: str) -> OSError:
    return {
        404: FileNotFoundError,
        403: PermissionError,
        412: FileExistsError,  # Overwrite: F precondition failed
        423: PermissionError,  # Locked
    }.get(exc.status_code, OSError)(f"{msg}: {exc}")


class DavStorageBase(AbstractStorage):
    """State, lifecycle and shared primitives for the DavStorage mixins.

    Splitting the operation mixins out of ``DavStorage`` keeps each transaction
    in its own module; they cooperate only through the members defined here, so
    this class is the whole contract between them.
    """

    _client: AsyncDavClient | None = None
    _config: DavConfig

    def __init__(self, config: str | Path | DavConfig) -> None:
        super().__init__()
        self._config = config if isinstance(config, DavConfig) else DavConfig.from_file(config)

    @property
    @override
    def display_id(self) -> str:
        parsed = urlparse(self._config.base_url)
        host = parsed.hostname or ""
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        elif parsed.scheme == "http":
            host = f"{host}:80"
        elif parsed.scheme == "https":
            host = f"{host}:443"
        return f"dav:{host}{self._config.root_prefix}"

    @property
    @override
    def namespace_identity(self) -> str:
        parsed = urlparse(self._config.base_url)
        return make_namespace_identity(
            "dav",
            base_path=parsed.path,
            hostname=parsed.hostname,
            port=parsed.port,
            root_prefix=self._config.root_prefix,
            scheme=parsed.scheme,
        )

    @property
    @override
    def capabilities(self) -> StorageCapabilities:
        return _DAV_CAPABILITIES

    @property
    def _url_prefix(self) -> str:
        """Server path prefix shared by every href (base_url path + root_prefix)."""
        parsed = urlparse(self._config.base_url)
        return f"{parsed.path}{self._config.root_prefix}"

    def _remote_path(self, path: PathLike) -> str:
        np = self.normalize_path(path)
        rel = np.relative_to("/")
        return rel.as_posix() if rel != PurePosixPath(".") else ""

    def _ensure_client(self) -> AsyncDavClient:
        if self._client is None:
            raise RuntimeError("Client is not connected.")
        return self._client

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @override
    async def connect(self) -> None:
        if self._client is not None:
            retained = self._client
            with anyio.CancelScope(shield=True):
                await retained.__aexit__(None, None, None)
                if self._client is retained:
                    self._client = None
        client = AsyncDavClient(self._config)
        self._client = client
        try:
            await client.__aenter__()
            if not await self.ping():
                raise RuntimeError("Failed to connect to WebDAV server. Please check your configuration.")
        except BaseException as primary:
            cleanup_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                try:
                    await client.__aexit__(None, None, None)
                except BaseException as secondary:
                    cleanup_error = secondary
                else:
                    self._client = None
            if cleanup_error is not None:
                self._client = client
                raise BaseExceptionGroup("WebDAV connection rollback failed", [primary, cleanup_error]) from None
            raise
        self.log.info(f"Connected to <c>{escape_tag(self._config.base_url)}</c>")

    @override
    async def close(self) -> None:
        if (client := self._client) is not None:
            # Shielded so a cancellation delivered mid-``aclose`` cannot leave
            # ``_client`` pointing at a half-closed client and leak its sockets.
            # The client is retained on failure so the next ``connect()`` retries
            # closing it, mirroring the connect-rollback path above.
            with anyio.CancelScope(shield=True):
                await client.__aexit__(None, None, None)
                if self._client is client:
                    self._client = None
        self.log.debug("Disconnected")

    @override
    async def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.propfind("", depth=0)
        except Exception:
            return False
        else:
            return True

    # ------------------------------------------------------------------
    # Directory primitives
    # ------------------------------------------------------------------

    async def _raw_directory_entries(self, path: PathLike) -> tuple[tuple[DavResource, str], ...]:
        rel = self._remote_path(path)
        resources = await self._ensure_client().propfind(rel, depth=1)
        entries: list[tuple[DavResource, str]] = []
        for resource in resources:
            storage_path = href_to_storage_path(resource.href, self._url_prefix)
            if storage_path == rel or storage_path == "":
                continue
            entries.append((resource, storage_path))
        return tuple(entries)

    async def _scan_directory(self, path: PathLike, *, strict: bool) -> tuple[FileInfo, ...]:
        entries: list[FileInfo] = []
        for resource, storage_path in await self._raw_directory_entries(path):
            try:
                entry = dav_resource_to_file_info(resource, storage_path)
            except UnsupportedOperationError:
                if strict:
                    raise
                self.log.debug(
                    f"Skipping unsupported WebDAV resource <y>{escape_tag(storage_path)}</y>: "
                    f"{resource.resource_types!r}"
                )
                continue
            entries.append(entry)
        return tuple(sorted(entries, key=lambda entry: entry.path))

    async def _strict_walk_snapshot(self, path: PathLike, *, root: FileInfo | None = None) -> tuple[WalkEntry, ...]:
        if root is None:
            root = await self.stat(path)
        if root.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")

        pending = [self.normalize_path(path)]
        snapshot: list[WalkEntry] = []
        while pending:
            current = pending.pop()
            entries = await self._scan_directory(current, strict=True)
            snapshot.append(WalkEntry(path=current.as_posix(), entries=entries))
            directories = [entry for entry in entries if entry.kind is EntryKind.DIRECTORY]
            pending.extend(PurePosixPath(entry.path) for entry in reversed(directories))
        return tuple(snapshot)

    @override
    async def _is_dir_empty(self, path: PathLike) -> bool:
        return not await self._raw_directory_entries(path)
