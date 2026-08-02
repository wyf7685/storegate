from __future__ import annotations

from typing import final, override

from wsgidav import dav_error
from wsgidav.dav_provider import DAVProvider as BaseDAVProvider

from storegate.storage import AbstractStorage, EntryKind

from .collection import StorageCollection
from .resource import StorageResource
from .utils import HiddenPathError, lstat_visible, run_async


@final
class StorageProvider(BaseDAVProvider):
    @override
    def __init__(self, storage: AbstractStorage, *, read_only: bool = False) -> None:
        super().__init__()
        self._storage = storage
        self._read_only = read_only

    @override
    def get_resource_inst(self, path: str, environ: dict[str, object]) -> StorageResource | StorageCollection | None:
        try:
            info = run_async(lstat_visible, self._storage, path)
        except HiddenPathError as exc:
            raise dav_error.DAVError(dav_error.HTTP_NOT_FOUND, path) from exc
        except FileNotFoundError:
            return None

        match info.kind:
            case EntryKind.FILE:
                return StorageResource(path, environ, self._storage, read_only=self._read_only)
            case EntryKind.DIRECTORY:
                return StorageCollection(path, environ, self._storage, read_only=self._read_only)
            case EntryKind.SYMLINK:
                return None

    @override
    def exists(self, path: str, environ: dict[str, object]) -> bool:
        try:
            info = run_async(lstat_visible, self._storage, path)
        except FileNotFoundError:
            return False

        match info.kind:
            case EntryKind.FILE | EntryKind.DIRECTORY:
                return True
            case EntryKind.SYMLINK:
                return False

    @override
    def is_readonly(self) -> bool:
        return self._read_only
