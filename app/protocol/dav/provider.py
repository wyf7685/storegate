from typing import final, override

from wsgidav.dav_provider import DAVProvider as BaseDAVProvider

from app.storage import AbstractStorage

from .collection import StorageCollection
from .resource import StorageResource
from .utils import run_async


@final
class StorageProvider(BaseDAVProvider):
    @override
    def __init__(self, storage: AbstractStorage) -> None:
        super().__init__()
        self._storage = storage

    @override
    def get_resource_inst(self, path: str, environ: dict[str, object]) -> StorageResource | StorageCollection | None:
        if run_async(self._storage.is_dir, path):
            return StorageCollection(path, environ, self._storage)
        if run_async(self._storage.is_file, path):
            return StorageResource(path, environ, self._storage)
        return None

    @override
    def exists(self, path: str, environ: dict[str, object]) -> bool:
        return run_async(self._storage.exists, path)
