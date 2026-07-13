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
        try:
            info = run_async(self._storage.stat, path)
        except FileNotFoundError:
            return None
        return (StorageCollection if info.is_dir else StorageResource)(path, environ, self._storage)

    @override
    def exists(self, path: str, environ: dict[str, object]) -> bool:
        return run_async(self._storage.exists, path)
