from pathlib import PurePosixPath
from typing import final, override

from wsgidav.dav_provider import DAVCollection as BaseDAVCollection

from app.storage import AbstractStorage

from .resource import StorageResource
from .utils import run_async


@final
class StorageCollection(BaseDAVCollection):
    @override
    def __init__(self, path: str, environ: dict[str, object], storage: AbstractStorage) -> None:
        super().__init__(path, environ)
        self._storage = storage
        self._pure_path = PurePosixPath(path)

    @override
    def create_empty_resource(self, name: str) -> StorageResource:
        path = (self._pure_path / name).as_posix()
        run_async(self._storage.upload_bytes, b"", path, overwrite=False)
        return StorageResource(path, self.environ, self._storage)

    @override
    def create_collection(self, name: str) -> StorageCollection:
        path = (self._pure_path / name).as_posix()
        run_async(self._storage.mkdir, path)
        return StorageCollection(path, self.environ, self._storage)

    @override
    def get_member(self, name: str) -> StorageResource | StorageCollection | None:
        path = (self._pure_path / name).as_posix()
        if run_async(self._storage.is_dir, path):
            return StorageCollection(path, self.environ, self._storage)
        if run_async(self._storage.is_file, path):
            return StorageResource(path, self.environ, self._storage)
        return None

    @override
    def get_member_names(self) -> list[str]:
        infos = run_async(self._storage.list_, self.path)
        return [info.name for info in infos]

    @override
    def support_etag(self) -> bool:
        return False

    @override
    def support_recursive_delete(self) -> bool:
        return True

    @override
    def delete(self) -> None:
        run_async(self._storage.rmtree, self.path)

    @override
    def copy_move_single(self, dest_path: str, *, is_move: bool) -> None:
        run_async(self._storage.mkdir, dest_path, parents=True, exist_ok=True)

    @override
    def support_recursive_move(self, dest_path: str) -> bool:
        return False

    # @override
    # def move_recursive(self, dest_path: str): ...
