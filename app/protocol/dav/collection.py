import functools
from pathlib import PurePosixPath
from typing import final, override

from wsgidav.dav_provider import DAVCollection as BaseDAVCollection

from app.storage import AbstractStorage, FileInfo

from .resource import StorageResource
from .utils import NativeHandlerResult, call_with_catch, run_async


@final
class StorageCollection(BaseDAVCollection):
    @override
    def __init__(self, path: str, environ: dict[str, object], storage: AbstractStorage) -> None:
        super().__init__(path, environ)
        self._storage = storage
        self._pure_path = PurePosixPath(path)
        self._info: FileInfo | None = None

    def _get_file_info(self) -> FileInfo:
        if self._info is None:
            self._info = run_async(self._storage.stat, self.path)
        return self._info

    @override
    def get_creation_date(self) -> float | None:
        info = self._get_file_info()
        return info.created.timestamp() if info.created else None

    @override
    def support_modified(self) -> bool:
        return True

    @override
    def get_last_modified(self) -> float | None:
        info = self._get_file_info()
        return info.modified.timestamp() if info.modified else None

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
        try:
            info = run_async(self._storage.stat, path)
        except FileNotFoundError:
            return None
        return (StorageCollection if info.is_dir else StorageResource)(path, self.environ, self._storage)

    @override
    def get_member_names(self) -> list[str]:
        infos = run_async(self._storage.list_, self.path)
        return [info.name for info in infos]

    @override
    def get_member_list(self) -> list[StorageResource | StorageCollection]:
        infos = run_async(self._storage.list_, self.path)
        return [
            (StorageCollection if info.is_dir else StorageResource)(
                self._pure_path.joinpath(info.name).as_posix(), self.environ, self._storage
            )
            for info in infos
        ]

    @override
    def support_etag(self) -> bool:
        return False

    @override
    def support_recursive_delete(self) -> bool:
        return True

    @override
    def handle_delete(self) -> NativeHandlerResult:
        return run_async(call_with_catch, self, functools.partial(self._storage.rmtree, self.path))

    @override
    def handle_copy(self, dest_path: str, *, depth_infinity: bool) -> NativeHandlerResult:
        return run_async(
            call_with_catch,
            self,
            functools.partial(self._storage.copytree, self.path, dest_path, overwrite=True),
        )

    @override
    def handle_move(self, dest_path: str) -> NativeHandlerResult:
        return run_async(
            call_with_catch,
            self,
            functools.partial(self._storage.movetree, self.path, dest_path, overwrite=True),
        )

    @override
    def copy_move_single(self, dest_path: str, *, is_move: bool) -> None:
        run_async(self._storage.mkdir, dest_path, parents=True, exist_ok=True)

    @override
    def support_recursive_move(self, dest_path: str) -> bool:
        return True

    # @override
    # def move_recursive(self, dest_path: str): ...
