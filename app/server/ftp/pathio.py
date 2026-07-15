import builtins
import os
import stat
from collections.abc import AsyncGenerator, AsyncIterable
from pathlib import PurePath, PurePosixPath
from typing import TYPE_CHECKING, final, override

from aioftp import AbstractAsyncLister, with_timeout
from aioftp.common import Connection
from aioftp.pathio import AbstractPathIO, defend_file_methods, universal_exception

from app.log import escape_tag
from app.storage import AbstractStorage, FileInfo
from app.utils import logger_wrapper

from .handle import FileHandle, ReadHandle, WriteHandle

if TYPE_CHECKING:
    from _typeshed import OpenBinaryMode, ReadableBuffer

PathType = PurePosixPath


def _file_info_to_stat(
    info: FileInfo,
    *,
    file_permissions: int = 0o644,
    dir_permissions: int = 0o755,
) -> os.stat_result:
    """将 FileInfo 转换为 os.stat_result。

    - modified 映射到 st_mtime
    - created 映射到 st_ctime
    - st_atime 优先使用 modified，其次使用 created
    - inode、设备号、链接数、UID、GID 使用默认值
    """
    permissions = dir_permissions if info.is_dir else file_permissions
    file_type = stat.S_IFDIR if info.is_dir else stat.S_IFREG
    mode = file_type | permissions

    created = info.created.timestamp() if info.created else 0
    modified = info.modified.timestamp() if info.modified else 0

    # FileInfo 没有访问时间，选择最接近的可用时间。
    accessed = modified or created

    return os.stat_result(
        (
            mode,  # st_mode
            0,  # st_ino
            0,  # st_dev
            1,  # st_nlink
            0,  # st_uid
            0,  # st_gid
            info.size,  # st_size
            accessed or 0.0,  # st_atime
            modified or 0.0,  # st_mtime
            created or 0.0,  # st_ctime
        )
    )


@final
class StoragePathIO(AbstractPathIO[PathType]):
    @override
    def __init__(
        self,
        storage: AbstractStorage,
        *,
        timeout: float | int | None = None,
        connection: Connection | None = None,
        state: builtins.list[object] | None = None,
    ):
        super().__init__(timeout=timeout, connection=connection)
        self.storage = storage

        if connection is not None:
            host, port = connection.client_host, connection.client_port
            name = f"StoragePathIO <c><i>{escape_tag(host)}</>:<i>{port}</></>"
        else:
            name = "StoragePathIO <c><i>Unknown</></>"
        self.log = logger_wrapper(name)

    @staticmethod
    def _normalize_path(path: PurePath) -> PathType:
        return AbstractStorage.normalize_path(PurePosixPath(path))

    @override
    @universal_exception
    async def exists(self, path: PathType) -> bool:
        path = self._normalize_path(path)
        self.log.debug(f"<le>exists</>(<y><u>{escape_tag(path)}</></>)")
        return await self.storage.exists(path)

    @override
    @universal_exception
    async def is_dir(self, path: PathType) -> bool:
        path = self._normalize_path(path)
        self.log.debug(f"<le>is_dir</>(<y><u>{escape_tag(path)}</></>)")
        return await self.storage.is_dir(path)

    @override
    @universal_exception
    async def is_file(self, path: PathType) -> bool:
        path = self._normalize_path(path)
        self.log.debug(f"<le>is_file</>(<y><u>{escape_tag(path)}</></>)")
        return await self.storage.is_file(path)

    @override
    @universal_exception
    async def mkdir(self, path: PathType, *, parents: bool = False, exist_ok: bool = False) -> None:
        path = self._normalize_path(path)
        self.log.debug(
            f"<le>mkdir</>(<y><u>{escape_tag(path)}</></>, parents=<c>{parents}</>, exist_ok=<c>{exist_ok}</>)"
        )
        await self.storage.mkdir(path, parents=parents, exist_ok=exist_ok)

    @override
    @universal_exception
    async def rmdir(self, path: PathType) -> None:
        path = self._normalize_path(path)
        self.log.debug(f"<le>rmdir</>(<y><u>{escape_tag(path)}</></>)")
        if path == PurePosixPath("/"):
            raise OSError("Cannot remove root directory")
        await self.storage.rmdir(path)

    @override
    @universal_exception
    async def unlink(self, path: PathType) -> None:
        path = self._normalize_path(path)
        self.log.debug(f"<le>unlink</>(<y><u>{escape_tag(path)}</></>)")
        await self.storage.unlink(path)

    @override
    def list(self, path: PathType) -> AsyncIterable[PathType]:
        path = self._normalize_path(path)
        self.log.debug(f"<le>list</>(<y><u>{escape_tag(path)}</></>)")

        async def generator() -> AsyncGenerator[PathType]:
            async for item in self.storage.iterdir(self._normalize_path(path)):
                yield PurePosixPath(item.path)

        class Lister(AbstractAsyncLister):
            @override
            def __init__(self, timeout: float | int | None = None) -> None:
                super().__init__(timeout=timeout)
                self._agen = None

            @override
            @universal_exception
            @with_timeout
            async def __anext__(self) -> PathType:
                if self._agen is None:
                    self._agen = generator()
                return await anext(self._agen)

        return Lister(timeout=self.timeout)

    @override
    @universal_exception
    async def stat(self, path: PathType) -> os.stat_result:
        path = self._normalize_path(path)
        self.log.debug(f"<le>stat</>(<y><u>{escape_tag(path)}</></>)")
        info = await self.storage.stat(path)
        return _file_info_to_stat(info)

    @override
    @universal_exception
    async def rename(self, src: PathType, dst: PathType) -> None:
        source = self._normalize_path(src)
        destination = self._normalize_path(dst)
        self.log.debug(f"<le>rename</>(<y><u>{escape_tag(source)}</></>, <y><u>{escape_tag(destination)}</></>)")

        if await self.storage.is_dir(source):
            await self.storage.movetree(source, destination)
        else:
            await self.storage.move(source, destination)

    @override
    @universal_exception
    async def _open(
        self,
        path: PathType,
        mode: OpenBinaryMode = "rb",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> FileHandle:
        path = self._normalize_path(path)
        self.log.debug(f"<le>open</>(<y><u>{escape_tag(path)}</></>, mode=<c>{mode}</>)")
        handle = {
            "rb": ReadHandle,
            "wb": WriteHandle,
        }.get(mode)
        if handle is None:
            raise OSError(f"Unsupported mode: {mode}. Only 'rb' and 'wb' are supported.")
        return handle(self.storage, self._normalize_path(path))

    @override
    @universal_exception
    @defend_file_methods
    async def seek(self, file: FileHandle, offset: int, whence: int = 0) -> int:
        return await file.seek(offset)

    @override
    @universal_exception
    @defend_file_methods
    async def write(self, file: FileHandle, data: ReadableBuffer) -> int:
        return await file.write(memoryview(data))

    @override
    @universal_exception
    @defend_file_methods
    async def read(self, file: FileHandle, block_size: int = -1) -> bytes:
        return await file.read(block_size)

    @override
    @universal_exception
    @defend_file_methods
    async def close(self, file: FileHandle) -> None:
        await file.close()
