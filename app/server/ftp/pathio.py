import builtins
import os
import stat
from collections.abc import AsyncGenerator, AsyncIterable
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING, cast, final, override

from aioftp import AbstractAsyncLister, with_timeout
from aioftp.common import Connection
from aioftp.pathio import AbstractPathIO, defend_file_methods, universal_exception

from app.log import escape_tag
from app.storage import AbstractStorage, FileInfo
from app.utils import logger_wrapper

from .handle import FileHandle, ReadHandle, WriteHandle

if TYPE_CHECKING:
    from _typeshed import OpenBinaryMode, ReadableBuffer


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
class StoragePathIO(AbstractPathIO[Path]):
    storage: AbstractStorage

    @override
    def __init__(
        self,
        *,
        timeout: float | int | None = None,
        connection: Connection | None = None,
        state: builtins.list[object] | None = None,
    ):
        if not hasattr(self, "storage"):
            raise TypeError("StoragePathIO should bound to a storage instance before instantiation.")

        super().__init__(timeout=timeout, connection=connection)
        if connection is not None:
            host, port = connection.client_host, connection.client_port
            name = f"StoragePathIO <c><i>{escape_tag(host)}</>:<i>{port}</></>"
        else:
            name = "StoragePathIO <c><i>Unknown</></>"
        self.log = logger_wrapper(name)

    @classmethod
    def with_storage(cls, storage: AbstractStorage) -> type[StoragePathIO]:
        new_cls = type("StoragePathIO", (cls,), {"storage": storage})
        return cast("type[StoragePathIO]", new_cls)

    @staticmethod
    def _normalize_path(path: PurePath) -> PurePosixPath:
        return AbstractStorage.normalize_path(PurePosixPath(path))

    @override
    @universal_exception
    async def exists(self, path: Path) -> bool:
        np = self._normalize_path(path)
        self.log.debug(f"<le>exists</>(<y><u>{escape_tag(np)}</></>)")
        return await self.storage.exists(np)

    @override
    @universal_exception
    async def is_dir(self, path: Path) -> bool:
        np = self._normalize_path(path)
        self.log.debug(f"<le>is_dir</>(<y><u>{escape_tag(np)}</></>)")
        return await self.storage.is_dir(np)

    @override
    @universal_exception
    async def is_file(self, path: Path) -> bool:
        np = self._normalize_path(path)
        self.log.debug(f"<le>is_file</>(<y><u>{escape_tag(np)}</></>)")
        return await self.storage.is_file(np)

    @override
    @universal_exception
    async def mkdir(self, path: Path, *, parents: bool = False, exist_ok: bool = False) -> None:
        np = self._normalize_path(path)
        self.log.debug(
            f"<le>mkdir</>(<y><u>{escape_tag(np)}</></>, parents=<c>{parents}</>, exist_ok=<c>{exist_ok}</>)"
        )
        await self.storage.mkdir(np, parents=parents, exist_ok=exist_ok)

    @override
    @universal_exception
    async def rmdir(self, path: Path) -> None:
        np = self._normalize_path(path)
        self.log.debug(f"<le>rmdir</>(<y><u>{escape_tag(np)}</></>)")
        if np == PurePosixPath("/"):
            raise OSError("Cannot remove root directory")
        await self.storage.rmdir(np)

    @override
    @universal_exception
    async def unlink(self, path: Path) -> None:
        np = self._normalize_path(path)
        self.log.debug(f"<le>unlink</>(<y><u>{escape_tag(np)}</></>)")
        await self.storage.unlink(np)

    @override
    def list(self, path: Path) -> AsyncIterable[Path]:
        np = self._normalize_path(path)
        self.log.debug(f"<le>list</>(<y><u>{escape_tag(np)}</></>)")

        async def generator() -> AsyncGenerator[PurePosixPath]:
            async for item in self.storage.iterdir(self._normalize_path(np)):
                yield PurePosixPath(item.path)

        class Lister(AbstractAsyncLister[Path]):
            @override
            def __init__(self, timeout: float | int | None = None) -> None:
                super().__init__(timeout=timeout)
                self.agen: AsyncGenerator[PurePosixPath] | None = None

            @override
            @universal_exception
            @with_timeout
            async def __anext__(self) -> Path:
                if self.agen is None:
                    self.agen = generator()
                return Path(await anext(self.agen))

        return Lister(timeout=self.timeout)

    @override
    @universal_exception
    async def stat(self, path: Path) -> os.stat_result:
        np = self._normalize_path(path)
        self.log.debug(f"<le>stat</>(<y><u>{escape_tag(np)}</></>)")
        info = await self.storage.stat(np)
        return _file_info_to_stat(info)

    @override
    @universal_exception
    async def rename(self, source: Path, destination: Path) -> Path:
        src = self._normalize_path(source)
        dst = self._normalize_path(destination)
        self.log.debug(f"<le>rename</>(<y><u>{escape_tag(src)}</></>, <y><u>{escape_tag(dst)}</></>)")

        if await self.storage.is_dir(src):
            await self.storage.movetree(src, dst)
        else:
            await self.storage.move(src, dst)

        return destination

    @override
    @universal_exception
    async def _open(
        self,
        path: Path,
        mode: OpenBinaryMode = "rb",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> FileHandle:
        np = self._normalize_path(path)
        self.log.debug(f"<le>open</>(<y><u>{escape_tag(np)}</></>, mode=<c>{mode}</>)")
        handle = {
            "rb": ReadHandle,
            "wb": WriteHandle,
        }.get(mode)
        if handle is None:
            raise OSError(f"Unsupported mode: {mode}. Only 'rb' and 'wb' are supported.")
        return handle(self.storage, self._normalize_path(np))

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
