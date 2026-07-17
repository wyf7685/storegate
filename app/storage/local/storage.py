import functools
import os
import shutil
from collections.abc import AsyncGenerator, AsyncIterable
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import final, override

import anyio
import anyio.lowlevel
import anyio.to_thread
import ayafileio

from app.storage.abstract import AbstractStorage, BytesLike, FileInfo, PathLike, make_cache_identity


@final
class LocalStorage(AbstractStorage):
    """Local filesystem storage backend.

    Usage::

        async with LocalStorage("/data/ftp") as storage:
            await storage.upload_bytes(b"hello", "foo.txt")
    """

    def __init__(self, root: str | Path) -> None:
        super().__init__()
        self._root: Path = Path(root).absolute()

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    @override
    def id(self) -> str:
        return f"local:{self._root.as_posix()}"

    @property
    @override
    def cache_identity(self) -> str:
        return make_cache_identity("local", root=self._root.resolve(strict=False).as_posix())

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @override
    async def connect(self) -> None:
        await anyio.Path(self._root).mkdir(parents=True, exist_ok=True)

    @override
    async def close(self) -> None:
        pass

    @override
    async def ping(self) -> bool:
        return await anyio.Path(self._root).is_dir()

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _resolve(self, path: PathLike) -> Path:
        """Resolve *path* to an absolute ``Path`` under ``self._root``.

        Raises :exc:`ValueError` if *path* attempts to escape the root
        directory.
        """
        # Normalise: treat every path as relative.
        p = self.normalize_path(path).relative_to("/")
        full = Path(os.path.normpath(self._root / p))

        try:
            full.relative_to(self._root)
        except ValueError:
            raise ValueError(f"Path traversal detected: {path!r}") from None

        return full

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        try:
            info = await self.stat(remote_path)
        except FileNotFoundError:
            pass
        else:
            if info.is_dir:
                raise IsADirectoryError(f"Is a directory: {remote_path}")
            if not overwrite:
                raise FileExistsError(f"File already exists: {remote_path}")

        target = self._resolve(remote_path)
        await anyio.Path(target.parent).mkdir(parents=True, exist_ok=True)

        async with ayafileio.open(target, "wb") as f:
            async for chunk in stream:
                await f.write(chunk if isinstance(chunk, (bytes, bytearray, memoryview)) else memoryview(chunk))

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    @override
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
        target = self._resolve(remote_path)

        if not await anyio.Path(target).is_file():
            raise FileNotFoundError(f"File not found: {remote_path}")

        async with ayafileio.open(target, "rb") as f:
            if offset:
                await f.seek(offset)
            async for chunk in f.chunk(1024 * 1024):
                yield bytes(chunk)

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    @override
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        target = self._resolve(path)
        await anyio.Path(target).unlink(missing_ok=missing_ok)

    @override
    async def rmdir(self, path: PathLike) -> None:
        target = self._resolve(path)
        await anyio.Path(target).rmdir()

    @override
    async def move(self, src: PathLike, dst: PathLike) -> None:
        source = self._resolve(src)
        dest = self._resolve(dst)

        await anyio.Path(dest.parent).mkdir(parents=True, exist_ok=True)
        await anyio.Path(source).rename(dest)

    @override
    async def copy(self, src: PathLike, dst: PathLike) -> None:
        source = self._resolve(src)
        dest = self._resolve(dst)

        await anyio.Path(dest.parent).mkdir(parents=True, exist_ok=True)
        await anyio.to_thread.run_sync(shutil.copy2, str(source), str(dest))

    # ------------------------------------------------------------------
    # Directory
    # ------------------------------------------------------------------

    @override
    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        target = self._resolve(path)
        await anyio.Path(target).mkdir(parents=parents, exist_ok=exist_ok)

    @override
    async def rmtree(self, path: PathLike) -> None:
        target = self._resolve(path)
        await anyio.to_thread.run_sync(functools.partial(shutil.rmtree, target))

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._resolve(src)
        destination = self._resolve(dst)

        if not await anyio.Path(source).is_dir():
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await anyio.Path(destination).exists():
            raise FileExistsError(f"Destination already exists: {dst}")

        await anyio.to_thread.run_sync(functools.partial(shutil.copytree, source, destination, dirs_exist_ok=overwrite))

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._resolve(src)
        dest = self._resolve(dst)

        if not await anyio.Path(source).is_dir():
            raise NotADirectoryError(f"Not a directory: {src}")
        if not overwrite and await anyio.Path(dest).exists():
            raise FileExistsError(f"Destination already exists: {dst}")

        try:
            await anyio.Path(source).rename(dest)
        except OSError:  # EXDEV cross-device
            await self.copytree(src, dst, overwrite=overwrite)
            await self.rmtree(src)

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @override
    async def exists(self, path: PathLike) -> bool:
        return await anyio.Path(self._resolve(path)).exists()

    @override
    async def is_file(self, path: PathLike) -> bool:
        return await anyio.Path(self._resolve(path)).is_file()

    @override
    async def is_dir(self, path: PathLike) -> bool:
        return await anyio.Path(self._resolve(path)).is_dir()

    @override
    async def stat(self, path: PathLike) -> FileInfo:
        target = self._resolve(path)
        p = anyio.Path(target)

        if not await p.exists():
            raise FileNotFoundError(f"Path not found: {path}")

        stat_result = await p.stat()
        is_dir = await p.is_dir()

        return FileInfo(
            path=self.normalize_path(path).as_posix(),
            name=target.name,
            is_dir=is_dir,
            size=0 if is_dir else stat_result.st_size,
            modified=datetime.fromtimestamp(stat_result.st_mtime).astimezone(),
            created=datetime.fromtimestamp(stat_result.st_ctime).astimezone(),
        )

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    @override
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        target = self._resolve(path)
        p = anyio.Path(target)

        if not await p.is_dir():
            raise NotADirectoryError(f"Not a directory: {path}")

        async for entry in p.iterdir():
            stat_result = await entry.stat()
            entry_is_dir = await entry.is_dir()
            yield FileInfo(
                path=self.normalize_path(
                    PurePosixPath(str(entry.relative_to(self._root)).replace("\\", "/"))
                ).as_posix(),
                name=entry.name,
                is_dir=entry_is_dir,
                size=0 if entry_is_dir else stat_result.st_size,
                modified=datetime.fromtimestamp(stat_result.st_mtime).astimezone(),
                created=datetime.fromtimestamp(stat_result.st_ctime).astimezone(),
            )

    @override
    async def walk(self, path: PathLike) -> AsyncGenerator[tuple[str, list[FileInfo], list[FileInfo]]]:
        target = self._resolve(path)
        p = anyio.Path(target)

        if not await p.is_dir():
            raise NotADirectoryError(f"Not a directory: {path}")

        dirs: list[FileInfo] = []
        files: list[FileInfo] = []

        async for entry in p.iterdir():
            stat_result = await entry.stat()
            entry_is_dir = await entry.is_dir()
            info = FileInfo(
                path=self.normalize_path(
                    PurePosixPath(str(entry.relative_to(self._root)).replace("\\", "/"))
                ).as_posix(),
                name=entry.name,
                is_dir=entry_is_dir,
                size=0 if entry_is_dir else stat_result.st_size,
                modified=datetime.fromtimestamp(stat_result.st_mtime).astimezone(),
                created=datetime.fromtimestamp(stat_result.st_ctime).astimezone(),
            )
            (dirs if entry_is_dir else files).append(info)

        yield self.normalize_path(path).as_posix(), dirs, files

        for d in dirs:
            async for sub_path, sub_dirs, sub_files in self.walk(d.path):
                yield sub_path, sub_dirs, sub_files
