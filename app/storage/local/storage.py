import os
import shutil
from collections.abc import AsyncIterable, AsyncIterator
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Self, final, override

import anyio
import anyio.lowlevel
import anyio.to_thread
import ayafileio

from app.storage.abstract import AbstractStorage, BytesLike, FileInfo


@final
class LocalStorage(AbstractStorage):
    """Local filesystem storage backend.

    Usage::

        storage = LocalStorage.from_directory("/data/ftp")
        async with storage:
            await storage.upload_bytes(b"hello", "foo.txt")
    """

    def __init__(self, root: str | Path) -> None:
        self._root: Path = Path(root).absolute()

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_directory(cls, directory: str | Path) -> Self:
        """Create a ``LocalStorage`` rooted at *directory*."""
        return cls(root=directory)

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @override
    @property
    def id(self) -> str:
        return f"local:{self._root.as_posix()}"

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

    def _resolve(self, path: str) -> Path:
        """Resolve *path* to an absolute ``Path`` under ``self._root``.

        Raises :exc:`ValueError` if *path* attempts to escape the root
        directory.
        """
        # Normalise: treat every path as relative.
        p = PurePosixPath(path)
        if p.is_absolute():
            p = p.relative_to("/")

        raw = os.path.normpath(str(self._root / str(p)))
        full = Path(raw)

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
        remote_path: str,
        *,
        overwrite: bool = True,
    ) -> None:
        target = self._resolve(remote_path)

        if not overwrite and await anyio.Path(target).exists():
            raise FileExistsError(f"File already exists: {remote_path}")

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
        remote_path: str,
    ) -> AsyncIterator[bytes]:
        target = self._resolve(remote_path)

        if not await anyio.Path(target).is_file():
            raise FileNotFoundError(f"File not found: {remote_path}")

        async with ayafileio.open(target, "rb") as f:
            async for chunk in f.chunk(1024 * 1024):
                yield bytes(chunk)

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    @override
    async def delete(self, path: str) -> None:
        target = self._resolve(path)
        p = anyio.Path(target)
        if await p.is_dir():
            await p.rmdir()
        else:
            await p.unlink(missing_ok=False)

    @override
    async def move(self, src: str, dst: str) -> None:
        source = self._resolve(src)
        dest = self._resolve(dst)

        await anyio.Path(dest.parent).mkdir(parents=True, exist_ok=True)
        await anyio.Path(source).rename(dest)

    @override
    async def copy(self, src: str, dst: str) -> None:
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
        path: str,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        target = self._resolve(path)
        await anyio.Path(target).mkdir(parents=parents, exist_ok=exist_ok)

    @override
    async def rmtree(self, path: str) -> None:
        target = self._resolve(path)
        await anyio.to_thread.run_sync(shutil.rmtree, str(target))

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @override
    async def exists(self, path: str) -> bool:
        return await anyio.Path(self._resolve(path)).exists()

    @override
    async def is_file(self, path: str) -> bool:
        return await anyio.Path(self._resolve(path)).is_file()

    @override
    async def is_dir(self, path: str) -> bool:
        return await anyio.Path(self._resolve(path)).is_dir()

    @override
    async def stat(self, path: str) -> FileInfo:
        target = self._resolve(path)
        p = anyio.Path(target)

        if not await p.exists():
            raise FileNotFoundError(f"Path not found: {path}")

        stat_result = await p.stat()
        is_dir = await p.is_dir()

        return FileInfo(
            path=path,
            name=target.name,
            is_dir=is_dir,
            size=None if is_dir else stat_result.st_size,
            modified=datetime.fromtimestamp(stat_result.st_mtime).astimezone(),
            created=datetime.fromtimestamp(stat_result.st_ctime).astimezone(),
        )

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    @override
    async def iterdir(self, path: str) -> AsyncIterator[FileInfo]:
        target = self._resolve(path)
        p = anyio.Path(target)

        if not await p.is_dir():
            raise NotADirectoryError(f"Not a directory: {path}")

        async for entry in p.iterdir():
            stat_result = await entry.stat()
            entry_is_dir = await entry.is_dir()
            yield FileInfo(
                path=str(entry.relative_to(self._root)).replace("\\", "/"),
                name=entry.name,
                is_dir=entry_is_dir,
                size=None if entry_is_dir else stat_result.st_size,
                modified=datetime.fromtimestamp(stat_result.st_mtime).astimezone(),
                created=datetime.fromtimestamp(stat_result.st_ctime).astimezone(),
            )

    @override
    async def walk(self, path: str) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
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
                path=str(entry.relative_to(self._root)).replace("\\", "/"),
                name=entry.name,
                is_dir=entry_is_dir,
                size=None if entry_is_dir else stat_result.st_size,
                modified=datetime.fromtimestamp(stat_result.st_mtime).astimezone(),
                created=datetime.fromtimestamp(stat_result.st_ctime).astimezone(),
            )
            (dirs if entry_is_dir else files).append(info)

        yield path, dirs, files

        for d in dirs:
            async for sub_path, sub_dirs, sub_files in self.walk(d.path):
                yield sub_path, sub_dirs, sub_files
