import functools
import os
import shutil
import stat
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

    The configured root is canonicalized once at construction. Every logical
    path is then checked for symlink/junction/reparse-point components before
    use; link components are rejected rather than followed. This protects
    against static path configuration mistakes, but is not a defense against
    a concurrent attacker swapping directory entries between the check and
    the filesystem operation (TOCTOU).

    Usage::

        async with LocalStorage("/data/ftp") as storage:
            await storage.upload_bytes(b"hello", "foo.txt")
    """

    def __init__(self, root: str | Path) -> None:
        super().__init__()
        # A symlink is an explicit, stable root configuration: resolve it once
        # so subsequent path checks cannot accidentally escape via the alias.
        self._root: Path = Path(root).absolute().resolve(strict=False)

    @staticmethod
    def _is_link_or_reparse(path: Path) -> bool:
        try:
            result = path.lstat()
        except FileNotFoundError:
            return False
        if path.is_symlink():
            return True
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        return bool(getattr(result, "st_file_attributes", 0) & reparse_flag)

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
        """Resolve *path* under the canonical root without following links.

        Existing symlink, junction, and Windows reparse-point components are
        rejected, including the final component. Missing final components are
        allowed for create operations, while all existing parent components
        must pass the same check.
        """
        p = self.normalize_path(path).relative_to("/")
        full = Path(os.path.normpath(self._root / p))

        try:
            relative = full.relative_to(self._root)
        except ValueError:
            raise ValueError(f"Path traversal detected: {path!r}") from None

        current = self._root
        for component in relative.parts:
            current /= component
            if self._is_link_or_reparse(current):
                raise ValueError(f"Path contains a symlink or reparse point: {path!r}")

        return full

    def _assert_tree_safe(self, root: Path) -> None:
        """Reject links anywhere in a tree before an operation can traverse it."""
        if self._is_link_or_reparse(root):
            raise ValueError(f"Path contains a symlink or reparse point: {root}")
        try:
            entries = list(os.scandir(root))
        except NotADirectoryError:
            return
        for entry in entries:
            child = Path(entry.path)
            if self._is_link_or_reparse(child):
                raise ValueError(f"Path contains a symlink or reparse point: {child}")
            if entry.is_dir(follow_symlinks=False):
                self._assert_tree_safe(child)

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
        if await anyio.Path(target).is_dir():
            raise IsADirectoryError(f"Is a directory: {path}")
        await anyio.Path(target).unlink(missing_ok=missing_ok)

    @override
    async def rmdir(self, path: PathLike) -> None:
        target = self._resolve(path)
        try:
            await anyio.Path(target).rmdir()
        except OSError as exc:
            if isinstance(exc, FileNotFoundError):
                raise
            if isinstance(exc, NotADirectoryError):
                raise
            raise OSError(f"Directory not empty: {path}") from exc

    @override
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._resolve(src)
        dest = self._resolve(dst)
        if await anyio.Path(source).is_dir():
            raise IsADirectoryError(f"Is a directory: {src}")
        if not await anyio.Path(source).exists():
            raise FileNotFoundError(f"Source not found: {src}")
        if await anyio.Path(dest).is_dir():
            raise IsADirectoryError(f"Destination is a directory: {dst}")
        if await anyio.Path(dest).exists() and not overwrite:
            raise FileExistsError(f"Destination file already exists: {dst}")
        await anyio.Path(dest.parent).mkdir(parents=True, exist_ok=True)
        if overwrite:
            await anyio.to_thread.run_sync(os.replace, str(source), str(dest))
        else:
            await anyio.Path(source).rename(dest)

    @override
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._resolve(src)
        dest = self._resolve(dst)
        if source == dest and overwrite:
            if not await anyio.Path(source).exists():
                raise FileNotFoundError(f"Source not found: {src}")
            if await anyio.Path(source).is_dir():
                raise IsADirectoryError(f"Is a directory: {src}")
            return
        if not await anyio.Path(source).exists():
            raise FileNotFoundError(f"Source not found: {src}")
        if await anyio.Path(source).is_dir():
            raise IsADirectoryError(f"Is a directory: {src}")
        if await anyio.Path(dest).is_dir():
            raise IsADirectoryError(f"Destination is a directory: {dst}")
        if await anyio.Path(dest).exists() and not overwrite:
            raise FileExistsError(f"Destination file already exists: {dst}")
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
        self._assert_tree_safe(target)
        await anyio.to_thread.run_sync(functools.partial(shutil.rmtree, target))

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._resolve(src)
        destination = self._resolve(dst)

        if not await anyio.Path(source).is_dir():
            raise NotADirectoryError(f"Not a directory: {src}")
        self._assert_tree_safe(source)
        if await anyio.Path(destination).exists():
            self._assert_tree_safe(destination)
        if not overwrite and await anyio.Path(destination).exists():
            raise FileExistsError(f"Destination already exists: {dst}")

        await anyio.to_thread.run_sync(functools.partial(shutil.copytree, source, destination, dirs_exist_ok=overwrite))

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._resolve(src)
        dest = self._resolve(dst)

        if not await anyio.Path(source).is_dir():
            raise NotADirectoryError(f"Not a directory: {src}")
        self._assert_tree_safe(source)
        if await anyio.Path(dest).exists():
            self._assert_tree_safe(dest)
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
            entry_path = Path(entry)
            if self._is_link_or_reparse(entry_path):
                raise ValueError(f"Path contains a symlink or reparse point: {entry_path}")
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
            entry_path = Path(entry)
            if self._is_link_or_reparse(entry_path):
                raise ValueError(f"Path contains a symlink or reparse point: {entry_path}")
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
