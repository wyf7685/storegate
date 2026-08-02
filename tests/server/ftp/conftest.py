from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator
from pathlib import PurePosixPath

import pytest

from storegate.server.ftp import FTPServer
from storegate.storage import AbstractStorage, BytesLike, EntryKind, FileInfo, WalkEntry
from storegate.storage.abstract import PathLike


class ProtocolStorage(AbstractStorage):
    """Small symlink-aware storage used to exercise the FTP visibility boundary."""

    def __init__(self) -> None:
        super().__init__()
        self.files: dict[str, bytes] = {}
        self.directories: set[str] = {"/"}
        self.links: dict[str, str] = {}
        self.lstat_errors: dict[str, OSError] = {}

    @property
    def display_id(self) -> str:
        return "ftp-protocol-test"

    @property
    def namespace_identity(self) -> str:
        return "ftp-protocol-test:sha256:test"

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def ping(self) -> bool:
        return True

    @staticmethod
    def _path(path: PathLike) -> str:
        return AbstractStorage.normalize_path(path).as_posix()

    @staticmethod
    def _parent(path: str) -> str:
        return PurePosixPath(path).parent.as_posix()

    def _follow(self, path: str) -> str:
        return self.links.get(path, path)

    def _ensure_parent(self, path: str) -> None:
        parent = self._parent(path)
        if parent not in self.directories:
            raise FileNotFoundError(f"Parent directory not found: {parent}")

    def add_symlink(self, link_path: str, target: str) -> None:
        link = self._path(link_path)
        self._ensure_parent(link)
        self.links[link] = self._path(target)

    def set_lstat_error(self, path: str, error: OSError) -> None:
        self.lstat_errors[self._path(path)] = error

    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        requested = self._path(remote_path)
        target = self._follow(requested)
        self._ensure_parent(target)
        if target in self.directories:
            raise IsADirectoryError(f"Is a directory: {remote_path}")
        if target in self.files and not overwrite:
            raise FileExistsError(f"File exists: {remote_path}")

        content = bytearray()
        async for chunk in stream:
            content.extend(chunk)
        self.files[target] = bytes(content)

    async def download_stream(self, remote_path: PathLike, *, offset: int = 0) -> AsyncGenerator[bytes]:
        target = self._follow(self._path(remote_path))
        try:
            content = self.files[target]
        except KeyError:
            raise FileNotFoundError(f"File not found: {remote_path}") from None
        if offset < len(content):
            yield content[offset:]

    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        target = self._path(path)
        if target in self.directories:
            raise IsADirectoryError(f"Is a directory: {path}")
        if target in self.links:
            del self.links[target]
            return
        if target in self.files:
            del self.files[target]
            return
        if not missing_ok:
            raise FileNotFoundError(f"File not found: {path}")

    async def rmdir(self, path: PathLike) -> None:
        target = self._path(path)
        if target not in self.directories:
            if target in self.files or target in self.links:
                raise NotADirectoryError(f"Not a directory: {path}")
            raise FileNotFoundError(f"Directory not found: {path}")
        prefix = f"{target.rstrip("/")}/"
        entries = set(self.files) | set(self.links) | self.directories
        if any(entry != target and entry.startswith(prefix) for entry in entries):
            raise OSError(f"Directory not empty: {path}")
        self.directories.remove(target)

    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._path(src)
        destination = self._path(dst)
        self._ensure_parent(destination)
        if source in self.directories or destination in self.directories:
            raise IsADirectoryError
        if destination in self.files or destination in self.links:
            if not overwrite:
                raise FileExistsError
            self.files.pop(destination, None)
            self.links.pop(destination, None)
        if source in self.files:
            self.files[destination] = self.files.pop(source)
        elif source in self.links:
            self.links[destination] = self.links.pop(source)
        else:
            raise FileNotFoundError

    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._path(src)
        destination = self._path(dst)
        if source in self.files:
            data = self.files[source]
            await self.upload_bytes(data, destination, overwrite=overwrite)
        elif source in self.links:
            if destination in self.files or destination in self.links:
                if not overwrite:
                    raise FileExistsError
                self.files.pop(destination, None)
                self.links.pop(destination, None)
            self.links[destination] = self.links[source]
        else:
            raise FileNotFoundError

    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        target = self._path(path)
        if target in self.directories:
            if exist_ok:
                return
            raise FileExistsError(f"Directory exists: {path}")
        if target in self.files or target in self.links:
            raise FileExistsError(f"Entry exists: {path}")
        parent = self._parent(target)
        if parents:
            current = PurePosixPath("/")
            for part in PurePosixPath(target).parts[1:]:
                current /= part
                self.directories.add(current.as_posix())
        elif parent not in self.directories:
            raise FileNotFoundError(f"Parent directory not found: {parent}")
        else:
            self.directories.add(target)

    async def rmtree(self, path: PathLike) -> None:
        target = self._path(path)
        if target not in self.directories:
            raise NotADirectoryError(f"Not a directory: {path}")
        prefix = f"{target.rstrip("/")}/"
        self.files = {entry: data for entry, data in self.files.items() if not entry.startswith(prefix)}
        self.links = {entry: link for entry, link in self.links.items() if not entry.startswith(prefix)}
        self.directories = {entry for entry in self.directories if entry != target and not entry.startswith(prefix)}

    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source = self._path(src)
        destination = self._path(dst)
        if source not in self.directories:
            raise NotADirectoryError(f"Not a directory: {src}")
        if destination in self.directories and not overwrite:
            raise FileExistsError(f"Directory exists: {dst}")
        self.directories.add(destination)
        prefix = f"{source.rstrip("/")}/"
        for directory in tuple(self.directories):
            if directory.startswith(prefix):
                self.directories.add(destination + directory[len(source) :])
        for entry, data in tuple(self.files.items()):
            if entry.startswith(prefix):
                self.files[destination + entry[len(source) :]] = data
        for entry, target in tuple(self.links.items()):
            if entry.startswith(prefix):
                self.links[destination + entry[len(source) :]] = target

    async def exists(self, path: PathLike) -> bool:
        target = self._follow(self._path(path))
        return target in self.files or target in self.directories

    async def is_file(self, path: PathLike) -> bool:
        return self._follow(self._path(path)) in self.files

    async def is_dir(self, path: PathLike) -> bool:
        return self._follow(self._path(path)) in self.directories

    async def stat(self, path: PathLike) -> FileInfo:
        requested = self._path(path)
        target = self._follow(requested)
        if target in self.files:
            kind = EntryKind.FILE
            size = len(self.files[target])
        elif target in self.directories:
            kind = EntryKind.DIRECTORY
            size = 0
        else:
            raise FileNotFoundError(f"Path not found: {path}")
        return FileInfo(path=requested, name=PurePosixPath(requested).name, kind=kind, size=size)

    async def lstat(self, path: PathLike) -> FileInfo:
        requested = self._path(path)
        if error := self.lstat_errors.pop(requested, None):
            raise error
        if requested in self.links:
            return FileInfo(
                path=requested,
                name=PurePosixPath(requested).name,
                kind=EntryKind.SYMLINK,
                size=len(self.links[requested]),
            )
        return await self.stat(requested)

    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        directory = self._path(path)
        info = await self.lstat(directory)
        if info.kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")
        entries = self.files.keys() | self.directories | self.links.keys()
        for entry in sorted(entries):
            if entry != directory and self._parent(entry) == directory:
                yield await self.lstat(entry)

    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        directory = self._path(path)
        entries = tuple([entry async for entry in self.iterdir(directory)])
        yield WalkEntry(path=directory, entries=entries)
        for entry in entries:
            if entry.kind is EntryKind.DIRECTORY:
                async for child in self.walk(entry.path):
                    yield child


@pytest.fixture
async def ftp_protocol_server() -> AsyncIterator[tuple[str, int, ProtocolStorage]]:
    host = "127.0.0.1"
    storage = ProtocolStorage()
    storage.files.update(
        {
            "/rename-source.txt": b"rename source",
            "/target.txt": b"original target",
            "/target-dir/sentinel.txt": b"directory target",
        }
    )
    storage.directories.update({"/links-only", "/target-dir"})
    storage.add_symlink("/file-link", "/target.txt")
    storage.add_symlink("/directory-link", "/target-dir")
    storage.add_symlink("/links-only/hidden-link", "/target.txt")
    storage.add_symlink("/rename-destination-link", "/target.txt")
    storage.set_lstat_error("/stor-lstat-error", PermissionError("metadata denied"))
    storage.set_lstat_error("/rnto-lstat-error", OSError("metadata unavailable"))
    server = FTPServer(storage, host=host, port=0)
    async with storage:
        await server.server.start(host, 0)
        try:
            yield host, server.server.server_port, storage
        finally:
            await server.server.close()


@pytest.fixture
async def ftp_endpoint(ftp_protocol_server: tuple[str, int, ProtocolStorage]) -> tuple[str, int]:
    host, port, _storage = ftp_protocol_server
    return host, port


@pytest.fixture
async def ftp_readonly_server() -> AsyncIterator[tuple[str, int, ProtocolStorage]]:
    host = "127.0.0.1"
    storage = ProtocolStorage()
    storage.files.update(
        {
            "/readonly-test.txt": b"readonly content",
            "/target-dir/sentinel.txt": b"sentinel",
        }
    )
    storage.directories.update({"/target-dir"})
    server = FTPServer(storage, host=host, port=0, read_only=True)
    async with storage:
        await server.server.start(host, 0)
        try:
            yield host, server.server.server_port, storage
        finally:
            await server.server.close()
