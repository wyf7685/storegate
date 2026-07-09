import functools
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Self

import anyio
import anyio.lowlevel
import ayafileio

from app.log import escape_tag
from app.utils import LoggerWrapper, logger_wrapper


@dataclass(slots=True, frozen=True)
class FileInfo:
    path: str
    name: str
    is_dir: bool
    size: int = 0
    modified: datetime | None = None
    created: datetime | None = None


type BytesLike = bytes | bytearray | memoryview


class AbstractStorage(ABC):
    """Abstract storage interface."""

    def __init__(self) -> None:
        self.__ctx = 0

    @functools.cached_property
    def log(self) -> LoggerWrapper:
        return logger_wrapper(f"{self.__class__.__name__} <c><i>{escape_tag(self.id)}</></>")

    @property
    @abstractmethod
    def id(self) -> str:
        """Return a unique identifier for this storage instance."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection."""
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Close connection."""
        raise NotImplementedError

    @abstractmethod
    async def ping(self) -> bool:
        """Check whether the connection is alive."""
        raise NotImplementedError

    async def __aenter__(self) -> Self:
        self.__ctx += 1
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.__ctx = max(0, self.__ctx - 1)
        if self.__ctx:
            return
        with anyio.CancelScope(shield=True):
            await self.close()

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    @abstractmethod
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: str,
        *,
        overwrite: bool = True,
    ) -> None:
        """Upload from a binary stream."""
        raise NotImplementedError

    async def upload_bytes(
        self,
        data: BytesLike,
        remote_path: str,
        *,
        overwrite: bool = True,
    ) -> None:
        """Upload bytes."""
        buf = memoryview(data).toreadonly()

        async def aiterable() -> AsyncIterable[memoryview[int]]:
            ptr = 0
            while ptr < len(buf):
                yield buf[ptr : ptr + 8192]
                ptr += 8192
                await anyio.lowlevel.checkpoint()

        await self.upload_stream(aiterable(), remote_path, overwrite=overwrite)

    async def upload_file(
        self,
        local_path: str | Path,
        remote_path: str,
        *,
        overwrite: bool = True,
    ) -> None:
        """Upload a local file."""

        async with ayafileio.open(local_path, "rb") as file:
            await self.upload_stream(file.chunk(1024 * 1024), remote_path, overwrite=overwrite)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    @abstractmethod
    async def download_stream(
        self,
        remote_path: str,
        *,
        offset: int = 0,
    ) -> AsyncIterator[bytes]:
        """Download as an async byte stream.

        Args:
            remote_path: Path to the file.
            offset: Byte offset to start reading from.  Default 0 (start of file).
        """
        raise NotImplementedError
        yield

    async def download_bytes(
        self,
        remote_path: str,
    ) -> bytes:
        """Download as bytes."""
        buffer = bytearray()
        async for chunk in self.download_stream(remote_path):
            buffer.extend(chunk)
        return bytes(buffer)

    async def download_file(
        self,
        remote_path: str,
        local_path: str | Path,
    ) -> None:
        """Download to a local file."""
        async with ayafileio.open(local_path, "wb") as file:
            async for chunk in self.download_stream(remote_path):
                await file.write(chunk)

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    @abstractmethod
    async def unlink(self, path: str, *, missing_ok: bool = False) -> None:
        """Delete a file.

        Args:
            path: Path to the file.
            missing_ok: If ``True``, silently succeed when the file does not exist.

        Raises:
            IsADirectoryError: If *path* is a directory.
            FileNotFoundError: If *path* does not exist and *missing_ok* is ``False``.
        """
        raise NotImplementedError

    @abstractmethod
    async def rmdir(self, path: str) -> None:
        """Delete an empty directory.

        Raises:
            NotADirectoryError: If *path* is a file.
            OSError: If the directory is not empty.
            FileNotFoundError: If *path* does not exist (implementations may
                silently succeed instead).
        """
        raise NotImplementedError

    async def delete(self, path: str) -> None:
        """Delete a file or an empty directory.

        Convenience method that calls :meth:`rmdir` if *path* is a directory,
        otherwise :meth:`unlink`.
        """
        if await self.is_dir(path):
            await self.rmdir(path)
        else:
            await self.unlink(path)

    async def delete_many(self, *paths: str) -> None:
        """Delete multiple files or empty directories."""
        for path in paths:
            await self.delete(path)

    @abstractmethod
    async def move(
        self,
        src: str,
        dst: str,
    ) -> None:
        """Move or rename."""
        raise NotImplementedError

    @abstractmethod
    async def copy(
        self,
        src: str,
        dst: str,
    ) -> None:
        """
        Copy a file.

        FTP does not support COPY natively; implementations may emulate it.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Directory
    # ------------------------------------------------------------------

    @abstractmethod
    async def mkdir(
        self,
        path: str,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """Create directory."""
        raise NotImplementedError

    @abstractmethod
    async def rmtree(self, path: str) -> None:
        """Recursively remove a directory."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @abstractmethod
    async def exists(self, path: str) -> bool:
        """Return whether a path exists."""
        raise NotImplementedError

    @abstractmethod
    async def is_file(self, path: str) -> bool:
        """Return whether the path is a file."""
        raise NotImplementedError

    @abstractmethod
    async def is_dir(self, path: str) -> bool:
        """Return whether the path is a directory."""
        raise NotImplementedError

    @abstractmethod
    async def stat(self, path: str) -> FileInfo:
        """Return metadata."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    async def list_(self, path: str) -> list[FileInfo]:
        """List directory."""
        return [item async for item in self.iterdir(path)]

    @abstractmethod
    async def iterdir(self, path: str) -> AsyncIterator[FileInfo]:
        """Iterate directory entries."""
        raise NotImplementedError
        yield

    @abstractmethod
    async def walk(self, path: str) -> AsyncIterator[tuple[str, list[FileInfo], list[FileInfo]]]:
        """Recursively walk a directory tree."""
        raise NotImplementedError
        yield

    async def _is_dir_empty(self, path: str) -> bool:
        """Check if a directory is empty."""
        async for _ in self.iterdir(path):
            return False
        return True
