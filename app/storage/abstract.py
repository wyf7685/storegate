from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Self

import anyio
import ayafileio


@dataclass(slots=True, frozen=True)
class FileInfo:
    path: str
    name: str
    is_dir: bool
    size: int | None
    modified: datetime | None = None
    created: datetime | None = None


type BytesLike = bytes | bytearray | memoryview


class AbstractStorage(ABC):
    """Abstract storage interface."""

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
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        with anyio.CancelScope(shield=True):
            await self.close()

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    @abstractmethod
    async def upload_bytes(
        self,
        data: BytesLike,
        remote_path: str,
        *,
        overwrite: bool = True,
    ) -> None:
        """Upload bytes."""
        raise NotImplementedError

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
    async def download_bytes(
        self,
        remote_path: str,
    ) -> bytes:
        """Download as bytes."""
        raise NotImplementedError

    @abstractmethod
    async def download_stream(
        self,
        remote_path: str,
    ) -> AsyncIterator[bytes]:
        """Download as an async byte stream."""
        raise NotImplementedError
        yield

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
    async def delete(self, path: str) -> None:
        """Delete a file or an empty directory."""
        raise NotImplementedError

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

    @abstractmethod
    async def list_(self, path: str) -> list[FileInfo]:
        """List directory."""
        raise NotImplementedError

    @abstractmethod
    async def iterdir(self, path: str) -> AsyncIterator[FileInfo]:
        """Iterate directory entries."""
        raise NotImplementedError
        yield

    @abstractmethod
    async def walk(self, path: str) -> AsyncIterator[tuple[str, list[FileInfo]]]:
        """Recursively walk a directory tree."""
        raise NotImplementedError
        yield
