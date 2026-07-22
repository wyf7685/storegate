import errno

from storegate.storage import AbstractStorage, EntryKind, FileInfo, UnsupportedOperationError
from storegate.storage.abstract import PathLike

_UNSUPPORTED_ERRNO = getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)


async def lstat_private_entry(storage: AbstractStorage, path: PathLike, *, label: str) -> FileInfo:
    """Inspect a private IndexStorage entry without following a final symlink."""
    info = await storage.lstat(path)
    if info.kind is EntryKind.SYMLINK:
        raise UnsupportedOperationError(
            _UNSUPPORTED_ERRNO,
            f"IndexStorage private {label} is a symbolic link: {storage.normalize_path(path)}",
        )
    return info


async def lstat_private_entry_or_none(
    storage: AbstractStorage,
    path: PathLike,
    *,
    label: str,
) -> FileInfo | None:
    try:
        return await lstat_private_entry(storage, path, label=label)
    except FileNotFoundError:
        return None


async def download_private_file(storage: AbstractStorage, path: PathLike, *, label: str) -> bytes:
    info = await lstat_private_entry(storage, path, label=label)
    if info.kind is EntryKind.DIRECTORY:
        raise IsADirectoryError(f"IndexStorage private {label} is a directory: {storage.normalize_path(path)}")
    return await storage.download_bytes(path)
