import contextlib
import errno
import functools
import ntpath
import os
import shutil
import stat
import tempfile
import uuid
from collections.abc import AsyncGenerator, AsyncIterable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import final, override

import anyio
import anyio.to_thread

from storegate.storage.abstract import (
    AbstractStorage,
    BytesLike,
    EntryKind,
    FileInfo,
    PathLike,
    StorageCapabilities,
    UnsupportedOperationError,
    WalkEntry,
    make_namespace_identity,
    validate_download_offset,
    validate_same_path_file_operation,
    validate_same_path_tree_operation,
)

_LOCAL_CAPABILITIES = StorageCapabilities(symlink_metadata=True, readlink=True, symlink_create=True)
_UNSUPPORTED_ERRNO = getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)
_MAX_SYMLINK_HOPS = 40
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
_DIRECTORY_ATTRIBUTE = getattr(stat, "FILE_ATTRIBUTE_DIRECTORY", 0x10)


class _RawKind(StrEnum):
    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"
    JUNCTION = "junction"
    SPECIAL = "special"


@dataclass(slots=True, frozen=True)
class _TreeEntry:
    relative_parts: tuple[str, ...]
    kind: EntryKind
    link_target: str | None = None
    target_is_directory: bool = False


@dataclass(slots=True)
class _MutationJournal:
    root: Path
    created: list[Path]
    backups: list[tuple[Path, Path]]
    backup_root: Path | None = None

    @classmethod
    def create(cls, root: Path) -> _MutationJournal:
        return cls(root=root, created=[], backups=[])

    def record_created(self, path: Path) -> None:
        self.created.append(path)

    def backup(self, path: Path) -> None:
        if self.backup_root is None:
            self.backup_root = Path(tempfile.mkdtemp(prefix=".storegate-local-backup-", dir=self.root))
        backup_path = self.backup_root / uuid.uuid4().hex
        path.replace(backup_path)
        self.backups.append((path, backup_path))

    def rollback(self) -> None:
        for path in reversed(self.created):
            try:
                result = os.lstat(path)
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(result.st_mode) and not stat.S_ISLNK(result.st_mode):
                path.rmdir()
            else:
                path.unlink()
        for original, backup in reversed(self.backups):
            original.parent.mkdir(parents=True, exist_ok=True)
            backup.replace(original)
        self.cleanup()

    def cleanup(self) -> None:
        if self.backup_root is not None:
            shutil.rmtree(self.backup_root, ignore_errors=False)
            self.backup_root = None


@final
class LocalStorage(AbstractStorage):
    """Local filesystem storage rooted at one canonical directory.

    Caller paths are mapped lexically below the configured root. Existing
    intermediate symlinks, junctions, and unknown reparse points are rejected.
    Final filesystem symlinks can be inspected, preserved, or safely followed;
    every follow hop is checked against the canonical root. These checks protect
    against stable path escapes, but cannot eliminate filesystem TOCTOU races.
    """

    def __init__(self, root: str | Path) -> None:
        super().__init__()
        self._root = Path(root).absolute().resolve(strict=False)

    # ------------------------------------------------------------------
    # Identity and lifecycle
    # ------------------------------------------------------------------

    @property
    @override
    def display_id(self) -> str:
        return f"local:{self._root.as_posix()}"

    @property
    @override
    def namespace_identity(self) -> str:
        return make_namespace_identity("local", root=self._root.as_posix())

    @property
    @override
    def capabilities(self) -> StorageCapabilities:
        return _LOCAL_CAPABILITIES

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
    # Path mapping and classification
    # ------------------------------------------------------------------

    def _logical_path(self, path: PathLike) -> PurePosixPath:
        raw = os.fspath(path)
        if os.name == "nt" and "\\" in raw:
            raise ValueError(f"Local paths must use POSIX separators: {path!r}")
        return self.normalize_path(raw)

    def _lexical_path(self, path: PathLike) -> tuple[PurePosixPath, Path]:
        logical = self._logical_path(path)
        relative = logical.relative_to("/")
        local = self._root.joinpath(*relative.parts)
        try:
            local.relative_to(self._root)
        except ValueError:
            raise ValueError(f"Path traversal detected: {path!r}") from None
        return logical, local

    @staticmethod
    def _classify_entry(path: Path, result: os.stat_result) -> _RawKind:
        is_junction = getattr(path, "is_junction", None)
        if is_junction is not None and is_junction():
            return _RawKind.JUNCTION
        if stat.S_ISLNK(result.st_mode) or path.is_symlink():
            return _RawKind.SYMLINK
        attributes = getattr(result, "st_file_attributes", 0)
        if attributes & _REPARSE_POINT:
            return _RawKind.SPECIAL
        if stat.S_ISREG(result.st_mode):
            return _RawKind.FILE
        if stat.S_ISDIR(result.st_mode):
            return _RawKind.DIRECTORY
        return _RawKind.SPECIAL

    @staticmethod
    def _public_kind(path: Path, result: os.stat_result) -> EntryKind:
        kind = LocalStorage._classify_entry(path, result)
        match kind:
            case _RawKind.FILE:
                return EntryKind.FILE
            case _RawKind.DIRECTORY:
                return EntryKind.DIRECTORY
            case _RawKind.SYMLINK:
                return EntryKind.SYMLINK
            case _RawKind.JUNCTION | _RawKind.SPECIAL:
                raise UnsupportedOperationError(_UNSUPPORTED_ERRNO, f"Unsupported filesystem entry: {path}")

    @staticmethod
    def _kind_or_none(path: Path) -> _RawKind | None:
        try:
            result = os.lstat(path)
        except FileNotFoundError:
            return None
        return LocalStorage._classify_entry(path, result)

    def _validate_intermediate_components(self, logical: PurePosixPath) -> None:
        """Reject a symlink, junction, or reparse point in an intermediate component.

        Walking stops at the first missing component: nothing can exist below it,
        so there is nothing further to classify. Callers that then create those
        components (see :meth:`_ensure_parent_directory`) must re-validate
        afterwards, because the newly present components were never checked.
        """
        current = self._root
        for component in logical.relative_to("/").parts[:-1]:
            current /= component
            try:
                result = os.lstat(current)
            except FileNotFoundError:
                return
            kind = self._classify_entry(current, result)
            if kind in {_RawKind.SYMLINK, _RawKind.JUNCTION, _RawKind.SPECIAL}:
                raise ValueError(f"Path contains an intermediate symlink or reparse point: {logical}")

    def _lexical_lstat(self, path: PathLike) -> tuple[PurePosixPath, Path, os.stat_result, EntryKind]:
        logical, local = self._lexical_path(path)
        self._validate_intermediate_components(logical)
        result = os.lstat(local)
        return logical, local, result, self._public_kind(local, result)

    def _lexical_directory(self, path: PathLike) -> tuple[PurePosixPath, Path, os.stat_result, EntryKind]:
        try:
            resolved = self._lexical_lstat(path)
        except FileNotFoundError:
            raise NotADirectoryError(f"Not a directory: {path}") from None
        if resolved[3] is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")
        return resolved

    def _contained_candidate(self, candidate: Path) -> Path:
        normalized = Path(os.path.abspath(os.path.normpath(candidate)))  # noqa: PTH100
        try:
            normalized.relative_to(self._root)
        except ValueError:
            raise PermissionError(errno.EACCES, f"Resolved target escapes storage root: {candidate}") from None
        return normalized

    @staticmethod
    def _native_target_path(raw_target: str) -> Path:
        if os.name == "nt":
            if raw_target.startswith("\\\\?\\UNC\\"):
                raw_target = f"\\\\{raw_target[8:]}"
            elif raw_target.startswith("\\\\?\\"):
                raw_target = raw_target[4:]
        return Path(raw_target)

    @staticmethod
    def _normalize_readlink_target(raw_target: str) -> str:
        """Return a stable OS-native symlink target without extended-path noise."""
        if os.name != "nt":
            return raw_target
        if raw_target.startswith("\\\\?\\UNC\\"):
            return f"\\\\{raw_target[8:]}"
        if raw_target.startswith("\\\\?\\"):
            return raw_target[4:]
        return raw_target

    def _follow(self, path: PathLike) -> tuple[PurePosixPath, Path, os.stat_result, EntryKind]:
        logical, candidate = self._lexical_path(path)
        self._validate_intermediate_components(logical)
        candidate = self._contained_candidate(candidate)
        visited: set[str] = set()
        hops = 0

        while True:
            relative_parts = candidate.relative_to(self._root).parts
            current = self._root
            restarted = False
            final_result = os.lstat(current)

            for index, component in enumerate(relative_parts):
                current /= component
                result = os.lstat(current)
                raw_kind = self._classify_entry(current, result)
                if raw_kind is _RawKind.SYMLINK:
                    key = os.path.normcase(str(current))
                    if key in visited or hops >= _MAX_SYMLINK_HOPS:
                        raise OSError(errno.ELOOP, f"Symlink cycle detected: {logical}")
                    visited.add(key)
                    hops += 1
                    raw_target = os.readlink(current)  # noqa: PTH115
                    target_path = self._native_target_path(raw_target)
                    expanded = target_path if target_path.is_absolute() else current.parent / target_path
                    candidate = self._contained_candidate(expanded.joinpath(*relative_parts[index + 1 :]))
                    restarted = True
                    break
                if raw_kind in {_RawKind.JUNCTION, _RawKind.SPECIAL}:
                    raise UnsupportedOperationError(_UNSUPPORTED_ERRNO, f"Unsupported filesystem entry: {current}")
                final_result = result

            if restarted:
                continue
            return logical, current, final_result, self._public_kind(current, final_result)

    @staticmethod
    def _file_info(
        logical: PurePosixPath,
        result: os.stat_result,
        kind: EntryKind,
        local: Path | None = None,
    ) -> FileInfo:
        if kind is EntryKind.DIRECTORY:
            size = 0
        elif kind is EntryKind.SYMLINK and os.name == "nt" and result.st_size == 0 and local is not None:
            # Windows often reports st_size=0 for reparse points; surface the raw
            # target length so symlink metadata remains useful for callers.
            try:
                size = len(LocalStorage._normalize_readlink_target(os.readlink(local)))  # noqa: PTH115
            except OSError:
                size = 0
        else:
            size = result.st_size
        return FileInfo(
            path=logical.as_posix(),
            name=logical.name,
            kind=kind,
            size=size,
            modified=datetime.fromtimestamp(result.st_mtime).astimezone(),
            created=datetime.fromtimestamp(result.st_ctime).astimezone(),
        )

    @staticmethod
    def _link_directory_hint(result: os.stat_result) -> bool:
        return bool(getattr(result, "st_file_attributes", 0) & _DIRECTORY_ATTRIBUTE)

    @staticmethod
    def _create_symlink_sync(target: str, link_path: Path, *, target_is_directory: bool) -> None:
        try:
            link_path.symlink_to(target, target_is_directory=target_is_directory)
        except OSError as exc:
            if getattr(exc, "winerror", None) == 1314:
                raise PermissionError(errno.EACCES, "Symlink creation is not permitted", link_path) from exc
            raise

    def _ensure_parent_directory(self, logical: PurePosixPath, local: Path, *, create: bool) -> None:
        self._validate_intermediate_components(logical)
        if create:
            local.parent.mkdir(parents=True, exist_ok=True)
            # Components that did not exist during the check above are now
            # present, so re-check the full chain rather than trusting the
            # pre-create snapshot. This narrows, but cannot close, the race a
            # local attacker can run against us (see the class docstring).
            self._validate_intermediate_components(logical)
        parent_result = os.lstat(local.parent)
        if self._public_kind(local.parent, parent_result) is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Parent is not a directory: {logical.parent}")

    # ------------------------------------------------------------------
    # Upload and download
    # ------------------------------------------------------------------

    @override
    async def upload_stream(
        self,
        stream: AsyncIterable[BytesLike],
        remote_path: PathLike,
        *,
        overwrite: bool = True,
    ) -> None:
        logical, target = self._lexical_path(remote_path)
        self._validate_intermediate_components(logical)
        try:
            result = await anyio.to_thread.run_sync(os.lstat, target)
        except FileNotFoundError:
            pass
        else:
            kind = self._public_kind(target, result)
            if kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Is a directory: {remote_path}")
            if kind is EntryKind.SYMLINK:
                raise FileExistsError(f"Upload refuses a symlink destination: {remote_path}")
            if not overwrite:
                raise FileExistsError(f"File already exists: {remote_path}")

        await anyio.to_thread.run_sync(functools.partial(self._ensure_parent_directory, logical, target, create=True))
        temporary = target.parent / f".storegate-local-upload-{uuid.uuid4().hex}"
        try:
            async with await anyio.open_file(temporary, "wb") as file:
                async for chunk in stream:
                    await file.write(bytes(chunk))
            replace = os.replace if overwrite else os.rename
            await anyio.to_thread.run_sync(replace, temporary, target)
        finally:
            with contextlib.suppress(FileNotFoundError):
                await anyio.to_thread.run_sync(temporary.unlink)

    @override
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
        offset = validate_download_offset(offset)
        _logical, target, _result, kind = await anyio.to_thread.run_sync(self._follow, remote_path)
        if kind is not EntryKind.FILE:
            raise FileNotFoundError(f"File not found: {remote_path}")
        async with await anyio.open_file(target, "rb") as file:
            if offset:
                await file.seek(offset)
            while chunk := await file.read(1024 * 1024):
                yield chunk

    # ------------------------------------------------------------------
    # File and symlink operations
    # ------------------------------------------------------------------

    @override
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        try:
            _logical, target, _result, kind = await anyio.to_thread.run_sync(self._lexical_lstat, path)
        except FileNotFoundError:
            if missing_ok:
                return
            raise
        if kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {path}")
        await anyio.to_thread.run_sync(target.unlink)

    @override
    async def rmdir(self, path: PathLike) -> None:
        _logical, target, _result, kind = await anyio.to_thread.run_sync(self._lexical_lstat, path)
        if kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")

        def remove_empty() -> None:
            with os.scandir(target) as entries:
                if next(entries, None) is not None:
                    raise OSError(errno.ENOTEMPTY, f"Directory not empty: {path}")
            target.rmdir()

        await anyio.to_thread.run_sync(remove_empty)

    @staticmethod
    def _copy_leaf_sync(source: Path, destination: Path, kind: EntryKind, *, overwrite: bool) -> None:
        temporary = destination.parent / f".storegate-local-copy-{uuid.uuid4().hex}"
        try:
            if kind is EntryKind.FILE:
                shutil.copy2(source, temporary)
            else:
                source_result = os.lstat(source)
                LocalStorage._create_symlink_sync(
                    os.readlink(source),  # noqa: PTH115
                    temporary,
                    target_is_directory=LocalStorage._link_directory_hint(source_result),
                )
            replace = os.replace if overwrite else os.rename
            replace(temporary, destination)
        finally:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()

    @override
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_logical = self._logical_path(src)
        dst_logical = self._logical_path(dst)
        _src_logical, source, _source_result, source_kind = await anyio.to_thread.run_sync(
            self._lexical_lstat, src_logical
        )
        if validate_same_path_file_operation(
            src_logical,
            dst_logical,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {src}")

        _dst_logical, destination = self._lexical_path(dst_logical)
        await anyio.to_thread.run_sync(
            functools.partial(self._ensure_parent_directory, dst_logical, destination, create=True)
        )
        try:
            destination_result = await anyio.to_thread.run_sync(os.lstat, destination)
        except FileNotFoundError:
            destination_kind = None
        else:
            destination_kind = self._public_kind(destination, destination_result)
            if destination_kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Destination is a directory: {dst}")

        if destination_kind is not None and not overwrite:
            raise FileExistsError(f"Destination already exists: {dst}")
        rename = os.replace if overwrite else os.rename
        try:
            await anyio.to_thread.run_sync(rename, source, destination)
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
            await anyio.to_thread.run_sync(
                functools.partial(
                    self._copy_leaf_sync,
                    source,
                    destination,
                    source_kind,
                    overwrite=overwrite,
                )
            )
            await anyio.to_thread.run_sync(source.unlink)

    @override
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_logical = self._logical_path(src)
        dst_logical = self._logical_path(dst)
        _src_logical, source, _source_result, source_kind = await anyio.to_thread.run_sync(
            self._lexical_lstat, src_logical
        )
        if validate_same_path_file_operation(
            src_logical,
            dst_logical,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {src}")

        _dst_logical, destination = self._lexical_path(dst_logical)
        await anyio.to_thread.run_sync(
            functools.partial(self._ensure_parent_directory, dst_logical, destination, create=True)
        )
        try:
            destination_result = await anyio.to_thread.run_sync(os.lstat, destination)
        except FileNotFoundError:
            destination_kind = None
        else:
            destination_kind = self._public_kind(destination, destination_result)
            if destination_kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Destination is a directory: {dst}")

        if destination_kind is not None and not overwrite:
            raise FileExistsError(f"Destination already exists: {dst}")
        await anyio.to_thread.run_sync(
            functools.partial(
                self._copy_leaf_sync,
                source,
                destination,
                source_kind,
                overwrite=overwrite,
            )
        )

    @override
    async def readlink(self, path: PathLike) -> str:
        _logical, target, _result, kind = await anyio.to_thread.run_sync(self._lexical_lstat, path)
        if kind is not EntryKind.SYMLINK:
            raise OSError(errno.EINVAL, f"Not a symlink: {path}")

        def _read() -> str:
            return LocalStorage._normalize_readlink_target(os.readlink(target))  # noqa: PTH115

        return await anyio.to_thread.run_sync(_read)

    @staticmethod
    def _validate_symlink_target(target: PathLike) -> str:
        raw_target = os.fspath(target)
        if "\0" in raw_target:
            raise ValueError("Symlink target contains a NUL byte")
        drive, _tail = ntpath.splitdrive(raw_target)
        if drive or ntpath.isabs(raw_target) or "\\" in raw_target or PurePosixPath(raw_target).is_absolute():
            raise ValueError("Symlink target must be a relative POSIX path")
        return raw_target

    @override
    async def symlink(
        self,
        target: PathLike,
        link_path: PathLike,
        *,
        target_is_directory: bool = False,
        overwrite: bool = False,
    ) -> None:
        raw_target = self._validate_symlink_target(target)

        logical, destination = self._lexical_path(link_path)
        await anyio.to_thread.run_sync(
            functools.partial(self._ensure_parent_directory, logical, destination, create=False)
        )
        try:
            destination_result = await anyio.to_thread.run_sync(os.lstat, destination)
        except FileNotFoundError:
            destination_kind = None
        else:
            destination_kind = self._public_kind(destination, destination_result)
            if not overwrite:
                raise FileExistsError(f"Destination already exists: {link_path}")
            if destination_kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Destination is a directory: {link_path}")

        temporary = destination.parent / f".storegate-local-link-{uuid.uuid4().hex}"
        try:
            await anyio.to_thread.run_sync(
                functools.partial(
                    self._create_symlink_sync,
                    raw_target,
                    temporary,
                    target_is_directory=target_is_directory,
                )
            )
            replace = os.replace if overwrite else os.rename
            await anyio.to_thread.run_sync(replace, temporary, destination)
        finally:
            with contextlib.suppress(FileNotFoundError):
                await anyio.to_thread.run_sync(temporary.unlink)

    # ------------------------------------------------------------------
    # Directory and tree operations
    # ------------------------------------------------------------------

    @override
    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        logical, target = self._lexical_path(path)
        self._validate_intermediate_components(logical)
        try:
            result = await anyio.to_thread.run_sync(os.lstat, target)
        except FileNotFoundError:
            pass
        else:
            kind = self._public_kind(target, result)
            if kind is EntryKind.DIRECTORY and exist_ok:
                return
            raise FileExistsError(f"Path already exists: {path}")
        if parents:
            await anyio.to_thread.run_sync(functools.partial(target.mkdir, parents=True, exist_ok=False))
        else:
            await anyio.to_thread.run_sync(target.mkdir)

    def _strict_tree_snapshot(self, root: Path) -> tuple[_TreeEntry, ...]:
        root_result = os.lstat(root)
        if self._public_kind(root, root_result) is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {root}")
        snapshot: list[_TreeEntry] = []

        def visit(directory: Path, relative_parts: tuple[str, ...]) -> None:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name)
            for entry in entries:
                child = Path(entry.path)
                result = entry.stat(follow_symlinks=False)
                kind = self._public_kind(child, result)
                child_parts = (*relative_parts, entry.name)
                if kind is EntryKind.SYMLINK:
                    snapshot.append(
                        _TreeEntry(
                            relative_parts=child_parts,
                            kind=kind,
                            link_target=os.readlink(child),  # noqa: PTH115
                            target_is_directory=self._link_directory_hint(result),
                        )
                    )
                else:
                    snapshot.append(_TreeEntry(relative_parts=child_parts, kind=kind))
                    if kind is EntryKind.DIRECTORY:
                        visit(child, child_parts)

        visit(root, ())
        return tuple(snapshot)

    @staticmethod
    def _paths_overlap(source: Path, destination: Path) -> bool:
        return source != destination and (source.is_relative_to(destination) or destination.is_relative_to(source))

    @override
    async def rmtree(self, path: PathLike) -> None:
        _logical, target, _result, kind = await anyio.to_thread.run_sync(self._lexical_lstat, path)
        if kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")
        snapshot = await anyio.to_thread.run_sync(self._strict_tree_snapshot, target)

        def remove_snapshot() -> None:
            for entry in reversed(snapshot):
                child = target.joinpath(*entry.relative_parts)
                if entry.kind is EntryKind.DIRECTORY:
                    child.rmdir()
                else:
                    child.unlink()
            target.rmdir()

        await anyio.to_thread.run_sync(remove_snapshot)

    def _create_destination_parents(self, destination: Path, journal: _MutationJournal) -> None:
        current = self._root
        for component in destination.parent.relative_to(self._root).parts:
            current /= component
            kind = self._kind_or_none(current)
            if kind is None:
                journal.record_created(current)
                current.mkdir()
            elif kind is _RawKind.DIRECTORY:
                continue
            elif kind in {_RawKind.SYMLINK, _RawKind.JUNCTION, _RawKind.SPECIAL}:
                raise ValueError(f"Path contains an intermediate symlink or reparse point: {destination}")
            else:
                raise NotADirectoryError(f"Parent is not a directory: {current}")

    def _rename_tree_sync(self, source: Path, destination: Path) -> bool:
        journal = _MutationJournal.create(self._root)
        try:
            self._create_destination_parents(destination, journal)
            source.rename(destination)
        except OSError as primary:
            try:
                journal.rollback()
            except BaseException as rollback_error:
                raise BaseExceptionGroup("Local movetree parent rollback failed", [primary, rollback_error]) from None
            if primary.errno == errno.EXDEV:
                return False
            raise
        journal.cleanup()
        return True

    def _copytree_sync(
        self,
        source: Path,
        destination: Path,
        source_snapshot: tuple[_TreeEntry, ...],
        *,
        overwrite: bool,
    ) -> None:
        destination_kind = self._kind_or_none(destination)
        if destination_kind in {_RawKind.JUNCTION, _RawKind.SPECIAL}:
            raise UnsupportedOperationError(_UNSUPPORTED_ERRNO, f"Unsupported filesystem entry: {destination}")
        if destination_kind is _RawKind.DIRECTORY:
            self._strict_tree_snapshot(destination)
        if destination_kind is not None and not overwrite:
            raise FileExistsError(f"Destination already exists: {destination}")

        replaced_prefixes: list[tuple[str, ...]] = []
        if destination_kind in {_RawKind.FILE, _RawKind.SYMLINK}:
            replaced_prefixes.append(())
        for entry in source_snapshot:
            if any(entry.relative_parts[: len(prefix)] == prefix for prefix in replaced_prefixes):
                existing = None
            else:
                existing = self._kind_or_none(destination.joinpath(*entry.relative_parts))
            if existing in {_RawKind.JUNCTION, _RawKind.SPECIAL}:
                raise UnsupportedOperationError(
                    _UNSUPPORTED_ERRNO,
                    f"Unsupported filesystem entry: {destination.joinpath(*entry.relative_parts)}",
                )
            if entry.kind is EntryKind.DIRECTORY and existing in {_RawKind.FILE, _RawKind.SYMLINK}:
                replaced_prefixes.append(entry.relative_parts)
            if entry.kind is not EntryKind.DIRECTORY and existing is _RawKind.DIRECTORY:
                raise IsADirectoryError(f"Destination is a directory: {destination.joinpath(*entry.relative_parts)}")

        journal = _MutationJournal.create(self._root)
        try:
            self._create_destination_parents(destination, journal)
            if destination_kind is None:
                journal.record_created(destination)
                destination.mkdir()
            elif destination_kind in {_RawKind.FILE, _RawKind.SYMLINK}:
                journal.backup(destination)
                journal.record_created(destination)
                destination.mkdir()

            for entry in source_snapshot:
                source_child = source.joinpath(*entry.relative_parts)
                destination_child = destination.joinpath(*entry.relative_parts)
                existing = self._kind_or_none(destination_child)
                if entry.kind is EntryKind.DIRECTORY:
                    if existing is None:
                        journal.record_created(destination_child)
                        destination_child.mkdir()
                    elif existing in {_RawKind.FILE, _RawKind.SYMLINK}:
                        journal.backup(destination_child)
                        journal.record_created(destination_child)
                        destination_child.mkdir()
                    continue

                if existing is not None:
                    journal.backup(destination_child)
                journal.record_created(destination_child)
                if entry.kind is EntryKind.FILE:
                    shutil.copy2(source_child, destination_child)
                else:
                    assert entry.link_target is not None
                    self._create_symlink_sync(
                        entry.link_target,
                        destination_child,
                        target_is_directory=entry.target_is_directory,
                    )
        except BaseException as primary:
            try:
                journal.rollback()
            except BaseException as rollback_error:
                raise BaseExceptionGroup("Local copytree rollback failed", [primary, rollback_error]) from None
            raise
        journal.cleanup()

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_logical = self._logical_path(src)
        dst_logical = self._logical_path(dst)
        _src_logical, source = self._lexical_path(src_logical)
        try:
            _src_logical, source, _source_result, source_kind = await anyio.to_thread.run_sync(
                self._lexical_lstat, src_logical
            )
        except FileNotFoundError:
            source_kind = None
        if validate_same_path_tree_operation(
            src_logical,
            dst_logical,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {src_logical}")
        if source_kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {src_logical}")
        _dst_logical, destination = self._lexical_path(dst_logical)
        await anyio.to_thread.run_sync(self._validate_intermediate_components, dst_logical)
        if self._paths_overlap(source, destination):
            raise ValueError("Source and destination trees must not overlap")
        source_snapshot = await anyio.to_thread.run_sync(self._strict_tree_snapshot, source)
        await anyio.to_thread.run_sync(
            functools.partial(
                self._copytree_sync,
                source,
                destination,
                source_snapshot,
                overwrite=overwrite,
            )
        )

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        src_logical = self._logical_path(src)
        dst_logical = self._logical_path(dst)
        _src_logical, source = self._lexical_path(src_logical)
        try:
            _src_logical, source, _source_result, source_kind = await anyio.to_thread.run_sync(
                self._lexical_lstat, src_logical
            )
        except FileNotFoundError:
            source_kind = None
        if validate_same_path_tree_operation(
            src_logical,
            dst_logical,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {src_logical}")
        if source_kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {src_logical}")
        _dst_logical, destination = self._lexical_path(dst_logical)
        await anyio.to_thread.run_sync(self._validate_intermediate_components, dst_logical)
        if self._paths_overlap(source, destination):
            raise ValueError("Source and destination trees must not overlap")

        await anyio.to_thread.run_sync(self._strict_tree_snapshot, source)
        destination_kind = await anyio.to_thread.run_sync(self._kind_or_none, destination)
        if destination_kind is _RawKind.DIRECTORY:
            await anyio.to_thread.run_sync(self._strict_tree_snapshot, destination)
        elif destination_kind in {_RawKind.JUNCTION, _RawKind.SPECIAL}:
            raise UnsupportedOperationError(_UNSUPPORTED_ERRNO, f"Unsupported filesystem entry: {destination}")
        if destination_kind is not None and not overwrite:
            raise FileExistsError(f"Destination already exists: {dst}")
        if destination_kind is None and await anyio.to_thread.run_sync(self._rename_tree_sync, source, destination):
            return
        await self.copytree(src, dst, overwrite=overwrite)
        await self.rmtree(src)

    # ------------------------------------------------------------------
    # Metadata and listing
    # ------------------------------------------------------------------

    @override
    async def exists(self, path: PathLike) -> bool:
        try:
            await self.stat(path)
        except FileNotFoundError:
            return False
        return True

    @override
    async def is_file(self, path: PathLike) -> bool:
        try:
            return (await self.stat(path)).is_file
        except FileNotFoundError:
            return False

    @override
    async def is_dir(self, path: PathLike) -> bool:
        try:
            return (await self.stat(path)).is_dir
        except FileNotFoundError:
            return False

    @override
    async def stat(self, path: PathLike) -> FileInfo:
        logical, target, result, kind = await anyio.to_thread.run_sync(self._follow, path)
        return self._file_info(logical, result, kind, target)

    @override
    async def lstat(self, path: PathLike) -> FileInfo:
        logical, target, result, kind = await anyio.to_thread.run_sync(self._lexical_lstat, path)
        return self._file_info(logical, result, kind, target)

    def _discovery_snapshot(self, logical: PurePosixPath, target: Path) -> tuple[FileInfo, ...]:
        result = os.lstat(target)
        if self._public_kind(target, result) is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {logical}")
        infos: list[FileInfo] = []
        with os.scandir(target) as iterator:
            entries = list(iterator)
        for entry in entries:
            child = Path(entry.path)
            child_result = entry.stat(follow_symlinks=False)
            raw_kind = self._classify_entry(child, child_result)
            if raw_kind in {_RawKind.JUNCTION, _RawKind.SPECIAL}:
                self.log.debug(f"Skipping unsupported local entry <y>{child}</>")
                continue
            kind = EntryKind(raw_kind.value)
            infos.append(self._file_info(logical / entry.name, child_result, kind, child))
        return tuple(sorted(infos, key=lambda info: info.path))

    @override
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        logical, target, _result, _kind = await anyio.to_thread.run_sync(self._lexical_directory, path)
        snapshot = await anyio.to_thread.run_sync(self._discovery_snapshot, logical, target)
        for info in snapshot:
            yield info

    @override
    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        logical, target, _result, _kind = await anyio.to_thread.run_sync(self._lexical_directory, path)
        snapshot = await anyio.to_thread.run_sync(self._discovery_snapshot, logical, target)
        yield WalkEntry(path=logical.as_posix(), entries=snapshot)
        for info in snapshot:
            if info.kind is EntryKind.DIRECTORY:
                async for entry in self.walk(info.path):
                    yield entry
