from __future__ import annotations

import errno
import itertools
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, Iterable
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import final, override

from storegate.storage.abstract import (
    AbstractStorage,
    BytesLike,
    EntryKind,
    FileInfo,
    PathLike,
    StorageCapabilities,
    VersionedBytes,
    WalkEntry,
    make_namespace_identity,
    validate_download_offset,
    validate_same_path_file_operation,
    validate_same_path_tree_operation,
)

_sid = itertools.count()
_MEMORY_CAPABILITIES = StorageCapabilities(
    symlink_metadata=True,
    readlink=True,
    symlink_create=True,
    compare_exchange=True,
)
_MAX_SYMLINK_HOPS = 40


@final
class MemoryStorage(AbstractStorage):
    """In-memory storage backend for testing and ephemeral use."""

    def __init__(self, root: str | None = None) -> None:
        super().__init__()
        raw_root = PurePosixPath(root or "/")
        if "\x00" in raw_root.as_posix():
            raise ValueError("Memory root must not contain NUL")
        if ".." in raw_root.parts:
            raise ValueError("Memory root must not contain '..' segments")
        root_parts = tuple(part for part in raw_root.parts if part not in {"/", "."})
        self._root = PurePosixPath("/", *root_parts)
        self._files: dict[str, bytes] = {}
        self._file_tokens: dict[str, str] = {}
        self._dirs: set[str] = {self._root.as_posix()}
        self._links: dict[str, str] = {}
        self._now = datetime.now(tz=UTC)
        self._id = next(_sid)

    # ------------------------------------------------------------------
    # Identity and lifecycle
    # ------------------------------------------------------------------

    @property
    @override
    def display_id(self) -> str:
        return f"memory:{self._id}:{self._root.as_posix()}"

    @property
    @override
    def namespace_identity(self) -> str:
        return make_namespace_identity("memory", instance=self._id, root=self._root.as_posix())

    @property
    @override
    def capabilities(self) -> StorageCapabilities:
        return _MEMORY_CAPABILITIES

    @override
    async def connect(self) -> None:
        pass

    @override
    async def close(self) -> None:
        pass

    @override
    async def ping(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Paths and namespace
    # ------------------------------------------------------------------

    def _logical_path(self, path: PathLike) -> PurePosixPath:
        raw = PurePosixPath(path)
        if "\x00" in raw.as_posix():
            raise ValueError("Memory path must not contain NUL")
        if ".." in raw.parts:
            raise ValueError("Memory path must not contain '..' segments")
        return self.normalize_path(raw)

    def _backend_path(self, path: PathLike) -> PurePosixPath:
        logical = self._logical_path(path)
        relative = logical.relative_to("/")
        return self._root if relative == PurePosixPath(".") else self._root / relative

    def _resolve(self, path: PathLike) -> str:
        """Return the lexical backend-native key used by fixture injection."""
        return self._backend_path(path).as_posix()

    def _logical_from_backend(self, path: PurePosixPath) -> PurePosixPath:
        relative = path.relative_to(self._root)
        return PurePosixPath("/") if relative == PurePosixPath(".") else PurePosixPath("/") / relative

    @staticmethod
    def _is_descendant(path: PurePosixPath, parent: PurePosixPath) -> bool:
        return path != parent and parent in path.parents

    def _assert_contained(self, path: PurePosixPath, *, raw_target: str | None = None) -> None:
        try:
            path.relative_to(self._root)
        except ValueError:
            message = f"Symlink target escapes storage root: {raw_target!r}" if raw_target is not None else str(path)
            raise PermissionError(errno.EACCES, message) from None

    @staticmethod
    def _canonical_native(path: PurePosixPath) -> PurePosixPath:
        parts: list[str] = []
        for part in path.parts:
            if part in {"", "/", "."}:
                continue
            if part == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(part)
        return PurePosixPath("/", *parts)

    def _entry_kind(self, path: PurePosixPath) -> EntryKind | None:
        key = path.as_posix()
        kinds = (
            EntryKind.FILE if key in self._files else None,
            EntryKind.DIRECTORY if key == self._root.as_posix() or key in self._dirs else None,
            EntryKind.SYMLINK if key in self._links else None,
        )
        present = tuple(kind for kind in kinds if kind is not None)
        if len(present) > 1:
            raise RuntimeError(f"Memory namespace invariant violated at {path}")
        return present[0] if present else None

    def _assert_no_intermediate_links(self, path: PurePosixPath) -> None:
        relative = path.relative_to(self._root)
        current = self._root
        for part in relative.parts[:-1]:
            current /= part
            kind = self._entry_kind(current)
            if kind is EntryKind.SYMLINK:
                raise OSError(errno.ELOOP, f"Intermediate path is a symlink: {self._logical_from_backend(current)}")
            if kind is EntryKind.FILE:
                raise NotADirectoryError(f"Intermediate path is not a directory: {self._logical_from_backend(current)}")

    def _path_pair(self, path: PathLike) -> tuple[PurePosixPath, PurePosixPath]:
        logical = self._logical_path(path)
        backend = self._backend_path(logical)
        self._assert_no_intermediate_links(backend)
        return logical, backend

    def _missing_parent_dirs(self, path: PurePosixPath) -> tuple[PurePosixPath, ...]:
        relative = path.parent.relative_to(self._root)
        current = self._root
        missing: list[PurePosixPath] = []
        for part in relative.parts:
            current /= part
            kind = self._entry_kind(current)
            if kind is EntryKind.SYMLINK:
                raise OSError(errno.ELOOP, f"Intermediate path is a symlink: {self._logical_from_backend(current)}")
            if kind is EntryKind.FILE:
                raise NotADirectoryError(f"Intermediate path is not a directory: {self._logical_from_backend(current)}")
            if kind is None:
                missing.append(current)
        return tuple(missing)

    def _require_parent_directory(self, path: PurePosixPath) -> None:
        parent = path.parent
        kind = self._entry_kind(parent)
        if kind is EntryKind.SYMLINK:
            raise OSError(errno.ELOOP, f"Parent path is a symlink: {self._logical_from_backend(parent)}")
        if kind is not EntryKind.DIRECTORY:
            if kind is None:
                raise FileNotFoundError(f"Parent directory not found: {self._logical_from_backend(path)}")
            raise NotADirectoryError(f"Parent path is not a directory: {self._logical_from_backend(parent)}")

    def _first_link(self, path: PurePosixPath) -> PurePosixPath | None:
        self._assert_contained(path)
        relative = path.relative_to(self._root)
        current = self._root
        for part in relative.parts:
            current /= part
            if current.as_posix() in self._links:
                return current
        return None

    def _link_target(self, link_path: PurePosixPath, raw_target: str, suffix: tuple[str, ...]) -> PurePosixPath:
        if "\x00" in raw_target:
            raise ValueError("Memory symlink target must not contain NUL")
        target = PurePosixPath(raw_target)
        base = target if target.is_absolute() else link_path.parent / target
        candidate = self._canonical_native(base.joinpath(*suffix))
        self._assert_contained(candidate, raw_target=raw_target)
        return candidate

    def _follow(self, path: PurePosixPath) -> PurePosixPath:
        candidate = path
        visited: set[str] = set()
        hops = 0
        while link_path := self._first_link(candidate):
            key = link_path.as_posix()
            if key in visited or hops >= _MAX_SYMLINK_HOPS:
                raise OSError(
                    errno.ELOOP,
                    f"Too many symbolic links while resolving {self._logical_from_backend(path)}",
                )
            visited.add(key)
            hops += 1
            suffix = candidate.relative_to(link_path).parts
            candidate = self._link_target(link_path, self._links[key], suffix)
        return candidate

    def _info(self, backend: PurePosixPath, logical: PurePosixPath, kind: EntryKind) -> FileInfo:
        key = backend.as_posix()
        size = len(self._files[key]) if kind is EntryKind.FILE else 0
        if kind is EntryKind.SYMLINK:
            size = len(self._links[key].encode())
        return FileInfo(
            path=logical.as_posix(),
            name=logical.name,
            kind=kind,
            size=size,
            modified=self._now,
            created=self._now,
        )

    def _direct_children(self, directory: PurePosixPath) -> tuple[tuple[PurePosixPath, EntryKind], ...]:
        children: list[tuple[PurePosixPath, EntryKind]] = []
        directory_key = directory.as_posix()
        for key in self._dirs:
            child = PurePosixPath(key)
            if child != directory and child.parent == directory:
                children.append((child, EntryKind.DIRECTORY))
        for key in self._files:
            child = PurePosixPath(key)
            if child.parent == directory:
                children.append((child, EntryKind.FILE))
        for key in self._links:
            child = PurePosixPath(key)
            if child.parent == directory:
                children.append((child, EntryKind.SYMLINK))
        children.sort(key=lambda item: item[0].as_posix())
        for child, _ in children:
            if child.parent.as_posix() != directory_key:
                raise RuntimeError("Memory namespace child invariant violated")
        return tuple(children)

    def _tree_entries(self, root: PurePosixPath) -> tuple[tuple[PurePosixPath, EntryKind], ...]:
        kind = self._entry_kind(root)
        if kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {self._logical_from_backend(root)}")
        entries: list[tuple[PurePosixPath, EntryKind]] = [(root, EntryKind.DIRECTORY)]
        for key in self._dirs:
            path = PurePosixPath(key)
            if self._is_descendant(path, root):
                entries.append((path, EntryKind.DIRECTORY))
        for key in self._files:
            path = PurePosixPath(key)
            if self._is_descendant(path, root):
                entries.append((path, EntryKind.FILE))
        for key in self._links:
            path = PurePosixPath(key)
            if self._is_descendant(path, root):
                entries.append((path, EntryKind.SYMLINK))
        entries.sort(key=lambda item: item[0].as_posix())
        return tuple(entries)

    @staticmethod
    def _remove_entry(
        path: PurePosixPath,
        files: dict[str, bytes],
        dirs: set[str],
        links: dict[str, str],
        file_tokens: dict[str, str],
    ) -> None:
        key = path.as_posix()
        files.pop(key, None)
        dirs.discard(key)
        links.pop(key, None)
        file_tokens.pop(key, None)

    @staticmethod
    def _new_file_token() -> str:
        return uuid.uuid4().hex

    def _ensure_dirs_in_state(
        self,
        directories: Iterable[PurePosixPath],
        files: dict[str, bytes],
        dirs: set[str],
        links: dict[str, str],
    ) -> None:
        for directory in directories:
            key = directory.as_posix()
            if key in files or key in links:
                raise NotADirectoryError(f"Parent path is not a directory: {self._logical_from_backend(directory)}")
            dirs.add(key)

    def _copytree_state(
        self,
        source: PurePosixPath,
        destination: PurePosixPath,
        *,
        overwrite: bool,
    ) -> tuple[dict[str, bytes], set[str], dict[str, str], dict[str, str]]:
        snapshot = self._tree_entries(source)
        if self._is_descendant(destination, source):
            raise ValueError("Destination must not be inside the source tree")

        files = self._files.copy()
        dirs = self._dirs.copy()
        links = self._links.copy()
        file_tokens = self._file_tokens.copy()
        destination_kind = self._entry_kind(destination)
        if destination_kind is not None and not overwrite:
            raise FileExistsError(f"Destination already exists: {self._logical_from_backend(destination)}")

        parent_dirs = self._missing_parent_dirs(destination)
        self._ensure_dirs_in_state(parent_dirs, files, dirs, links)
        if destination_kind in {EntryKind.FILE, EntryKind.SYMLINK}:
            self._remove_entry(destination, files, dirs, links, file_tokens)
        dirs.add(destination.as_posix())

        for source_path, source_kind in snapshot[1:]:
            relative = source_path.relative_to(source)
            target = destination / relative
            target_key = target.as_posix()
            target_kind = (
                EntryKind.FILE
                if target_key in files
                else EntryKind.DIRECTORY
                if target_key in dirs
                else EntryKind.SYMLINK
                if target_key in links
                else None
            )
            if source_kind is EntryKind.DIRECTORY:
                if target_kind in {EntryKind.FILE, EntryKind.SYMLINK}:
                    if not overwrite:
                        raise FileExistsError(f"Destination already exists: {self._logical_from_backend(target)}")
                    self._remove_entry(target, files, dirs, links, file_tokens)
                dirs.add(target_key)
                continue
            if target_kind is EntryKind.DIRECTORY:
                raise IsADirectoryError(f"Destination is a directory: {self._logical_from_backend(target)}")
            if target_kind is not None and not overwrite:
                raise FileExistsError(f"Destination already exists: {self._logical_from_backend(target)}")
            self._remove_entry(target, files, dirs, links, file_tokens)
            source_key = source_path.as_posix()
            if source_kind is EntryKind.FILE:
                files[target_key] = self._files[source_key]
                file_tokens[target_key] = self._file_tokens[source_key]
            else:
                links[target_key] = self._links[source_key]
        return files, dirs, links, file_tokens

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
        _, target = self._path_pair(remote_path)
        kind = self._entry_kind(target)
        if kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {remote_path}")
        if kind is EntryKind.SYMLINK:
            raise OSError(errno.ELOOP, f"Refusing to upload through symlink: {remote_path}")
        if kind is EntryKind.FILE and not overwrite:
            raise FileExistsError(f"File already exists: {remote_path}")
        missing_parents = self._missing_parent_dirs(target)

        buffer = bytearray()
        async for chunk in stream:
            buffer.extend(chunk)

        for directory in missing_parents:
            self._dirs.add(directory.as_posix())
        key = target.as_posix()
        self._files[key] = bytes(buffer)
        self._file_tokens[key] = self._new_file_token()

    @override
    async def read_versioned(self, path: PathLike) -> VersionedBytes | None:
        _, lexical = self._path_pair(path)
        target = self._follow(lexical)
        kind = self._entry_kind(target)
        if kind is None:
            return None
        if kind is not EntryKind.FILE:
            raise IsADirectoryError(f"Not a regular file: {path}")
        key = target.as_posix()
        token = self._file_tokens.get(key)
        if token is None:
            token = self._new_file_token()
            self._file_tokens[key] = token
        return VersionedBytes(data=self._files[key], token=token)

    @override
    async def compare_exchange(
        self,
        path: PathLike,
        *,
        expected_token: str | None,
        data: BytesLike,
    ) -> VersionedBytes | None:
        _, target = self._path_pair(path)
        kind = self._entry_kind(target)
        if kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {path}")
        if kind is EntryKind.SYMLINK:
            raise OSError(errno.ELOOP, f"Refusing to compare-exchange through symlink: {path}")

        key = target.as_posix()
        current_token = self._file_tokens.get(key) if kind is EntryKind.FILE else None
        if expected_token is None:
            if kind is EntryKind.FILE:
                return None
        elif current_token != expected_token:
            return None

        missing_parents = self._missing_parent_dirs(target)
        for directory in missing_parents:
            self._dirs.add(directory.as_posix())

        payload = bytes(data)
        token = self._new_file_token()
        self._files[key] = payload
        self._file_tokens[key] = token
        return VersionedBytes(data=payload, token=token)

    @override
    async def download_stream(
        self,
        remote_path: PathLike,
        *,
        offset: int = 0,
    ) -> AsyncGenerator[bytes]:
        offset = validate_download_offset(offset)
        _, lexical = self._path_pair(remote_path)
        target = self._follow(lexical)
        kind = self._entry_kind(target)
        if kind is None:
            raise FileNotFoundError(f"File not found: {remote_path}")
        if kind is not EntryKind.FILE:
            raise IsADirectoryError(f"Not a regular file: {remote_path}")
        data = self._files[target.as_posix()]
        step = 1024 * 1024
        for index in range(offset, len(data), step):
            yield data[index : index + step]

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    @override
    async def unlink(self, path: PathLike, *, missing_ok: bool = False) -> None:
        _, target = self._path_pair(path)
        kind = self._entry_kind(target)
        if kind is EntryKind.FILE:
            key = target.as_posix()
            del self._files[key]
            self._file_tokens.pop(key, None)
            return
        if kind is EntryKind.SYMLINK:
            del self._links[target.as_posix()]
            return
        if kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {path}")
        if not missing_ok:
            raise FileNotFoundError(f"File not found: {path}")

    @override
    async def rmdir(self, path: PathLike) -> None:
        _, target = self._path_pair(path)
        if self._entry_kind(target) is not EntryKind.DIRECTORY:
            if self._entry_kind(target) is None:
                raise FileNotFoundError(f"Directory not found: {path}")
            raise NotADirectoryError(f"Not a directory: {path}")
        if self._direct_children(target):
            raise OSError(errno.ENOTEMPTY, f"Directory not empty: {path}")
        if target != self._root:
            self._dirs.discard(target.as_posix())

    def _prepare_entry_destination(
        self,
        destination: PurePosixPath,
        *,
        overwrite: bool,
    ) -> tuple[PurePosixPath, ...]:
        destination_kind = self._entry_kind(destination)
        if destination_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Destination is a directory: {self._logical_from_backend(destination)}")
        if destination_kind is not None and not overwrite:
            raise FileExistsError(f"Destination already exists: {self._logical_from_backend(destination)}")
        return self._missing_parent_dirs(destination)

    @override
    async def move(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source_logical, source = self._path_pair(src)
        destination_logical, destination = self._path_pair(dst)
        source_kind = self._entry_kind(source)
        if validate_same_path_file_operation(
            source_logical,
            destination_logical,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {source_logical}")
        if source_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {source_logical}")
        missing_parents = self._prepare_entry_destination(
            destination,
            overwrite=overwrite,
        )
        for directory in missing_parents:
            self._dirs.add(directory.as_posix())
        self._remove_entry(destination, self._files, self._dirs, self._links, self._file_tokens)
        if source_kind is EntryKind.FILE:
            source_key = source.as_posix()
            destination_key = destination.as_posix()
            self._files[destination_key] = self._files.pop(source_key)
            self._file_tokens[destination_key] = self._file_tokens.pop(source_key)
        else:
            self._links[destination.as_posix()] = self._links.pop(source.as_posix())

    @override
    async def copy(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source_logical, source = self._path_pair(src)
        destination_logical, destination = self._path_pair(dst)
        source_kind = self._entry_kind(source)
        if validate_same_path_file_operation(
            source_logical,
            destination_logical,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {source_logical}")
        if source_kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Is a directory: {source_logical}")
        missing_parents = self._prepare_entry_destination(
            destination,
            overwrite=overwrite,
        )
        for directory in missing_parents:
            self._dirs.add(directory.as_posix())
        self._remove_entry(destination, self._files, self._dirs, self._links, self._file_tokens)
        if source_kind is EntryKind.FILE:
            source_key = source.as_posix()
            destination_key = destination.as_posix()
            self._files[destination_key] = self._files[source_key]
            self._file_tokens[destination_key] = self._file_tokens[source_key]
        else:
            self._links[destination.as_posix()] = self._links[source.as_posix()]

    # ------------------------------------------------------------------
    # Directory operations
    # ------------------------------------------------------------------

    @override
    async def mkdir(
        self,
        path: PathLike,
        *,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        _, target = self._path_pair(path)
        kind = self._entry_kind(target)
        if kind is EntryKind.DIRECTORY:
            if exist_ok:
                return
            raise FileExistsError(f"Directory already exists: {path}")
        if kind is not None:
            raise FileExistsError(f"Path already exists: {path}")

        if parents:
            missing = self._missing_parent_dirs(target)
            for directory in missing:
                self._dirs.add(directory.as_posix())
        else:
            self._require_parent_directory(target)
        self._dirs.add(target.as_posix())

    @override
    async def rmtree(self, path: PathLike) -> None:
        _, target = self._path_pair(path)
        snapshot = self._tree_entries(target)
        files = self._files.copy()
        dirs = self._dirs.copy()
        links = self._links.copy()
        file_tokens = self._file_tokens.copy()
        for entry, _ in reversed(snapshot):
            self._remove_entry(entry, files, dirs, links, file_tokens)
        dirs.add(self._root.as_posix())
        self._files, self._dirs, self._links, self._file_tokens = files, dirs, links, file_tokens

    @override
    async def copytree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source_logical, source = self._path_pair(src)
        destination_logical, destination = self._path_pair(dst)
        source_kind = self._entry_kind(source)
        if validate_same_path_tree_operation(
            source_logical,
            destination_logical,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {source_logical}")
        if source_kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {source_logical}")
        files, dirs, links, file_tokens = self._copytree_state(source, destination, overwrite=overwrite)
        self._files, self._dirs, self._links, self._file_tokens = files, dirs, links, file_tokens

    @override
    async def movetree(self, src: PathLike, dst: PathLike, *, overwrite: bool = True) -> None:
        source_logical, source = self._path_pair(src)
        destination_logical, destination = self._path_pair(dst)
        source_kind = self._entry_kind(source)
        if validate_same_path_tree_operation(
            source_logical,
            destination_logical,
            source_kind=source_kind,
            overwrite=overwrite,
        ):
            return
        if source_kind is None:
            raise FileNotFoundError(f"Source not found: {source_logical}")
        if source_kind is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {source_logical}")
        files, dirs, links, file_tokens = self._copytree_state(source, destination, overwrite=overwrite)
        source_snapshot = self._tree_entries(source)
        for entry, _ in reversed(source_snapshot):
            self._remove_entry(entry, files, dirs, links, file_tokens)
        dirs.add(self._root.as_posix())
        self._files, self._dirs, self._links, self._file_tokens = files, dirs, links, file_tokens

    # ------------------------------------------------------------------
    # Metadata and symlinks
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
            return (await self.stat(path)).kind is EntryKind.FILE
        except FileNotFoundError:
            return False

    @override
    async def is_dir(self, path: PathLike) -> bool:
        try:
            return (await self.stat(path)).kind is EntryKind.DIRECTORY
        except FileNotFoundError:
            return False

    @override
    async def lstat(self, path: PathLike) -> FileInfo:
        logical, target = self._path_pair(path)
        kind = self._entry_kind(target)
        if kind is None:
            raise FileNotFoundError(f"Path not found: {path}")
        return self._info(target, logical, kind)

    @override
    async def stat(self, path: PathLike) -> FileInfo:
        logical, lexical = self._path_pair(path)
        target = self._follow(lexical)
        kind = self._entry_kind(target)
        if kind is None:
            raise FileNotFoundError(f"Path not found: {path}")
        if kind is EntryKind.SYMLINK:
            raise RuntimeError("Memory symlink resolver returned an unresolved link")
        return self._info(target, logical, kind)

    @override
    async def is_symlink(self, path: PathLike) -> bool:
        try:
            return (await self.lstat(path)).kind is EntryKind.SYMLINK
        except FileNotFoundError:
            return False

    @override
    async def readlink(self, path: PathLike) -> str:
        _, target = self._path_pair(path)
        try:
            return self._links[target.as_posix()]
        except KeyError:
            if self._entry_kind(target) is None:
                raise FileNotFoundError(f"Path not found: {path}") from None
            raise OSError(errno.EINVAL, f"Not a symbolic link: {path}") from None

    @override
    async def symlink(
        self,
        target: PathLike,
        link_path: PathLike,
        *,
        target_is_directory: bool = False,
        overwrite: bool = False,
    ) -> None:
        del target_is_directory
        raw_target = str(target)
        if "\x00" in raw_target:
            raise ValueError("Memory symlink target must not contain NUL")
        if PurePosixPath(raw_target).is_absolute():
            raise ValueError("Memory symlink target must be relative")
        _, link = self._path_pair(link_path)
        self._require_parent_directory(link)
        kind = self._entry_kind(link)
        if kind is not None and not overwrite:
            raise FileExistsError(f"Destination already exists: {link_path}")
        if kind is EntryKind.DIRECTORY:
            raise IsADirectoryError(f"Destination is a directory: {link_path}")
        self._remove_entry(link, self._files, self._dirs, self._links, self._file_tokens)
        self._links[link.as_posix()] = raw_target

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    @override
    async def iterdir(self, path: PathLike) -> AsyncGenerator[FileInfo]:
        logical, target = self._path_pair(path)
        if self._entry_kind(target) is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")
        for child, kind in self._direct_children(target):
            child_logical = logical / child.name
            yield self._info(child, child_logical, kind)

    @override
    async def walk(self, path: PathLike) -> AsyncGenerator[WalkEntry]:
        logical, target = self._path_pair(path)
        if self._entry_kind(target) is not EntryKind.DIRECTORY:
            raise NotADirectoryError(f"Not a directory: {path}")

        async def visit(directory: PurePosixPath, directory_logical: PurePosixPath) -> AsyncGenerator[WalkEntry]:
            children = self._direct_children(directory)
            entries = tuple(self._info(child, directory_logical / child.name, kind) for child, kind in children)
            yield WalkEntry(path=directory_logical.as_posix(), entries=entries)
            for child, kind in children:
                if kind is EntryKind.DIRECTORY:
                    async for result in visit(child, directory_logical / child.name):
                        yield result

        async for result in visit(target, logical):
            yield result
