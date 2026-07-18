"""Capability-gated symbolic-link contracts for AbstractStorage."""

import contextlib
import errno

import pytest

from app.storage import AbstractStorage, EntryKind, FileInfo, UnsupportedOperationError, WalkEntry
from app.storage.cached import CachedStorage
from app.storage.local import LocalStorage
from app.storage.memory import MemoryStorage
from app.storage.sftp import SFTPStorage
from tests.support.ids import uid

_UNSUPPORTED_ERRNOS = {errno.EOPNOTSUPP, getattr(errno, "ENOTSUP", errno.EOPNOTSUPP)}


def _require_capabilities(
    storage: AbstractStorage,
    *,
    metadata: bool = False,
    readlink: bool = False,
    create: bool = False,
) -> None:
    capabilities = storage.capabilities
    missing = [
        name
        for name, required, available in (
            ("symlink_metadata", metadata, capabilities.symlink_metadata),
            ("readlink", readlink, capabilities.readlink),
            ("symlink_create", create, capabilities.symlink_create),
        )
        if required and not available
    ]
    if missing:
        pytest.skip(f"storage lacks required capabilities: {", ".join(missing)}")


def _uses_host_filesystem_symlinks(storage: AbstractStorage) -> bool:
    """Return whether create operations need a real host-OS symlink."""
    if isinstance(storage, MemoryStorage):
        return False
    if isinstance(storage, CachedStorage):
        return _uses_host_filesystem_symlinks(storage._storage)
    return isinstance(storage, (LocalStorage, SFTPStorage))


def _require_host_symlink_create(
    storage: AbstractStorage,
    request: pytest.FixtureRequest,
) -> None:
    """Skip create-dependent contracts when host FS cannot create symlinks.

    Pure in-memory backends (MemoryStorage and CachedStorage over Memory) always
    run. LocalStorage and SFTPStorage need host privilege; SFTP also needs the
    fixture's own ``symlink_supported`` probe.
    """
    if not _uses_host_filesystem_symlinks(storage):
        return

    host_ok = request.getfixturevalue("host_symlink_create")
    if not host_ok:
        pytest.skip("host filesystem denied symlink creation")

    if isinstance(storage, SFTPStorage):
        server = request.getfixturevalue("_sftp_server")
        if not server.symlink_supported:
            pytest.skip("host filesystem denied symlink creation for the SFTP fixture")


async def _cleanup_tree(storage: AbstractStorage, path: str) -> None:
    with contextlib.suppress(Exception):
        await storage.rmtree(path)


async def _collect_walk(storage: AbstractStorage, path: str) -> list[WalkEntry]:
    return [entry async for entry in storage.walk(path)]


_INTERMEDIATE_OPERATIONS = (
    "stat",
    "lstat",
    "exists",
    "is-file",
    "is-dir",
    "is-symlink",
    "readlink",
    "download",
    "upload",
    "delete",
    "unlink",
    "rmdir",
    "rmtree",
    "copy-source",
    "copy-destination",
    "move-source",
    "move-destination",
    "copytree-source",
    "copytree-destination",
    "movetree-source",
    "movetree-destination",
    "mkdir-parent",
    "symlink-parent",
    "iterdir",
    "list",
    "walk",
)


def _assert_entry_kind(info: FileInfo, expected: EntryKind) -> None:
    assert info.kind is expected
    assert (info.is_file, info.is_dir, info.is_symlink) == {
        EntryKind.FILE: (True, False, False),
        EntryKind.DIRECTORY: (False, True, False),
        EntryKind.SYMLINK: (False, False, True),
    }[expected]


async def _assert_lexically_missing(storage: AbstractStorage, path: str) -> None:
    with pytest.raises(FileNotFoundError):
        await storage.lstat(path)


async def _prepare_intermediate_fixture(storage: AbstractStorage, base: str) -> None:
    real = f"{base}/real"
    await storage.mkdir(f"{real}/subtree", parents=True)
    await storage.mkdir(f"{real}/empty")
    await storage.upload_bytes(b"original", f"{real}/data.txt")
    await storage.upload_bytes(b"child", f"{real}/subtree/child.txt")
    await storage.symlink("data.txt", f"{real}/inner-link")
    await storage.symlink("real", f"{base}/alias", target_is_directory=True)
    await storage.upload_bytes(b"source", f"{base}/source.txt")
    await storage.mkdir(f"{base}/source-tree")
    await storage.upload_bytes(b"source-tree", f"{base}/source-tree/child.txt")


async def _run_intermediate_operation(storage: AbstractStorage, base: str, operation: str) -> None:
    alias = f"{base}/alias"
    file_path = f"{alias}/data.txt"
    tree_path = f"{alias}/subtree"
    match operation:
        case "stat":
            await storage.stat(file_path)
        case "lstat":
            await storage.lstat(file_path)
        case "exists":
            await storage.exists(file_path)
        case "is-file":
            await storage.is_file(file_path)
        case "is-dir":
            await storage.is_dir(tree_path)
        case "is-symlink":
            await storage.is_symlink(f"{alias}/inner-link")
        case "readlink":
            await storage.readlink(f"{alias}/inner-link")
        case "download":
            await storage.download_bytes(file_path)
        case "upload":
            await storage.upload_bytes(b"replacement", f"{alias}/uploaded.txt")
        case "delete":
            await storage.delete(file_path)
        case "unlink":
            await storage.unlink(file_path)
        case "rmdir":
            await storage.rmdir(f"{alias}/empty")
        case "rmtree":
            await storage.rmtree(tree_path)
        case "copy-source":
            await storage.copy(file_path, f"{base}/copy-out.txt")
        case "copy-destination":
            await storage.copy(f"{base}/source.txt", f"{alias}/copied.txt")
        case "move-source":
            await storage.move(file_path, f"{base}/move-out.txt")
        case "move-destination":
            await storage.move(f"{base}/source.txt", f"{alias}/moved.txt")
        case "copytree-source":
            await storage.copytree(tree_path, f"{base}/copy-tree-out")
        case "copytree-destination":
            await storage.copytree(f"{base}/source-tree", f"{alias}/copied-tree")
        case "movetree-source":
            await storage.movetree(tree_path, f"{base}/move-tree-out")
        case "movetree-destination":
            await storage.movetree(f"{base}/source-tree", f"{alias}/moved-tree")
        case "mkdir-parent":
            await storage.mkdir(f"{alias}/new-dir")
        case "symlink-parent":
            await storage.symlink("data.txt", f"{alias}/new-link")
        case "iterdir":
            _ = [entry async for entry in storage.iterdir(tree_path)]
        case "list":
            await storage.list_(tree_path)
        case "walk":
            await _collect_walk(storage, tree_path)
        case _:
            pytest.fail(f"unhandled operation: {operation}")


async def _assert_intermediate_rejected(storage: AbstractStorage, base: str, operation: str) -> None:
    caught: ValueError | OSError | None = None
    try:
        await _run_intermediate_operation(storage, base, operation)
    except (ValueError, OSError) as exc:
        caught = exc

    if caught is None:
        pytest.fail(f"intermediate symlink operation succeeded: {operation}")
    if isinstance(caught, ValueError):
        message = str(caught).lower()
        assert "intermediate" in message
        assert "symlink" in message or "reparse point" in message
        return
    assert not isinstance(caught, FileNotFoundError)
    assert caught.errno in {errno.ELOOP, errno.EACCES}


async def _assert_intermediate_fixture_unchanged(storage: AbstractStorage, base: str) -> None:
    real = f"{base}/real"
    alias = f"{base}/alias"
    _assert_entry_kind(await storage.lstat(alias), EntryKind.SYMLINK)
    assert await storage.readlink(alias) == "real"
    assert await storage.download_bytes(f"{real}/data.txt") == b"original"
    assert await storage.download_bytes(f"{real}/subtree/child.txt") == b"child"
    _assert_entry_kind(await storage.lstat(f"{real}/empty"), EntryKind.DIRECTORY)
    _assert_entry_kind(await storage.lstat(f"{real}/inner-link"), EntryKind.SYMLINK)
    assert await storage.readlink(f"{real}/inner-link") == "data.txt"
    assert await storage.download_bytes(f"{base}/source.txt") == b"source"
    assert await storage.download_bytes(f"{base}/source-tree/child.txt") == b"source-tree"
    for path in (
        f"{real}/uploaded.txt",
        f"{real}/new-dir",
        f"{real}/copied.txt",
        f"{real}/moved.txt",
        f"{real}/copied-tree",
        f"{real}/moved-tree",
        f"{real}/new-link",
        f"{base}/copy-out.txt",
        f"{base}/move-out.txt",
        f"{base}/copy-tree-out",
        f"{base}/move-tree-out",
    ):
        await _assert_lexically_missing(storage, path)


class TestSymlinkPrimitives:
    async def test_relative_create_and_readlink(self, storage: AbstractStorage, request: pytest.FixtureRequest) -> None:
        _require_capabilities(storage, readlink=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-relative-{uid()}"
        try:
            await storage.mkdir(base)
            await storage.upload_bytes(b"target", f"{base}/target.txt")
            await storage.symlink("target.txt", f"{base}/link.txt")

            assert await storage.readlink(f"{base}/link.txt") == "target.txt"
        finally:
            await _cleanup_tree(storage, base)

    async def test_readlink_non_link_reports_einval(self, storage: AbstractStorage) -> None:
        _require_capabilities(storage, readlink=True)
        base = f"test-readlink-non-link-{uid()}"
        try:
            await storage.mkdir(f"{base}/directory", parents=True)
            await storage.upload_bytes(b"file", f"{base}/file.txt")

            for path in (f"{base}/file.txt", f"{base}/directory"):
                with pytest.raises(OSError, match=r".*") as exc_info:
                    await storage.readlink(path)
                assert exc_info.value.errno == errno.EINVAL
        finally:
            await _cleanup_tree(storage, base)

    async def test_absolute_create_rejected(self, storage: AbstractStorage) -> None:
        _require_capabilities(storage, create=True)
        base = f"test-symlink-absolute-{uid()}"
        try:
            await storage.mkdir(base)
            with pytest.raises(ValueError, match=r".*"):
                await storage.symlink("/absolute-target", f"{base}/link")
        finally:
            await _cleanup_tree(storage, base)

    async def test_lstat_and_stat_keep_query_identity(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-identity-{uid()}"
        link = f"{base}/link.txt"
        try:
            await storage.mkdir(base)
            await storage.upload_bytes(b"payload", f"{base}/target.txt")
            await storage.symlink("target.txt", link)

            lexical = await storage.lstat(link)
            followed = await storage.stat(link)

            assert lexical.path == followed.path == f"/{link}"
            assert lexical.name == followed.name == "link.txt"
            assert lexical.kind is EntryKind.SYMLINK
            assert (lexical.is_file, lexical.is_dir, lexical.is_symlink) == (False, False, True)
            assert followed.kind is EntryKind.FILE
            assert (followed.is_file, followed.is_dir, followed.is_symlink) == (True, False, False)
            assert followed.size == len(b"payload")
        finally:
            await _cleanup_tree(storage, base)

    async def test_dangling_link_has_lexical_identity(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, readlink=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-dangling-{uid()}"
        link = f"{base}/dangling"
        try:
            await storage.mkdir(base)
            await storage.symlink("missing-target", link)

            lexical = await storage.lstat(link)
            assert lexical.kind is EntryKind.SYMLINK
            assert lexical.path == f"/{link}"
            assert lexical.name == "dangling"
            assert await storage.readlink(link) == "missing-target"
            assert await storage.is_symlink(link)
            assert not await storage.exists(link)
            assert not await storage.is_file(link)
            assert not await storage.is_dir(link)
            with pytest.raises(FileNotFoundError):
                await storage.stat(link)
        finally:
            await _cleanup_tree(storage, base)

    async def test_chain_follows_and_cycle_reports_eloop(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-chain-{uid()}"
        try:
            await storage.mkdir(base)
            await storage.upload_bytes(b"chain", f"{base}/target.txt")
            await storage.symlink("target.txt", f"{base}/first")
            await storage.symlink("first", f"{base}/second")

            followed = await storage.stat(f"{base}/second")
            assert followed.kind is EntryKind.FILE
            assert followed.path == f"/{base}/second"
            assert followed.name == "second"
            assert await storage.download_bytes(f"{base}/second") == b"chain"

            await storage.symlink("cycle-b", f"{base}/cycle-a")
            await storage.symlink("cycle-a", f"{base}/cycle-b")
            with pytest.raises(OSError, match=r".*") as exc_info:
                await storage.stat(f"{base}/cycle-a")
            assert exc_info.value.errno == errno.ELOOP
        finally:
            await _cleanup_tree(storage, base)

    async def test_root_escape_reports_eacces(self, storage: AbstractStorage, request: pytest.FixtureRequest) -> None:
        _require_capabilities(storage, metadata=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-escape-{uid()}"
        link = f"{base}/escape"
        try:
            await storage.mkdir(base)
            await storage.symlink("../../outside-root", link)

            with pytest.raises(PermissionError) as exc_info:
                await storage.stat(link)
            assert exc_info.value.errno == errno.EACCES
            with pytest.raises(PermissionError) as exists_exc:
                await storage.exists(link)
            assert exists_exc.value.errno == errno.EACCES
        finally:
            await _cleanup_tree(storage, base)

    @pytest.mark.parametrize("operation", _INTERMEDIATE_OPERATIONS)
    async def test_intermediate_link_is_rejected_without_mutation(
        self,
        storage: AbstractStorage,
        request: pytest.FixtureRequest,
        operation: str,
    ) -> None:
        _require_capabilities(storage, metadata=True, readlink=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-intermediate-{uid()}"
        try:
            await _prepare_intermediate_fixture(storage, base)

            await _assert_intermediate_rejected(storage, base, operation)

            await _assert_intermediate_fixture_unchanged(storage, base)
        finally:
            await _cleanup_tree(storage, base)

    async def test_target_is_directory_allows_dangling_directory_link(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, readlink=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-dir-hint-{uid()}"
        link = f"{base}/directory-link"
        try:
            await storage.mkdir(base)
            await storage.symlink("missing-directory", link, target_is_directory=True)

            lexical = await storage.lstat(link)
            assert lexical.kind is EntryKind.SYMLINK
            assert await storage.readlink(link) == "missing-directory"
            assert not await storage.is_dir(link)
            with pytest.raises(FileNotFoundError):
                await storage.stat(link)
        finally:
            await _cleanup_tree(storage, base)


class TestSymlinkDiscovery:
    async def test_iterdir_and_list_return_file_directory_and_symlink(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-discovery-{uid()}"
        try:
            await storage.mkdir(f"{base}/directory", parents=True)
            await storage.upload_bytes(b"file", f"{base}/file.txt")
            await storage.symlink("file.txt", f"{base}/link.txt")

            listings = (
                [entry async for entry in storage.iterdir(base)],
                await storage.list_(base),
            )
            for entries in listings:
                entries_by_name = {entry.name: entry for entry in entries}
                assert set(entries_by_name) == {"directory", "file.txt", "link.txt"}
                _assert_entry_kind(entries_by_name["directory"], EntryKind.DIRECTORY)
                _assert_entry_kind(entries_by_name["file.txt"], EntryKind.FILE)
                _assert_entry_kind(entries_by_name["link.txt"], EntryKind.SYMLINK)
                assert entries_by_name["directory"].path == f"/{base}/directory"
                assert entries_by_name["file.txt"].path == f"/{base}/file.txt"
                assert entries_by_name["link.txt"].path == f"/{base}/link.txt"
        finally:
            await _cleanup_tree(storage, base)


class TestSymlinkDeletion:
    async def test_unlink_delete_rmdir_and_rmtree_do_not_follow(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-delete-{uid()}"
        target_dir = f"{base}/target"
        link = f"{base}/link"
        try:
            await storage.mkdir(target_dir, parents=True)
            await storage.upload_bytes(b"keep", f"{target_dir}/child.txt")

            await storage.symlink("target", link, target_is_directory=True)
            await storage.unlink(link)
            assert not await storage.is_symlink(link)
            assert await storage.download_bytes(f"{target_dir}/child.txt") == b"keep"

            await storage.symlink("target", link, target_is_directory=True)
            await storage.delete(link)
            assert not await storage.is_symlink(link)
            assert await storage.download_bytes(f"{target_dir}/child.txt") == b"keep"

            await storage.symlink("target", link, target_is_directory=True)
            with pytest.raises(NotADirectoryError):
                await storage.rmdir(link)
            with pytest.raises(NotADirectoryError):
                await storage.rmtree(link)
            assert await storage.is_symlink(link)
            assert await storage.download_bytes(f"{target_dir}/child.txt") == b"keep"

            container = f"{base}/container"
            await storage.mkdir(container)
            await storage.symlink("../target", f"{container}/leaf", target_is_directory=True)
            with pytest.raises(OSError, match=r".*"):
                await storage.rmdir(container)
            await storage.rmtree(container)
            assert not await storage.exists(container)
            await _assert_lexically_missing(storage, container)
            await _assert_lexically_missing(storage, f"{container}/leaf")
            assert await storage.download_bytes(f"{target_dir}/child.txt") == b"keep"
        finally:
            await _cleanup_tree(storage, base)


class TestSymlinkCopyAndMove:
    async def test_copy_and_move_preserve_raw_target(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, readlink=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-copy-move-{uid()}"
        source = f"{base}/source"
        copied = f"{base}/copied"
        moved = f"{base}/moved"
        raw_target = "missing/../raw-target"
        try:
            await storage.mkdir(base)
            await storage.symlink(raw_target, source)

            await storage.copy(source, copied)
            assert await storage.is_symlink(source)
            assert await storage.is_symlink(copied)
            assert await storage.readlink(source) == raw_target
            assert await storage.readlink(copied) == raw_target

            await storage.move(copied, moved)
            assert not await storage.is_symlink(copied)
            assert await storage.is_symlink(moved)
            assert await storage.readlink(moved) == raw_target
        finally:
            await _cleanup_tree(storage, base)

    async def test_copytree_and_movetree_preserve_links(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, readlink=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-tree-preserve-{uid()}"
        source = f"{base}/source"
        copied = f"{base}/copied"
        moved = f"{base}/moved"
        try:
            await storage.mkdir(f"{source}/sub", parents=True)
            await storage.upload_bytes(b"target", f"{source}/target.txt")
            await storage.symlink("../target.txt", f"{source}/sub/link.txt")
            await storage.symlink("missing-target", f"{source}/dangling")

            await storage.copytree(source, copied)
            assert await storage.readlink(f"{copied}/sub/link.txt") == "../target.txt"
            assert await storage.readlink(f"{copied}/dangling") == "missing-target"
            assert await storage.download_bytes(f"{copied}/sub/link.txt") == b"target"

            await storage.movetree(source, moved)
            assert not await storage.exists(source)
            assert await storage.readlink(f"{moved}/sub/link.txt") == "../target.txt"
            assert await storage.readlink(f"{moved}/dangling") == "missing-target"
            assert await storage.download_bytes(f"{moved}/sub/link.txt") == b"target"
        finally:
            await _cleanup_tree(storage, base)

    async def test_symlink_root_is_rejected_by_traversal_and_tree_operations(
        self,
        storage: AbstractStorage,
        request: pytest.FixtureRequest,
    ) -> None:
        _require_capabilities(storage, metadata=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-root-{uid()}"
        link = f"{base}/link"
        try:
            await storage.mkdir(f"{base}/target", parents=True)
            await storage.upload_bytes(b"keep", f"{base}/target/child.txt")
            await storage.symlink("target", link, target_is_directory=True)

            with pytest.raises(NotADirectoryError):
                _ = [entry async for entry in storage.iterdir(link)]
            with pytest.raises(NotADirectoryError):
                await _collect_walk(storage, link)
            with pytest.raises(NotADirectoryError):
                await storage.copytree(link, f"{base}/copy-dst")
            with pytest.raises(NotADirectoryError):
                await storage.movetree(link, f"{base}/move-dst")
            with pytest.raises(NotADirectoryError):
                await storage.rmtree(link)

            assert await storage.is_symlink(link)
            assert await storage.download_bytes(f"{base}/target/child.txt") == b"keep"
        finally:
            await _cleanup_tree(storage, base)

    async def test_walk_returns_symlink_as_sorted_leaf(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-walk-{uid()}"
        try:
            await storage.mkdir(f"{base}/directory", parents=True)
            await storage.upload_bytes(b"child", f"{base}/directory/child.txt")
            await storage.symlink("directory", f"{base}/directory-link", target_is_directory=True)

            snapshots = await _collect_walk(storage, base)
            assert all(isinstance(snapshot, WalkEntry) for snapshot in snapshots)
            root = next(snapshot for snapshot in snapshots if snapshot.path == f"/{base}")
            assert tuple(entry.path for entry in root.entries) == tuple(sorted(entry.path for entry in root.entries))

            entries_by_name = {entry.name: entry for entry in root.entries}
            directory = entries_by_name["directory"]
            link = entries_by_name["directory-link"]
            assert directory.kind is EntryKind.DIRECTORY
            assert (directory.is_file, directory.is_dir, directory.is_symlink) == (False, True, False)
            assert link.kind is EntryKind.SYMLINK
            assert (link.is_file, link.is_dir, link.is_symlink) == (False, False, True)
            assert f"/{base}/directory" in {snapshot.path for snapshot in snapshots}
            assert f"/{base}/directory-link" not in {snapshot.path for snapshot in snapshots}
        finally:
            await _cleanup_tree(storage, base)


class TestSymlinkReadAndWrite:
    async def test_download_follows_and_upload_rejects_final_link(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, readlink=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-io-{uid()}"
        target = f"{base}/target.txt"
        link = f"{base}/link.txt"
        try:
            await storage.mkdir(base)
            await storage.upload_bytes(b"original", target)
            await storage.symlink("target.txt", link)

            assert await storage.download_bytes(link) == b"original"
            for overwrite in (False, True):
                with pytest.raises(OSError, match=r".*"):
                    await storage.upload_bytes(b"replacement", link, overwrite=overwrite)
                assert await storage.download_bytes(target) == b"original"
                assert await storage.readlink(link) == "target.txt"
        finally:
            await _cleanup_tree(storage, base)


class TestSymlinkOverwrite:
    async def test_create_overwrite_file_link_and_directory(
        self, storage: AbstractStorage, request: pytest.FixtureRequest
    ) -> None:
        _require_capabilities(storage, metadata=True, readlink=True, create=True)
        _require_host_symlink_create(storage, request)
        base = f"test-symlink-overwrite-{uid()}"
        file_path = f"{base}/file-slot"
        link_path = f"{base}/link-slot"
        directory_path = f"{base}/directory-slot"
        try:
            await storage.mkdir(base)
            await storage.upload_bytes(b"target", f"{base}/target.txt")
            await storage.upload_bytes(b"old", f"{base}/old-target.txt")

            await storage.upload_bytes(b"file", file_path)
            with pytest.raises(FileExistsError):
                await storage.symlink("target.txt", file_path, overwrite=False)
            assert await storage.download_bytes(file_path) == b"file"
            await storage.symlink("target.txt", file_path, overwrite=True)
            assert await storage.readlink(file_path) == "target.txt"

            await storage.symlink("old-target.txt", link_path)
            with pytest.raises(FileExistsError):
                await storage.symlink("target.txt", link_path, overwrite=False)
            assert await storage.readlink(link_path) == "old-target.txt"
            await storage.symlink("target.txt", link_path, overwrite=True)
            assert await storage.readlink(link_path) == "target.txt"

            await storage.mkdir(directory_path)
            with pytest.raises(FileExistsError):
                await storage.symlink("target.txt", directory_path, overwrite=False)
            with pytest.raises(OSError, match=r".*"):
                await storage.symlink("target.txt", directory_path, overwrite=True)
            info = await storage.lstat(directory_path)
            assert info.kind is EntryKind.DIRECTORY
            assert (info.is_file, info.is_dir, info.is_symlink) == (False, True, False)
        finally:
            await _cleanup_tree(storage, base)


class TestNoSymlinkCapabilities:
    async def test_ordinary_lstat_and_unsupported_primitives(self, storage: AbstractStorage) -> None:
        capabilities = storage.capabilities
        if capabilities.symlink_metadata or capabilities.readlink or capabilities.symlink_create:
            pytest.skip("storage exposes at least one symlink capability")

        base = f"test-no-symlink-capabilities-{uid()}"
        file_path = f"{base}/file.txt"
        directory_path = f"{base}/directory"
        link_path = f"{base}/link"
        try:
            await storage.mkdir(base)
            await storage.mkdir(directory_path)
            await storage.upload_bytes(b"file", file_path)

            file_stat = await storage.stat(file_path)
            directory_stat = await storage.stat(directory_path)
            assert await storage.lstat(file_path) == file_stat
            assert await storage.lstat(directory_path) == directory_stat
            assert file_stat.kind is EntryKind.FILE
            assert (file_stat.is_file, file_stat.is_dir, file_stat.is_symlink) == (True, False, False)
            assert directory_stat.kind is EntryKind.DIRECTORY
            assert (directory_stat.is_file, directory_stat.is_dir, directory_stat.is_symlink) == (False, True, False)
            assert not await storage.is_symlink(file_path)
            assert not await storage.is_symlink(directory_path)
            assert not await storage.is_symlink(f"{base}/missing")

            with pytest.raises(UnsupportedOperationError) as readlink_exc:
                await storage.readlink(file_path)
            assert readlink_exc.value.errno in _UNSUPPORTED_ERRNOS
            with pytest.raises(UnsupportedOperationError) as symlink_exc:
                await storage.symlink("file.txt", link_path)
            assert symlink_exc.value.errno in _UNSUPPORTED_ERRNOS
            assert not await storage.is_symlink(link_path)
        finally:
            await _cleanup_tree(storage, base)
