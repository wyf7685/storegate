import errno
import os

import pytest

from storegate.storage import EntryKind, UnsupportedOperationError
from storegate.storage.sftp import SFTPStorage
from tests.fixtures.protocol_servers import SFTPServerInfo
from tests.storage.sftp.test_lifecycle import make_config
from tests.support.ids import uid


def require_host_symlinks(server: SFTPServerInfo) -> None:
    if not server.symlink_supported:
        pytest.skip("host filesystem denied symlink creation for the SFTP fixture")


@pytest.mark.integration
async def test_symlink_metadata_follow_listing_walk_and_stream_safety(sftp_server: SFTPServerInfo) -> None:
    require_host_symlinks(sftp_server)
    root = f"/links-{uid()}"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        assert storage.capabilities.symlink_metadata
        assert storage.capabilities.readlink
        assert storage.capabilities.symlink_create

        await storage.upload_bytes(b"payload", f"{root}/target.bin")
        await storage.mkdir(f"{root}/target-dir", parents=True)
        await storage.symlink("target.bin", f"{root}/file-link")
        await storage.symlink("target-dir", f"{root}/dir-link")
        await storage.symlink("missing.bin", f"{root}/dangling")

        lexical = await storage.lstat(f"{root}/file-link")
        followed = await storage.stat(f"{root}/file-link")
        assert lexical.kind is EntryKind.SYMLINK
        assert followed.kind is EntryKind.FILE
        assert followed.path == f"{root}/file-link"
        assert followed.name == "file-link"
        assert await storage.readlink(f"{root}/file-link") == "target.bin"
        assert await storage.is_file(f"{root}/file-link")
        assert await storage.is_dir(f"{root}/dir-link")
        assert not await storage.exists(f"{root}/dangling")
        assert await storage.is_symlink(f"{root}/dangling")
        assert await storage.download_bytes(f"{root}/file-link") == b"payload"
        with pytest.raises(OSError, match="symbolic link"):
            await storage.upload_bytes(b"blocked", f"{root}/file-link")
        assert await storage.download_bytes(f"{root}/target.bin") == b"payload"

        listed = [entry async for entry in storage.iterdir(root)]
        assert {entry.name: entry.kind for entry in listed} == {
            "dangling": EntryKind.SYMLINK,
            "dir-link": EntryKind.SYMLINK,
            "file-link": EntryKind.SYMLINK,
            "target-dir": EntryKind.DIRECTORY,
            "target.bin": EntryKind.FILE,
        }
        walked = [entry async for entry in storage.walk(root)]
        assert [entry.path for entry in walked] == [root, f"{root}/target-dir"]

        await storage.unlink(f"{root}/file-link")
        assert await storage.download_bytes(f"{root}/target.bin") == b"payload"
        await storage.rmtree(root)


@pytest.mark.integration
async def test_link_chain_cycle_escape_and_intermediate_rejection(sftp_server: SFTPServerInfo) -> None:
    require_host_symlinks(sftp_server)
    root = f"/resolver-{uid()}"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.upload_bytes(b"inside", f"{root}/target.bin")
        await storage.symlink("target.bin", f"{root}/first")
        await storage.symlink("first", f"{root}/second")
        await storage.symlink("self", f"{root}/self")
        await storage.symlink("../../outside/outside.bin", f"{root}/escape")
        sftp_server.outside_root.joinpath("outside.bin").write_bytes(b"outside")

        assert await storage.download_bytes(f"{root}/second") == b"inside"
        with pytest.raises(OSError, match="Too many symbolic link levels") as cycle:
            await storage.stat(f"{root}/self")
        assert cycle.value.errno == errno.ELOOP
        with pytest.raises(PermissionError) as escape:
            await storage.stat(f"{root}/escape")
        assert escape.value.errno == errno.EACCES
        with pytest.raises(PermissionError):
            await storage.stat(f"{root}/first/child")
        with pytest.raises(ValueError, match="must be relative"):
            await storage.symlink("/absolute", f"{root}/absolute")
        await storage.rmtree(root)


@pytest.mark.integration
async def test_copy_move_and_tree_operations_preserve_raw_targets(sftp_server: SFTPServerInfo) -> None:
    require_host_symlinks(sftp_server)
    token = uid()
    root = f"/preserve-{token}"
    source = f"{root}/source"
    copied = f"{root}/copied"
    moved = f"{root}/moved"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.mkdir(source, parents=True)
        await storage.symlink("../missing-target", f"{source}/dangling")
        await storage.copy(f"{source}/dangling", f"{root}/copy-link")
        assert await storage.readlink(f"{root}/copy-link") == "../missing-target"
        await storage.move(f"{root}/copy-link", f"{root}/moved-link")
        assert await storage.readlink(f"{root}/moved-link") == "../missing-target"

        await storage.copytree(source, copied)
        assert await storage.readlink(f"{copied}/dangling") == "../missing-target"
        await storage.movetree(copied, moved)
        assert await storage.readlink(f"{moved}/dangling") == "../missing-target"
        with pytest.raises(NotADirectoryError):
            await storage.rmtree(f"{root}/moved-link")

        names = [
            entry.name
            async for walk_entry in storage.walk(root)
            for entry in walk_entry.entries
            if ".storegate-" in entry.name
        ]
        assert names == []
        await storage.rmtree(root)


@pytest.mark.integration
async def test_rmdir_counts_dangling_link_and_rmtree_unlinks_it(sftp_server: SFTPServerInfo) -> None:
    require_host_symlinks(sftp_server)
    root = f"/lexical-{uid()}"
    target = f"/target-{uid()}.bin"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        await storage.upload_bytes(b"safe", target)
        await storage.mkdir(root)
        await storage.symlink(target.lstrip("/"), f"{root}/link")
        with pytest.raises(OSError, match="Directory not empty"):
            await storage.rmdir(root)
        await storage.rmtree(root)
        assert await storage.download_bytes(target) == b"safe"
        await storage.unlink(target)


@pytest.mark.integration
async def test_strict_tree_preflight_rejects_special_before_mutation(sftp_server: SFTPServerInfo) -> None:
    if not hasattr(os, "mkfifo"):
        pytest.skip("host platform does not provide FIFO creation")
    root = f"special-{uid()}"
    source_host = sftp_server.storage_root / root
    source_host.mkdir()
    os.mkfifo(source_host / "fifo")
    destination = f"/{root}-copy"
    async with SFTPStorage(make_config(sftp_server)) as storage:
        with pytest.raises(UnsupportedOperationError):
            await storage.copytree(f"/{root}", destination)
        assert not await storage.exists(destination)
        with pytest.raises(UnsupportedOperationError):
            await storage.rmtree(f"/{root}")
        assert source_host.exists()
