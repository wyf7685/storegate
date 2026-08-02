from __future__ import annotations

import errno
from pathlib import PurePosixPath

import pytest
from pydantic import SecretStr

from storegate.storage import EntryKind
from storegate.storage.sftp import SFTPConfig, SFTPStorage
from storegate.utils import flatten_exception_group
from tests.storage.sftp.fake_client import FakePool, FakeSFTPClient, install_fake_pool


def fake_storage() -> tuple[SFTPStorage, FakeSFTPClient, FakePool]:
    storage = SFTPStorage(
        SFTPConfig(
            host="sftp.invalid",
            username="tester",
            password=SecretStr("unused"),
            disable_host_key_check=True,
            root_prefix="/storage",
            chunk_size=3,
        )
    )
    client = FakeSFTPClient()
    pool = install_fake_pool(storage, client)
    return storage, client, pool


def assert_no_temporaries(client: FakeSFTPClient) -> None:
    assert client.temporary_paths() == []


@pytest.mark.anyio
async def test_deterministic_metadata_listing_follow_stream_and_lexical_deletion() -> None:
    storage, client, pool = fake_storage()
    client.add_dir("/storage/root")
    client.add_dir("/storage/root/target-dir")
    client.add_file("/storage/root/target.bin", b"payload")
    await storage.symlink("target.bin", "/root/file-link")
    await storage.symlink("target-dir", "/root/dir-link")
    await storage.symlink("missing.bin", "/root/dangling")

    lexical = await storage.lstat("/root/file-link")
    followed = await storage.stat("/root/file-link")
    assert lexical.kind is EntryKind.SYMLINK
    assert followed.kind is EntryKind.FILE
    assert followed.path == "/root/file-link"
    assert followed.name == "file-link"
    assert await storage.readlink("/root/file-link") == "target.bin"
    assert await storage.is_file("/root/file-link")
    assert await storage.is_dir("/root/dir-link")
    assert not await storage.exists("/root/dangling")
    assert await storage.is_symlink("/root/dangling")

    listed = [entry async for entry in storage.iterdir("/root")]
    assert {entry.name: entry.kind for entry in listed} == {
        "dangling": EntryKind.SYMLINK,
        "dir-link": EntryKind.SYMLINK,
        "file-link": EntryKind.SYMLINK,
        "target-dir": EntryKind.DIRECTORY,
        "target.bin": EntryKind.FILE,
    }
    walked = [entry async for entry in storage.walk("/root")]
    assert [entry.path for entry in walked] == ["/root", "/root/target-dir"]
    assert [(entry.name, entry.path, entry.kind) for entry in walked[0].entries] == [
        ("dangling", "/root/dangling", EntryKind.SYMLINK),
        ("dir-link", "/root/dir-link", EntryKind.SYMLINK),
        ("file-link", "/root/file-link", EntryKind.SYMLINK),
        ("target-dir", "/root/target-dir", EntryKind.DIRECTORY),
        ("target.bin", "/root/target.bin", EntryKind.FILE),
    ]
    assert walked[1].path == "/root/target-dir"
    assert walked[1].entries == ()

    stream = storage.download_stream("/root/file-link")
    assert pool.active_leases == 0
    assert await anext(stream) == b"pay"
    assert pool.active_leases == 1
    await stream.aclose()
    assert pool.active_leases == 0
    assert await storage.download_bytes("/root/file-link") == b"payload"
    with pytest.raises(OSError, match="symbolic link") as upload_error:
        await storage.upload_bytes(b"blocked", "/root/file-link")
    assert upload_error.value.errno == errno.EACCES
    assert await storage.download_bytes("/root/target.bin") == b"payload"

    client.add_dir("/storage/raw-empty")
    await storage.symlink("missing", "/raw-empty/dangling")
    with pytest.raises(OSError, match="Directory not empty"):
        await storage.rmdir("/raw-empty")
    await storage.rmtree("/raw-empty")

    client.add_file("/storage/safe-target", b"safe")
    client.add_dir("/storage/tree")
    await storage.symlink("../safe-target", "/tree/link")
    await storage.rmtree("/tree")
    assert await storage.download_bytes("/safe-target") == b"safe"

    await storage.unlink("/root/file-link")
    assert await storage.download_bytes("/root/target.bin") == b"payload"
    assert_no_temporaries(client)


@pytest.mark.anyio
async def test_symlink_overwrite_false_rejects_directory_with_file_exists() -> None:
    storage, client, _ = fake_storage()
    client.add_dir("/storage/root")
    client.add_dir("/storage/root/directory-slot")
    with pytest.raises(FileExistsError):
        await storage.symlink("target.txt", "/root/directory-slot", overwrite=False)
    with pytest.raises(IsADirectoryError):
        await storage.symlink("target.txt", "/root/directory-slot", overwrite=True)
    info = await storage.lstat("/root/directory-slot")
    assert info.kind is EntryKind.DIRECTORY
    assert_no_temporaries(client)


@pytest.mark.anyio
async def test_symlink_and_mkdir_reject_intermediate_symlink_parent() -> None:
    storage, client, _ = fake_storage()
    client.add_dir("/storage/root")
    client.add_dir("/storage/root/real")
    client.add_link("/storage/root/alias", "real")

    with pytest.raises(PermissionError) as symlink_parent:
        await storage.symlink("data.txt", "/root/alias/new-link")
    assert symlink_parent.value.errno == errno.EACCES
    assert "Intermediate symlink" in str(symlink_parent.value)

    with pytest.raises(PermissionError) as mkdir_parent:
        await storage.mkdir("/root/alias/new-dir")
    assert mkdir_parent.value.errno == errno.EACCES
    assert "Intermediate symlink" in str(mkdir_parent.value)

    with pytest.raises(PermissionError) as parents_mkdir:
        await storage.mkdir("/root/alias/nested/new-dir", parents=True)
    assert parents_mkdir.value.errno == errno.EACCES
    assert "Intermediate symlink" in str(parents_mkdir.value)

    assert await storage.lstat("/root/alias")
    assert (await storage.lstat("/root/alias")).kind is EntryKind.SYMLINK
    assert PurePosixPath("/storage/root/alias/new-link") not in client.nodes
    assert PurePosixPath("/storage/root/alias/new-dir") not in client.nodes
    assert_no_temporaries(client)


@pytest.mark.anyio
async def test_deterministic_copy_and_move_preserve_raw_dangling_target() -> None:
    storage, client, _ = fake_storage()
    client.add_dir("/storage/ops")
    await storage.symlink("../missing-target", "/ops/source")
    await storage.copy("/ops/source", "/ops/copied")
    assert await storage.readlink("/ops/copied") == "../missing-target"
    await storage.move("/ops/copied", "/ops/moved")
    assert not await storage.is_symlink("/ops/copied")
    assert await storage.readlink("/ops/moved") == "../missing-target"

    await storage.symlink("old-target", "/ops/existing")
    await storage.copy("/ops/source", "/ops/existing")
    assert await storage.readlink("/ops/existing") == "../missing-target"
    await storage.move("/ops/source", "/ops/existing")
    assert not await storage.is_symlink("/ops/source")
    assert await storage.readlink("/ops/existing") == "../missing-target"
    assert_no_temporaries(client)


@pytest.mark.anyio
async def test_deterministic_copytree_and_movetree_preserve_links_and_clean_backups() -> None:
    storage, client, _ = fake_storage()
    client.add_dir("/storage/source")
    client.add_dir("/storage/source/nested")
    client.add_file("/storage/source/nested/file.bin", b"data")
    client.add_link("/storage/source/nested/dangling", "../../missing")
    client.add_link("/storage/source/dir-link", "nested")

    client.add_dir("/storage/copied")
    client.add_dir("/storage/copied/nested")
    client.add_link("/storage/copied/nested/dangling", "old-target")
    client.add_file("/storage/copied/unrelated.bin", b"keep")
    await storage.copytree("/source", "/copied")
    assert await storage.readlink("/copied/nested/dangling") == "../../missing"
    assert await storage.readlink("/copied/dir-link") == "nested"
    assert await storage.download_bytes("/copied/nested/file.bin") == b"data"
    assert await storage.download_bytes("/copied/unrelated.bin") == b"keep"
    assert_no_temporaries(client)

    client.add_dir("/storage/moved")
    client.add_file("/storage/moved/unrelated.bin", b"keep")
    await storage.movetree("/source", "/moved")
    assert not await storage.exists("/source")
    assert await storage.readlink("/moved/nested/dangling") == "../../missing"
    assert await storage.readlink("/moved/dir-link") == "nested"
    assert await storage.download_bytes("/moved/nested/file.bin") == b"data"
    assert await storage.download_bytes("/moved/unrelated.bin") == b"keep"
    assert_no_temporaries(client)


@pytest.mark.anyio
async def test_dangling_link_backup_rollback_restores_destination_without_leaks() -> None:
    storage, client, _ = fake_storage()
    client.add_dir("/storage/source")
    client.add_link("/storage/source/link", "new-missing")
    client.add_dir("/storage/destination")
    client.add_link("/storage/destination/link", "old-missing")

    destination = "/storage/destination/link"
    client.fail(
        "rename",
        lambda args: ".storegate-copytree-" in PurePosixPath(str(args[0])).name and args[1] == destination,
        OSError("primary mutation failure"),
    )
    with pytest.raises(OSError, match="primary mutation failure"):
        await storage.copytree("/source", "/destination")

    assert await storage.readlink("/destination/link") == "old-missing"
    assert await storage.readlink("/source/link") == "new-missing"
    assert_no_temporaries(client)


@pytest.mark.anyio
async def test_tree_failure_group_keeps_primary_first_and_recovers_without_leaks() -> None:
    storage, client, _ = fake_storage()
    client.add_dir("/storage/source")
    client.add_link("/storage/source/link", "new-missing")
    client.add_dir("/storage/destination")
    client.add_link("/storage/destination/link", "old-missing")

    destination = "/storage/destination/link"
    client.fail(
        "rename",
        lambda args: ".storegate-copytree-" in PurePosixPath(str(args[0])).name and args[1] == destination,
        OSError("primary mutation failure"),
    )
    client.fail(
        "rename",
        lambda args: ".storegate-copytree-backup-" in PurePosixPath(str(args[0])).name and args[1] == destination,
        OSError("rollback acknowledgement failure"),
        after=True,
    )

    with pytest.raises(BaseExceptionGroup) as captured:
        await storage.copytree("/source", "/destination")
    assert isinstance(captured.value.exceptions[0], OSError)
    assert str(captured.value.exceptions[0]) == "primary mutation failure"
    assert await storage.readlink("/destination/link") == "old-missing"
    assert await storage.readlink("/source/link") == "new-missing"
    assert_no_temporaries(client)


@pytest.mark.anyio
async def test_overwrite_upload_commits_without_posix_rename() -> None:
    """Servers lacking posix-rename@openssh.com take the staged backup path.

    That whole fallback -- rename target aside, rename temp into place, drop the
    backup -- was never executed by the suite because the fake always implemented
    the extension.
    """
    storage, client, _ = fake_storage()
    client.supports_posix_rename = False
    client.add_dir("/storage/root")
    client.add_file("/storage/root/file.bin", b"old-content")

    await storage.upload_bytes(b"new-content", "/root/file.bin")

    assert await storage.download_bytes("/root/file.bin") == b"new-content"
    assert_no_temporaries(client)


@pytest.mark.anyio
async def test_staged_commit_rollback_restores_target_without_posix_rename() -> None:
    """If the temp->target rename fails, the backup must come back."""
    storage, client, _ = fake_storage()
    client.supports_posix_rename = False
    client.add_dir("/storage/root")
    client.add_file("/storage/root/file.bin", b"old-content")

    target = "/storage/root/file.bin"
    client.fail(
        "rename",
        lambda args: (
            ".storegate-" in PurePosixPath(str(args[0])).name
            and ".storegate-backup-" not in PurePosixPath(str(args[0])).name
            and args[1] == target
        ),
        OSError("staged commit failure"),
    )

    with pytest.raises(OSError, match="staged commit failure"):
        await storage.upload_bytes(b"new-content", "/root/file.bin")

    # The original content is intact and nothing is stranded under a temp name.
    assert await storage.download_bytes("/root/file.bin") == b"old-content"
    assert_no_temporaries(client)


@pytest.mark.anyio
async def test_staged_commit_rollback_failure_is_grouped_primary_first() -> None:
    """When restoring the backup also fails, both errors survive, primary first."""
    storage, client, _ = fake_storage()
    client.supports_posix_rename = False
    client.add_dir("/storage/root")
    client.add_file("/storage/root/file.bin", b"old-content")

    target = "/storage/root/file.bin"
    client.fail(
        "rename",
        lambda args: (
            ".storegate-" in PurePosixPath(str(args[0])).name
            and ".storegate-backup-" not in PurePosixPath(str(args[0])).name
            and args[1] == target
        ),
        OSError("staged commit failure"),
    )
    client.fail(
        "rename",
        lambda args: ".storegate-backup-" in PurePosixPath(str(args[0])).name and args[1] == target,
        OSError("backup restore failure"),
    )

    with pytest.raises(BaseExceptionGroup) as captured:
        await storage.upload_bytes(b"new-content", "/root/file.bin")

    flattened = list(flatten_exception_group(captured.value))
    assert str(flattened[0]) == "staged commit failure"
    assert any("backup restore failure" in str(exc) for exc in flattened[1:])


@pytest.mark.anyio
async def test_move_overwrite_uses_backup_path_without_posix_rename() -> None:
    storage, client, _ = fake_storage()
    client.supports_posix_rename = False
    client.add_dir("/storage/root")
    client.add_file("/storage/root/source.bin", b"source-content")
    client.add_file("/storage/root/target.bin", b"target-content")

    await storage.move("/root/source.bin", "/root/target.bin", overwrite=True)

    assert await storage.download_bytes("/root/target.bin") == b"source-content"
    assert not await storage.exists("/root/source.bin")
    assert_no_temporaries(client)
