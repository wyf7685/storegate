"""FTPStorage behavior tests."""

from pathlib import PurePosixPath

import aioftp
import pytest

from storegate.storage.ftp import FTPStorage
from storegate.utils import flatten_exception_group
from tests.support.ids import uid

pytestmark = pytest.mark.integration


class TestCopyAndTrees:
    async def test_copy_uses_streaming_auxiliary_client(self, ftp_storage: FTPStorage) -> None:
        base = f"/ftp-copy-{uid()}"
        await ftp_storage.upload_bytes(b"0123456789", f"{base}/source.bin")
        try:
            await ftp_storage.copy(f"{base}/source.bin", f"{base}/copy.bin")
            assert await ftp_storage.download_bytes(f"{base}/copy.bin") == b"0123456789"
            with pytest.raises(FileExistsError):
                await ftp_storage.copy(f"{base}/source.bin", f"{base}/source.bin", overwrite=False)
        finally:
            await ftp_storage.rmtree(base)

    async def test_copy_failure_removes_new_partial_destination(
        self,
        ftp_storage: FTPStorage,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        base = f"/ftp-copy-cleanup-{uid()}"
        source = PurePosixPath(base, "source.bin")
        destination = PurePosixPath(base, "destination.bin")
        await ftp_storage.upload_bytes(b"source", source)

        async def write_partial_then_fail(
            self: FTPStorage,
            source_client: aioftp.Client,
            destination_client: aioftp.Client,
            source_path: PurePosixPath,
            destination_path: PurePosixPath,
        ) -> None:
            _ = source_client, source_path
            async with destination_client.upload_stream(self._remote_path(destination_path)) as writer:
                await writer.write(b"partial")
            raise OSError("injected copy failure")

        monkeypatch.setattr(FTPStorage, "_copy_stream", write_partial_then_fail)
        try:
            with pytest.raises(OSError, match="injected copy failure"):
                await ftp_storage.copy(source, destination)
            assert not await ftp_storage.exists(destination)
        finally:
            await ftp_storage.rmtree(base)

    async def test_copy_failure_preserves_existing_destination(
        self,
        ftp_storage: FTPStorage,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        base = f"/ftp-copy-preserve-{uid()}"
        source = PurePosixPath(base, "source.bin")
        destination = PurePosixPath(base, "destination.bin")
        await ftp_storage.upload_bytes(b"source", source)
        await ftp_storage.upload_bytes(b"existing", destination)

        async def fail_before_write(
            _self: FTPStorage,
            _source_client: aioftp.Client,
            _destination_client: aioftp.Client,
            _source_path: PurePosixPath,
            _destination_path: PurePosixPath,
        ) -> None:
            raise OSError("injected copy failure")

        monkeypatch.setattr(FTPStorage, "_copy_stream", fail_before_write)
        try:
            with pytest.raises(OSError, match="injected copy failure"):
                await ftp_storage.copy(source, destination)
            assert await ftp_storage.download_bytes(destination) == b"existing"
        finally:
            await ftp_storage.rmtree(base)

    async def test_copytree_merges_existing_destination(self, ftp_storage: FTPStorage) -> None:
        base = f"/ftp-tree-{uid()}"
        source = f"{base}/source"
        destination = f"{base}/destination"
        await ftp_storage.upload_bytes(b"new", f"{source}/sub/new.txt")
        await ftp_storage.upload_bytes(b"keep", f"{destination}/keep.txt")
        try:
            await ftp_storage.copytree(source, destination, overwrite=True)
            assert await ftp_storage.download_bytes(f"{destination}/sub/new.txt") == b"new"
            assert await ftp_storage.download_bytes(f"{destination}/keep.txt") == b"keep"
            with pytest.raises(ValueError, match="inside"):
                await ftp_storage.copytree(source, f"{source}/child")
        finally:
            await ftp_storage.rmtree(base)

    async def test_copytree_rollback_preserves_existing_content(
        self,
        ftp_storage: FTPStorage,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        base = f"/ftp-rollback-{uid()}"
        source = PurePosixPath(base, "source")
        destination = PurePosixPath(base, "destination")
        await ftp_storage.upload_bytes(b"a", source / "a.txt")
        await ftp_storage.upload_bytes(b"b", source / "b.txt")
        await ftp_storage.upload_bytes(b"keep", destination / "keep.txt")

        original = FTPStorage._copy_stream

        async def fail_second_copy(
            self: FTPStorage,
            source_client: aioftp.Client,
            destination_client: aioftp.Client,
            source_path: PurePosixPath,
            destination_path: PurePosixPath,
        ) -> None:
            if source_path.name == "b.txt":
                raise OSError("injected copy failure")
            await original(self, source_client, destination_client, source_path, destination_path)

        monkeypatch.setattr(FTPStorage, "_copy_stream", fail_second_copy)
        try:
            with pytest.raises(OSError, match="injected copy failure"):
                await ftp_storage.copytree(source, destination, overwrite=True)
            assert await ftp_storage.download_bytes(destination / "keep.txt") == b"keep"
            assert not await ftp_storage.exists(destination / "a.txt")
            assert await ftp_storage.exists(source / "a.txt")
            assert await ftp_storage.exists(source / "b.txt")
        finally:
            monkeypatch.setattr(FTPStorage, "_copy_stream", original)
            await ftp_storage.rmtree(base)

    async def test_copytree_public_rollback_group_preserves_primary_first(
        self,
        ftp_storage: FTPStorage,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        base = f"/ftp-rollback-group-{uid()}"
        source = PurePosixPath(base, "source")
        destination = PurePosixPath(base, "destination")
        await ftp_storage.upload_bytes(b"a", source / "a.txt")
        await ftp_storage.upload_bytes(b"b", source / "b.txt")
        await ftp_storage.upload_bytes(b"old-a", destination / "a.txt")

        original_copy = FTPStorage._copy_stream

        async def fail_second_copy(
            self: FTPStorage,
            source_client: aioftp.Client,
            destination_client: aioftp.Client,
            source_path: PurePosixPath,
            destination_path: PurePosixPath,
        ) -> None:
            if source_path.name == "b.txt":
                raise OSError("injected protocol copy failure")
            await original_copy(self, source_client, destination_client, source_path, destination_path)

        async def fail_rollback(
            self: FTPStorage,
            client: aioftp.Client,
            created_files: set[PurePosixPath],
            created_dirs: set[PurePosixPath],
            backups: dict[PurePosixPath, PurePosixPath],
        ) -> None:
            _ = self, client, created_files, created_dirs, backups
            raise OSError("injected rollback failure")

        monkeypatch.setattr(FTPStorage, "_copy_stream", fail_second_copy)
        monkeypatch.setattr(FTPStorage, "_rollback_copytree_with_fallback", fail_rollback)
        try:
            with pytest.raises(BaseExceptionGroup) as caught:
                await ftp_storage.copytree(source, destination, overwrite=True)
            flattened = list(flatten_exception_group(caught.value))
            assert len(flattened) == 2
            assert "injected protocol copy failure" in str(flattened[0])
            assert "injected rollback failure" in str(flattened[1])
        finally:
            monkeypatch.setattr(FTPStorage, "_copy_stream", original_copy)
            await ftp_storage.rmtree(base)

    async def test_copytree_failure_restores_overwritten_files(
        self,
        ftp_storage: FTPStorage,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        base = f"/ftp-rollback-overwrite-{uid()}"
        source = PurePosixPath(base, "source")
        destination = PurePosixPath(base, "destination")
        await ftp_storage.upload_bytes(b"new-a", source / "a.txt")
        await ftp_storage.upload_bytes(b"new-b", source / "b.txt")
        await ftp_storage.upload_bytes(b"old-a", destination / "a.txt")
        await ftp_storage.upload_bytes(b"old-b", destination / "b.txt")
        original = FTPStorage._copy_stream

        async def fail_second_copy(
            self: FTPStorage,
            source_client: aioftp.Client,
            destination_client: aioftp.Client,
            source_path: PurePosixPath,
            destination_path: PurePosixPath,
        ) -> None:
            if source_path.name == "b.txt":
                raise OSError("injected overwrite copy failure")
            await original(self, source_client, destination_client, source_path, destination_path)

        monkeypatch.setattr(FTPStorage, "_copy_stream", fail_second_copy)
        try:
            with pytest.raises(OSError, match="injected overwrite copy failure"):
                await ftp_storage.copytree(source, destination, overwrite=True)
            assert await ftp_storage.download_bytes(destination / "a.txt") == b"old-a"
            assert await ftp_storage.download_bytes(destination / "b.txt") == b"old-b"
            assert await ftp_storage.download_bytes(source / "a.txt") == b"new-a"
            assert await ftp_storage.download_bytes(source / "b.txt") == b"new-b"
        finally:
            monkeypatch.setattr(FTPStorage, "_copy_stream", original)
            await ftp_storage.rmtree(base)

    async def test_move_rename_mutates_then_raises_restores_paths(
        self, ftp_storage: FTPStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        base = f"/ftp-move-rename-failure-{uid()}"
        source = PurePosixPath(base, "source.bin")
        destination = PurePosixPath(base, "destination.bin")
        await ftp_storage.upload_bytes(b"source", source)
        await ftp_storage.upload_bytes(b"existing", destination)
        original_rename = aioftp.Client.rename
        raised = False

        async def mutate_then_raise(client: aioftp.Client, old: str, new: str) -> None:
            nonlocal raised
            await original_rename(client, old, new)
            if not raised and old.endswith("/source.bin") and new.endswith("/destination.bin"):
                raised = True
                raise OSError("rename response lost after commit")

        monkeypatch.setattr(aioftp.Client, "rename", mutate_then_raise)
        try:
            with pytest.raises(OSError, match="rename response lost"):
                await ftp_storage.move(source, destination, overwrite=True)
            assert await ftp_storage.download_bytes(source) == b"source"
            assert await ftp_storage.download_bytes(destination) == b"existing"
            entries = [info async for info in ftp_storage.iterdir(base)]
            assert not any(info.name.startswith(".storegate-move-") for info in entries)
        finally:
            await ftp_storage.rmtree(base)

    async def test_move_backup_cleanup_failure_restores_paths(
        self, ftp_storage: FTPStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        base = f"/ftp-move-cleanup-failure-{uid()}"
        source = PurePosixPath(base, "source.bin")
        destination = PurePosixPath(base, "destination.bin")
        await ftp_storage.upload_bytes(b"source", source)
        await ftp_storage.upload_bytes(b"existing", destination)
        original_remove = aioftp.Client.remove_file
        raised = False

        async def fail_backup_cleanup(client: aioftp.Client, path: str) -> None:
            nonlocal raised
            if not raised and ".storegate-move-" in path:
                raised = True
                raise OSError("control channel lost removing backup")
            await original_remove(client, path)

        monkeypatch.setattr(aioftp.Client, "remove_file", fail_backup_cleanup)
        try:
            with pytest.raises(OSError, match="control channel lost"):
                await ftp_storage.move(source, destination, overwrite=True)
            assert await ftp_storage.download_bytes(source) == b"source"
            assert await ftp_storage.download_bytes(destination) == b"existing"
            entries = [info async for info in ftp_storage.iterdir(base)]
            assert not any(info.name.startswith(".storegate-move-") for info in entries)
        finally:
            await ftp_storage.rmtree(base)

    async def test_move_backup_cleanup_mutates_then_raises_commits(
        self, ftp_storage: FTPStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        base = f"/ftp-move-cleanup-committed-{uid()}"
        source = PurePosixPath(base, "source.bin")
        destination = PurePosixPath(base, "destination.bin")
        await ftp_storage.upload_bytes(b"source", source)
        await ftp_storage.upload_bytes(b"existing", destination)
        original_remove = aioftp.Client.remove_file
        raised = False

        async def remove_then_raise(client: aioftp.Client, path: str) -> None:
            nonlocal raised
            if not raised and ".storegate-move-" in path:
                raised = True
                await original_remove(client, path)
                raise OSError("cleanup response lost after commit")
            await original_remove(client, path)

        monkeypatch.setattr(aioftp.Client, "remove_file", remove_then_raise)
        try:
            await ftp_storage.move(source, destination, overwrite=True)
            assert not await ftp_storage.exists(source)
            assert await ftp_storage.download_bytes(destination) == b"source"
            entries = [info async for info in ftp_storage.iterdir(base)]
            assert not any(info.name.startswith(".storegate-move-") for info in entries)
        finally:
            await ftp_storage.rmtree(base)

    async def test_movetree_failure_keeps_source(
        self,
        ftp_storage: FTPStorage,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        base = f"/ftp-move-tree-{uid()}"
        source = PurePosixPath(base, "source")
        destination = PurePosixPath(base, "destination")
        await ftp_storage.upload_bytes(b"source", source / "file.txt")
        await ftp_storage.upload_bytes(b"existing", destination / "existing.txt")

        async def fail_copytree(
            self: FTPStorage,
            source_client: aioftp.Client,
            destination_client: aioftp.Client,
            source_path: PurePosixPath,
            destination_path: PurePosixPath,
            *,
            overwrite: bool,
        ) -> None:
            _ = self, source_client, destination_client, source_path, destination_path, overwrite
            raise OSError("injected tree failure")

        monkeypatch.setattr(FTPStorage, "_copytree", fail_copytree)
        try:
            with pytest.raises(OSError, match="injected tree failure"):
                await ftp_storage.movetree(source, destination, overwrite=True)
            assert await ftp_storage.download_bytes(source / "file.txt") == b"source"
            assert await ftp_storage.download_bytes(destination / "existing.txt") == b"existing"
        finally:
            await ftp_storage.rmtree(base)
