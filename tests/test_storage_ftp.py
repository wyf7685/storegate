import json
from collections.abc import AsyncIterator
from datetime import UTC
from pathlib import Path, PurePosixPath
from typing import cast

import aioftp
import anyio
import pytest
from pydantic import ValidationError

from app.storage.factory import resolve_storage_from_file
from app.storage.ftp import FTPConfig, FTPStorage
from tests.conftest import uid

pytestmark = pytest.mark.ftp


@pytest.fixture
async def ftp_storage(ftp_endpoint: tuple[str, int]) -> AsyncIterator[FTPStorage]:
    host, port = ftp_endpoint
    async with FTPStorage(FTPConfig(host=host, port=port, chunk_size=4)) as storage:
        yield storage


class TestFTPConfig:
    def test_defaults_and_normalization(self) -> None:
        config = FTPConfig(host=" FTP.EXAMPLE.TEST ", root_prefix="/tenant//files/")
        assert config.host == "ftp.example.test"
        assert config.port == 21
        assert config.username == "anonymous"
        assert config.password.get_secret_value() == "anon@"
        assert config.root_prefix == "/tenant/files"
        assert config.chunk_size == 1024 * 1024
        assert config.encoding == "utf-8"
        assert config.timeout == 30.0

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("host", ""),
            ("port", 0),
            ("port", 65536),
            ("chunk_size", 0),
            ("timeout", 0),
            ("encoding", "not-an-encoding"),
            ("root_prefix", "relative"),
            ("root_prefix", "//double-root"),
            ("root_prefix", "/a/../b"),
            ("root_prefix", "/a\x00b"),
        ],
    )
    def test_invalid_values(self, field: str, value: object) -> None:
        kwargs: dict[str, object] = {"host": "ftp.example.test", field: value}
        with pytest.raises(ValidationError):
            FTPConfig.model_validate(kwargs)

    def test_from_file(self, tmp_path: Path) -> None:
        path = tmp_path / "ftp.json"
        path.write_text(
            json.dumps(
                {
                    "host": "ftp.example.test",
                    "password": "secret",
                    "root_prefix": "/tenant",
                }
            )
        )
        config = FTPConfig.from_file(path)
        assert config.host == "ftp.example.test"
        assert config.password.get_secret_value() == "secret"
        assert config.root_prefix == "/tenant"

    def test_factory_from_file(self, tmp_path: Path) -> None:
        path = tmp_path / "storage.json"
        path.write_text(
            json.dumps(
                {
                    "$factory": "~ftp",
                    "config": {
                        "host": "ftp.example.test",
                        "password": "secret",
                        "root_prefix": "/tenant",
                    },
                }
            )
        )
        storage = resolve_storage_from_file(path)
        assert isinstance(storage, FTPStorage)
        assert storage.id == "ftp:anonymous@ftp.example.test:21/tenant"


class TestIdentityAndPaths:
    def test_identity_is_account_scoped_and_secret_free(self) -> None:
        first = FTPStorage(FTPConfig(host="ftp.example.test", username="alice", password="first", root_prefix="/files"))
        rotated = FTPStorage(
            FTPConfig(host="ftp.example.test", username="alice", password="second", root_prefix="/files")
        )
        other_user = FTPStorage(
            FTPConfig(host="ftp.example.test", username="bob", password="first", root_prefix="/files")
        )

        assert first.cache_identity == rotated.cache_identity
        assert first.cache_identity != other_user.cache_identity
        assert first.id != other_user.id
        assert "first" not in first.id
        assert "first" not in first.cache_identity

    async def test_rejects_traversal_and_nul(self, ftp_storage: FTPStorage) -> None:
        with pytest.raises(ValueError, match="segments"):
            await ftp_storage.stat("/../escape")
        with pytest.raises(ValueError, match="NUL"):
            await ftp_storage.stat("/bad\x00name")

    async def test_root_operations_are_protected(self, ftp_storage: FTPStorage) -> None:
        with pytest.raises(OSError, match="Cannot remove root"):
            await ftp_storage.rmdir("/")
        with pytest.raises(OSError, match="Cannot remove root"):
            await ftp_storage.rmtree("/")
        with pytest.raises(OSError, match="Cannot move root"):
            await ftp_storage.move("/", "/elsewhere")
        with pytest.raises(OSError, match="Cannot move root"):
            await ftp_storage.movetree("/", "/elsewhere")
        with pytest.raises(IsADirectoryError):
            await ftp_storage.unlink("/")
        with pytest.raises(IsADirectoryError):
            await ftp_storage.copy("/", "/elsewhere")

    async def test_root_prefix_isolated(self, ftp_endpoint: tuple[str, int]) -> None:
        host, port = ftp_endpoint
        prefix = f"/ftp-prefix-{uid()}"
        outside = f"/outside-{uid()}.txt"
        async with aioftp.Client.context(host, port) as client:
            await client.make_directory(prefix)
            async with client.upload_stream(outside) as writer:
                await writer.write(b"outside")

        storage = FTPStorage(FTPConfig(host=host, port=port, root_prefix=prefix))
        try:
            async with storage:
                await storage.upload_bytes(b"inside", "/inside.txt")
                names = {entry.name async for entry in storage.iterdir("/")}
                assert names == {"inside.txt"}

            async with aioftp.Client.context(host, port) as client:
                assert (await client.stat(f"{prefix}/inside.txt"))["size"] == "6"
                assert (await client.stat(outside))["size"] == "7"
                await client.remove_file(f"{prefix}/inside.txt")
                await client.remove_directory(prefix)
                await client.remove_file(outside)
        finally:
            await storage.close()


class TestMetadataAndStreaming:
    def test_fact_conversion_uses_utc_and_rejects_unknown_type(self) -> None:
        storage = FTPStorage(FTPConfig(host="ftp.example.test"))
        info = storage._file_info_from_facts(
            PurePosixPath("/file.txt"),
            {
                "type": "file",
                "size": "12",
                "modify": "20260102030405.123",
                "create": "20250102030405",
            },
        )
        assert info.size == 12
        assert info.modified is not None
        assert info.modified.tzinfo is UTC
        assert info.created is not None
        assert info.created.tzinfo is UTC

        with pytest.raises(OSError, match="Unsupported FTP entry type"):
            storage._file_info_from_facts(PurePosixPath("/link"), {"type": "slink", "size": "0"})

    async def test_empty_multichunk_and_offsets(self, ftp_storage: FTPStorage) -> None:
        base = f"/ftp-stream-{uid()}"
        await ftp_storage.upload_bytes(b"", f"{base}/empty.bin")
        await ftp_storage.upload_bytes(b"0123456789", f"{base}/data.bin")
        try:
            assert await ftp_storage.download_bytes(f"{base}/empty.bin") == b""
            assert await ftp_storage.download_bytes(f"{base}/data.bin") == b"0123456789"
            chunks = [chunk async for chunk in ftp_storage.download_stream(f"{base}/data.bin", offset=4)]
            assert b"".join(chunks) == b"456789"
            beyond = [chunk async for chunk in ftp_storage.download_stream(f"{base}/data.bin", offset=99)]
            assert beyond == []
            with pytest.raises(ValueError, match="non-negative"):
                await anext(ftp_storage.download_stream(f"{base}/data.bin", offset=-1))
        finally:
            await ftp_storage.rmtree(base)

    async def test_early_download_close_releases_client(self, ftp_storage: FTPStorage) -> None:
        path = f"/ftp-close-{uid()}.bin"
        await ftp_storage.upload_bytes(b"abcdefgh", path)
        try:
            stream = ftp_storage.download_stream(path)
            assert await anext(stream) == b"abcd"
            await stream.aclose()
            with anyio.fail_after(1):
                assert (await ftp_storage.stat(path)).size == 8
        finally:
            await ftp_storage.unlink(path, missing_ok=True)

    async def test_listing_snapshot_does_not_hold_lock(self, ftp_storage: FTPStorage) -> None:
        base = f"/ftp-list-{uid()}"
        await ftp_storage.upload_bytes(b"a", f"{base}/a.txt")
        await ftp_storage.upload_bytes(b"b", f"{base}/b.txt")
        try:
            async for entry in ftp_storage.iterdir(base):
                with anyio.fail_after(1):
                    assert await ftp_storage.stat(entry.path) == entry
        finally:
            await ftp_storage.rmtree(base)

    async def test_upload_holds_main_client_lock(self, ftp_storage: FTPStorage) -> None:
        path = f"/ftp-lock-{uid()}.bin"
        producer_started = anyio.Event()
        release_producer = anyio.Event()

        async def producer() -> AsyncIterator[bytes]:
            yield b"first"
            producer_started.set()
            await release_producer.wait()
            yield b"second"

        async def upload() -> None:
            await ftp_storage.upload_stream(producer(), path)

        try:
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(upload)
                await producer_started.wait()
                with anyio.move_on_after(0.05) as scope:
                    await ftp_storage.stat("/")
                assert scope.cancel_called
                release_producer.set()
            assert await ftp_storage.download_bytes(path) == b"firstsecond"
        finally:
            release_producer.set()
            await ftp_storage.unlink(path, missing_ok=True)


class TestCopyAndTrees:
    async def test_copy_uses_streaming_auxiliary_client(self, ftp_storage: FTPStorage) -> None:
        base = f"/ftp-copy-{uid()}"
        await ftp_storage.upload_bytes(b"0123456789", f"{base}/source.bin")
        try:
            await ftp_storage.copy(f"{base}/source.bin", f"{base}/copy.bin")
            assert await ftp_storage.download_bytes(f"{base}/copy.bin") == b"0123456789"
            with pytest.raises(FileExistsError):
                await ftp_storage.copy(f"{base}/source.bin", f"{base}/source.bin")
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

        original = FTPStorage._copy_stream_unlocked

        async def fail_second_copy(
            self: FTPStorage,
            source_path: PurePosixPath,
            destination_path: PurePosixPath,
            destination_client: aioftp.Client,
        ) -> None:
            if source_path.name == "b.txt":
                raise OSError("injected copy failure")
            await original(self, source_path, destination_path, destination_client)

        monkeypatch.setattr(FTPStorage, "_copy_stream_unlocked", fail_second_copy)
        try:
            with pytest.raises(OSError, match="injected copy failure"):
                await ftp_storage.copytree(source, destination, overwrite=True)
            assert await ftp_storage.download_bytes(destination / "keep.txt") == b"keep"
            assert not await ftp_storage.exists(destination / "a.txt")
            assert await ftp_storage.exists(source / "a.txt")
            assert await ftp_storage.exists(source / "b.txt")
        finally:
            monkeypatch.setattr(FTPStorage, "_copy_stream_unlocked", original)
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
            source_path: PurePosixPath,
            destination_path: PurePosixPath,
            *,
            overwrite: bool,
        ) -> None:
            _ = self, source_path, destination_path, overwrite
            raise OSError("injected tree failure")

        monkeypatch.setattr(FTPStorage, "_copytree_unlocked", fail_copytree)
        try:
            with pytest.raises(OSError, match="injected tree failure"):
                await ftp_storage.movetree(source, destination, overwrite=True)
            assert await ftp_storage.download_bytes(source / "file.txt") == b"source"
            assert await ftp_storage.download_bytes(destination / "existing.txt") == b"existing"
        finally:
            await ftp_storage.rmtree(base)


class TestLifecycleAndErrors:
    async def test_connect_and_close_are_idempotent(self, ftp_endpoint: tuple[str, int]) -> None:
        host, port = ftp_endpoint
        storage = FTPStorage(FTPConfig(host=host, port=port))
        await storage.connect()
        first_client = storage._client
        await storage.connect()
        assert storage._client is first_client
        await storage.close()
        await storage.close()
        assert storage._client is None

    async def test_missing_root_prefix_fails_connect(self, ftp_endpoint: tuple[str, int]) -> None:
        host, port = ftp_endpoint
        storage = FTPStorage(FTPConfig(host=host, port=port, root_prefix=f"/missing-{uid()}"))
        with pytest.raises(OSError, match="Failed to connect"):
            await storage.connect()
        assert storage._client is None

    async def test_authentication_error_becomes_permission_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        closed = False

        class FailingClient:
            async def connect(self, _host: str, _port: int) -> None:
                pass

            async def login(self, _username: str, _password: str) -> None:
                raise aioftp.StatusCodeError(aioftp.Code("2xx"), aioftp.Code("530"), "login failed")

            def close(self) -> None:
                nonlocal closed
                closed = True

        fake_client = FailingClient()
        storage = FTPStorage(FTPConfig(host="ftp.example.test"))

        def new_client() -> aioftp.Client:
            return cast("aioftp.Client", fake_client)

        monkeypatch.setattr(storage, "_new_client", new_client)
        with pytest.raises(PermissionError):
            await storage.connect()
        assert closed
        assert storage._client is None
