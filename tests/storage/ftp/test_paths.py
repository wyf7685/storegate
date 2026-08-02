"""FTPStorage behavior tests."""

from __future__ import annotations

import aioftp
import pytest
from pydantic import SecretStr

from storegate.storage.ftp import FTPConfig, FTPStorage
from tests.support.ids import uid

pytestmark = pytest.mark.integration


class TestIdentityAndPaths:
    def test_identity_is_account_scoped_and_secret_free(self) -> None:
        first = FTPStorage(
            FTPConfig(
                host="ftp.example.test",
                username="alice",
                password=SecretStr("first"),
                root_prefix="/files",
            )
        )
        rotated = FTPStorage(
            FTPConfig(
                host="ftp.example.test",
                username="alice",
                password=SecretStr("second"),
                root_prefix="/files",
            )
        )
        other_user = FTPStorage(
            FTPConfig(
                host="ftp.example.test",
                username="bob",
                password=SecretStr("first"),
                root_prefix="/files",
            )
        )

        assert first.namespace_identity == rotated.namespace_identity
        assert first.namespace_identity != other_user.namespace_identity
        assert first.display_id != other_user.display_id
        assert "first" not in first.display_id
        assert "first" not in first.namespace_identity

        wider_pool = FTPStorage(
            FTPConfig(
                host="ftp.example.test",
                username="alice",
                password=SecretStr("first"),
                root_prefix="/files",
                max_connections=4,
            )
        )
        assert first.display_id == wider_pool.display_id
        assert first.namespace_identity == wider_pool.namespace_identity

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
        with pytest.raises(IsADirectoryError):
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
