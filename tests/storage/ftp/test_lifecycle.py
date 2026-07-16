"""FTPStorage behavior tests."""

from typing import cast

import aioftp
import pytest

from app.storage.ftp import FTPConfig, FTPStorage
from tests.support.ids import uid

pytestmark = pytest.mark.integration


class TestLifecycleAndErrors:
    async def test_connect_and_close_are_idempotent(self, ftp_endpoint: tuple[str, int]) -> None:
        host, port = ftp_endpoint
        storage = FTPStorage(FTPConfig(host=host, port=port))
        await storage.connect()
        first_client = storage._pool._idle[-1]
        await storage.connect()
        assert storage._pool._idle[-1] is first_client
        assert storage._pool._total == 1
        await storage.close()
        await storage.close()
        assert storage._pool._total == 0
        assert not storage._pool.is_open

    async def test_missing_root_prefix_fails_connect(self, ftp_endpoint: tuple[str, int]) -> None:
        host, port = ftp_endpoint
        storage = FTPStorage(FTPConfig(host=host, port=port, root_prefix=f"/missing-{uid()}"))
        with pytest.raises(OSError, match="Failed to connect"):
            await storage.connect()
        assert storage._pool._total == 0
        assert not storage._pool.is_open

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
        assert storage._pool._total == 0
        assert not storage._pool.is_open
