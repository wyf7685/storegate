from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from storegate.storage.sftp import SFTPConfig, SFTPStorage
from tests.fixtures.protocol_servers import SFTPServerInfo


def make_config(server: SFTPServerInfo, **overrides: object) -> SFTPConfig:
    values: dict[str, object] = {
        "host": server.host,
        "port": server.port,
        "username": server.username,
        "password": SecretStr(server.password),
        "known_hosts": server.known_hosts,
        "root_prefix": server.root_prefix,
    }
    values.update(overrides)
    return SFTPConfig.model_validate(values)


@pytest.mark.integration
async def test_connect_ping_close_and_reconnect(sftp_server: SFTPServerInfo) -> None:
    storage = SFTPStorage(make_config(sftp_server))
    await storage.connect()
    assert await storage.ping()
    await storage.connect()
    await storage.close()
    assert not await storage.ping()
    await storage.connect()
    assert await storage.ping()
    await storage.close()


@pytest.mark.integration
async def test_wrong_password_is_rejected_without_leaking_secret(sftp_server: SFTPServerInfo) -> None:
    secret = "definitely-wrong-password"
    storage = SFTPStorage(make_config(sftp_server, password=SecretStr(secret)))
    with pytest.raises(PermissionError) as captured:
        await storage.connect()
    assert secret not in str(captured.value)


@pytest.mark.integration
async def test_wrong_host_key_is_rejected(sftp_server: SFTPServerInfo, tmp_path: Path) -> None:
    import asyncssh

    wrong_key = asyncssh.generate_private_key("ssh-ed25519").export_public_key().decode().strip()
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text(f"[{sftp_server.host}]:{sftp_server.port} {wrong_key}\n")
    storage = SFTPStorage(make_config(sftp_server, known_hosts=known_hosts))
    with pytest.raises(OSError, match="host key verification failed"):
        await storage.connect()


@pytest.mark.integration
async def test_explicitly_disabled_host_key_check(sftp_server: SFTPServerInfo) -> None:
    storage = SFTPStorage(make_config(sftp_server, known_hosts=None, disable_host_key_check=True))
    async with storage:
        assert await storage.ping()
