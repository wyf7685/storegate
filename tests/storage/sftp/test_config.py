import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.storage.sftp.config import SFTPConfig


class TestSFTPConfig:
    def test_defaults_and_normalization(self) -> None:
        config = SFTPConfig(
            host=" SFTP.EXAMPLE.TEST ",
            username=" user ",
            password="secret",
            root_prefix="//tenant/./files//",
        )
        assert config.host == "sftp.example.test"
        assert config.port == 22
        assert config.username == "user"
        assert config.password is not None
        assert config.password.get_secret_value() == "secret"
        assert config.client_keys == ()
        assert config.root_prefix == "/tenant/files"
        assert config.chunk_size == 1024 * 1024
        assert config.max_channels == 4
        assert config.connect_timeout == 30.0
        assert config.login_timeout == 30.0
        assert config.io_timeout == 30.0
        assert config.close_timeout == 30.0
        assert config.keepalive_interval == 30.0
        assert config.keepalive_count_max == 3

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("host", ""),
            ("host", "bad\x00host"),
            ("username", ""),
            ("username", "bad\x00user"),
            ("port", 0),
            ("port", 65536),
            ("chunk_size", 0),
            ("max_channels", 0),
            ("connect_timeout", 0),
            ("login_timeout", 0),
            ("io_timeout", 0),
            ("close_timeout", 0),
            ("keepalive_interval", -1),
            ("keepalive_count_max", 0),
            ("root_prefix", "relative"),
            ("root_prefix", "/a/../b"),
            ("root_prefix", "/a\x00b"),
        ],
    )
    def test_invalid_values(self, field: str, value: object) -> None:
        kwargs: dict[str, object] = {
            "host": "sftp.example.test",
            "username": "user",
            "password": "secret",
            field: value,
        }
        with pytest.raises(ValidationError):
            SFTPConfig.model_validate(kwargs)

    def test_requires_explicit_authentication(self) -> None:
        with pytest.raises(ValidationError, match="password or at least one client key"):
            SFTPConfig(host="sftp.example.test", username="user")

    def test_accepts_client_key_authentication(self, tmp_path: Path) -> None:
        key_path = tmp_path / "id_ed25519"
        config = SFTPConfig(host="sftp.example.test", username="user", client_keys=[key_path])
        assert config.client_keys == (key_path,)

    def test_rejects_empty_client_key_path(self) -> None:
        with pytest.raises(ValidationError, match="client key paths must not be empty"):
            SFTPConfig(host="sftp.example.test", username="user", client_keys=[""])

    def test_passphrase_requires_client_key(self) -> None:
        with pytest.raises(ValidationError, match="passphrase requires"):
            SFTPConfig(host="sftp.example.test", username="user", password="secret", passphrase="key-secret")

    def test_known_hosts_conflicts_with_disabled_check(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError, match="mutually exclusive"):
            SFTPConfig(
                host="sftp.example.test",
                username="user",
                password="secret",
                known_hosts=tmp_path / "known_hosts",
                disable_host_key_check=True,
            )

    def test_secrets_are_redacted(self) -> None:
        config = SFTPConfig(
            host="sftp.example.test",
            username="user",
            password="password-secret",
            client_keys=["id_ed25519"],
            passphrase="passphrase-secret",
        )
        rendered = repr(config)
        assert "password-secret" not in rendered
        assert "passphrase-secret" not in rendered

    def test_from_file(self, tmp_path: Path) -> None:
        path = tmp_path / "sftp.json"
        path.write_text(
            json.dumps(
                {
                    "host": "sftp.example.test",
                    "username": "user",
                    "password": "secret",
                    "root_prefix": "/tenant",
                }
            )
        )
        config = SFTPConfig.from_file(path)
        assert config.host == "sftp.example.test"
        assert config.password is not None
        assert config.password.get_secret_value() == "secret"
        assert config.root_prefix == "/tenant"
