"""FTPStorage behavior tests."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.storage.factory import resolve_storage_from_file
from app.storage.ftp import FTPConfig, FTPStorage


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
        assert config.max_connections == 1

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("host", ""),
            ("port", 0),
            ("port", 65536),
            ("chunk_size", 0),
            ("timeout", 0),
            ("max_connections", 0),
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
