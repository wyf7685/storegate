"""WebDAV client configuration tests."""

import json
from pathlib import Path

import pytest
from pydantic import SecretStr

from storegate.storage.dav.client.models import DavConfig


class TestDavConfigValidation:
    def test_basic_requires_username_and_password(self) -> None:
        with pytest.raises(ValueError, match="basic auth requires"):
            DavConfig(base_url="https://host/dav", auth_mode="basic", username="user")

    def test_bearer_requires_token(self) -> None:
        with pytest.raises(ValueError, match="bearer auth requires"):
            DavConfig(base_url="https://host/dav", auth_mode="bearer")

    def test_anonymous_ignores_credentials(self) -> None:
        cfg = DavConfig(base_url="https://host/dav", auth_mode="anonymous")
        assert cfg.auth_mode == "anonymous"

    def test_invalid_scheme_rejected(self) -> None:
        with pytest.raises(ValueError, match="scheme"):
            DavConfig(base_url="ftp://host/dav", auth_mode="anonymous")

    def test_missing_host_rejected(self) -> None:
        with pytest.raises(ValueError, match="host"):
            DavConfig(base_url="https://", auth_mode="anonymous")

    def test_base_url_trailing_slash_stripped(self) -> None:
        cfg = DavConfig(base_url="https://host/dav/", auth_mode="anonymous")
        assert cfg.base_url == "https://host/dav"

    def test_root_prefix_normalized(self) -> None:
        cfg = DavConfig(base_url="https://host/dav", auth_mode="anonymous", root_prefix="storegate/")
        assert cfg.root_prefix == "/storegate"

    def test_root_prefix_empty_allowed(self) -> None:
        cfg = DavConfig(base_url="https://host/dav", auth_mode="anonymous")
        assert cfg.root_prefix == ""

    def test_valid_basic_config(self) -> None:
        cfg = DavConfig(
            base_url="https://host/dav",
            auth_mode="basic",
            username="user",
            password=SecretStr("secret"),
        )
        assert cfg.username == "user"
        assert cfg.password is not None
        assert cfg.password.get_secret_value() == "secret"

    def test_from_file(self, tmp_path: Path) -> None:
        config_file = tmp_path / "dav.json"
        config_file.write_text(
            json.dumps(
                {
                    "base_url": "https://host/dav",
                    "auth_mode": "bearer",
                    "token": "abc123",
                    "root_prefix": "/storegate",
                }
            )
        )
        cfg = DavConfig.from_file(config_file)
        assert cfg.auth_mode == "bearer"
        assert cfg.token is not None
        assert cfg.token.get_secret_value() == "abc123"
        assert cfg.root_prefix == "/storegate"
