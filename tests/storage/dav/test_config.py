"""WebDAV client configuration tests."""

import json
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from storegate.storage.dav import DavStorage
from storegate.storage.dav.client.models import DavConfig
from storegate.storage.factory import resolve_storage_from_file


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

    def test_root_prefix_collapses_dots_and_slashes(self) -> None:
        cfg = DavConfig(
            base_url="https://host/dav",
            auth_mode="anonymous",
            root_prefix="/tenant//./files/",
        )
        assert cfg.root_prefix == "/tenant/files"

    def test_root_prefix_empty_allowed(self) -> None:
        cfg = DavConfig(base_url="https://host/dav", auth_mode="anonymous")
        assert cfg.root_prefix == ""

    def test_root_prefix_slash_only_normalized_to_empty(self) -> None:
        cfg = DavConfig(base_url="https://host/dav", auth_mode="anonymous", root_prefix="/")
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

    def test_defaults_are_positive(self) -> None:
        cfg = DavConfig(base_url="https://host/dav", auth_mode="anonymous")
        assert cfg.timeout == 30.0
        assert cfg.max_concurrency == 8
        assert cfg.chunk_size == 4 * 1024 * 1024

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("timeout", 0),
            ("timeout", -1),
            ("max_concurrency", 0),
            ("max_concurrency", -1),
            ("chunk_size", 0),
            ("chunk_size", -1),
            ("root_prefix", "/a/../b"),
            ("root_prefix", "../escape"),
            ("root_prefix", "/a\x00b"),
            ("root_prefix", "a\x00b"),
        ],
    )
    def test_invalid_values(self, field: str, value: object) -> None:
        kwargs: dict[str, object] = {
            "base_url": "https://host/dav",
            "auth_mode": "anonymous",
            field: value,
        }
        with pytest.raises(ValidationError):
            DavConfig.model_validate(kwargs)

    def test_direct_constructor_rejects_zero_concurrency(self) -> None:
        with pytest.raises(ValidationError):
            DavConfig(base_url="https://host/dav", auth_mode="anonymous", max_concurrency=0)

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

    def test_from_file_rejects_invalid_boundaries(self, tmp_path: Path) -> None:
        config_file = tmp_path / "dav-bad.json"
        config_file.write_text(
            json.dumps(
                {
                    "base_url": "https://host/dav",
                    "auth_mode": "anonymous",
                    "max_concurrency": 0,
                    "root_prefix": "/a/../b",
                }
            )
        )
        with pytest.raises(ValidationError):
            DavConfig.from_file(config_file)

    def test_factory_from_file(self, tmp_path: Path) -> None:
        path = tmp_path / "storage.json"
        path.write_text(
            json.dumps(
                {
                    "$factory": "~dav",
                    "config": {
                        "base_url": "https://host/dav",
                        "auth_mode": "anonymous",
                        "root_prefix": "tenant/files/",
                    },
                }
            )
        )
        storage = resolve_storage_from_file(path)
        assert isinstance(storage, DavStorage)
        assert storage.display_id == "dav:host:443/tenant/files"

    def test_factory_from_file_rejects_invalid_config(self, tmp_path: Path) -> None:
        path = tmp_path / "storage-bad.json"
        path.write_text(
            json.dumps(
                {
                    "$factory": "~dav",
                    "config": {
                        "base_url": "https://host/dav",
                        "auth_mode": "anonymous",
                        "chunk_size": 0,
                    },
                }
            )
        )
        with pytest.raises(ValidationError):
            resolve_storage_from_file(path)
