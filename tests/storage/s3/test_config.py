"""S3Config construction boundary tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from storegate.storage.factory import resolve_storage_from_file
from storegate.storage.s3 import S3Config, S3Storage


def _valid_kwargs(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "access_key_id": "akid",
        "secret_access_key": "secret",
        "region": "us-east-1",
        "bucket": "test-bucket",
    }
    base.update(overrides)
    return base


class TestS3Config:
    def test_defaults_and_valid_values(self) -> None:
        config = S3Config.model_validate(_valid_kwargs())
        assert config.region == "us-east-1"
        assert config.bucket == "test-bucket"
        assert config.scheme == "https"
        assert config.max_concurrency == 8
        assert config.timeout == 30
        assert config.endpoint_url is None
        assert config.access_key_id.get_secret_value() == "akid"
        assert config.secret_access_key.get_secret_value() == "secret"

    def test_endpoint_and_scheme_normalization(self) -> None:
        config = S3Config.model_validate(
            _valid_kwargs(
                endpoint_url="  localhost:9000  ",
                scheme="http",
                max_concurrency=1,
                timeout=0.5,
            )
        )
        assert config.endpoint_url == "localhost:9000"
        assert config.scheme == "http"
        assert config.max_concurrency == 1
        assert config.timeout == 0.5

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("max_concurrency", 0),
            ("max_concurrency", -1),
            ("timeout", 0),
            ("timeout", -1.0),
            ("scheme", "ftp"),
            ("scheme", "HTTP"),
            ("region", ""),
            ("region", "  "),
            ("region", "us\x00east"),
            ("bucket", ""),
            ("bucket", "  "),
            ("bucket", "b\x00ucket"),
            ("endpoint_url", ""),
            ("endpoint_url", "  "),
            ("endpoint_url", "https://localhost:9000"),
            ("endpoint_url", "user@localhost:9000"),
            ("endpoint_url", "localhost:9000/path"),
            ("endpoint_url", "localhost:9000?x=1"),
            ("endpoint_url", "localhost:9000#frag"),
            ("endpoint_url", "host\x00name"),
        ],
    )
    def test_invalid_values(self, field: str, value: object) -> None:
        with pytest.raises(ValidationError):
            S3Config.model_validate(_valid_kwargs(**{field: value}))

    def test_from_file_rejects_invalid_values(self, tmp_path: Path) -> None:
        path = tmp_path / "s3.json"
        path.write_text(
            json.dumps(
                {
                    "access_key_id": "akid",
                    "secret_access_key": "secret",
                    "region": "us-east-1",
                    "bucket": "bucket",
                    "max_concurrency": 0,
                }
            )
        )
        with pytest.raises(ValidationError):
            S3Config.from_file(path)

    def test_from_file_accepts_valid_values(self, tmp_path: Path) -> None:
        path = tmp_path / "s3.json"
        path.write_text(
            json.dumps(
                {
                    "access_key_id": "akid",
                    "secret_access_key": "secret",
                    "region": "us-east-1",
                    "bucket": "bucket",
                    "endpoint_url": "localhost:9000",
                    "path_style": True,
                    "scheme": "http",
                }
            )
        )
        config = S3Config.from_file(path)
        assert config.endpoint_url == "localhost:9000"
        assert config.scheme == "http"
        assert config.path_style is True

    def test_factory_from_file(self, tmp_path: Path) -> None:
        path = tmp_path / "storage.json"
        path.write_text(
            json.dumps(
                {
                    "$factory": "~s3",
                    "config": {
                        "access_key_id": "akid",
                        "secret_access_key": "secret",
                        "region": "us-east-1",
                        "bucket": "bucket",
                        "endpoint_url": "localhost:9000",
                        "path_style": True,
                        "scheme": "http",
                    },
                }
            )
        )
        storage = resolve_storage_from_file(path)
        assert isinstance(storage, S3Storage)
        assert storage.display_id == "s3:bucket:us-east-1"

    def test_storage_constructor_validates_config(self) -> None:
        with pytest.raises(ValidationError):
            S3Storage(
                S3Config(
                    access_key_id=SecretStr("akid"),
                    secret_access_key=SecretStr("secret"),
                    region="",
                    bucket="bucket",
                )
            )
