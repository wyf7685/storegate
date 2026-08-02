"""Storage namespace identity tests."""

from __future__ import annotations

from pathlib import Path

from pydantic import SecretStr

from storegate.storage.dav import DavConfig, DavStorage
from storegate.storage.index import IndexStorage
from storegate.storage.local import LocalStorage
from storegate.storage.memory import MemoryStorage
from storegate.storage.s3 import S3Storage
from storegate.storage.s3.client import S3Config


def _s3_config(*, secret_access_key: str, endpoint_url: str) -> S3Config:
    return S3Config(
        access_key_id=SecretStr("access-key"),
        secret_access_key=SecretStr(secret_access_key),
        region="test-region",
        bucket="test-bucket",
        endpoint_url=endpoint_url,
        path_style=True,
    )


def _dav_config(*, password: str, root_prefix: str) -> DavConfig:
    return DavConfig(
        base_url="https://dav.example.test/webdav",
        username="user",
        password=SecretStr(password),
        root_prefix=root_prefix,
    )


def test_storage_namespace_identities_are_stable_and_non_secret(tmp_path: Path) -> None:
    local = LocalStorage(tmp_path / "one")
    assert local.namespace_identity == LocalStorage(tmp_path / "one").namespace_identity
    assert local.namespace_identity != LocalStorage(tmp_path / "two").namespace_identity
    assert local.namespace_identity.startswith("local:sha256:")

    s3 = S3Storage(_s3_config(secret_access_key="secret", endpoint_url="minio.example.test:9000"))
    assert (
        s3.namespace_identity
        == S3Storage(
            _s3_config(secret_access_key="rotated-secret", endpoint_url="minio.example.test:9000")
        ).namespace_identity
    )
    assert (
        s3.namespace_identity
        != S3Storage(_s3_config(secret_access_key="secret", endpoint_url="other.example.test:9000")).namespace_identity
    )
    assert "secret" not in s3.namespace_identity
    assert s3.namespace_identity.startswith("s3:sha256:")

    dav = DavStorage(_dav_config(password="password", root_prefix="/files"))
    assert (
        dav.namespace_identity
        == DavStorage(_dav_config(password="rotated-password", root_prefix="/files")).namespace_identity
    )
    assert (
        dav.namespace_identity != DavStorage(_dav_config(password="password", root_prefix="/other")).namespace_identity
    )
    assert "password" not in dav.namespace_identity
    assert dav.namespace_identity.startswith("dav:sha256:")

    index = IndexStorage(LocalStorage(tmp_path / "index"), LocalStorage(tmp_path / "chunks"), block_size=1024)
    changed_block_size = IndexStorage(
        LocalStorage(tmp_path / "index"),
        LocalStorage(tmp_path / "chunks"),
        block_size=2048,
    )
    assert index.namespace_identity != changed_block_size.namespace_identity
    assert index.namespace_identity.startswith("index:sha256:")

    first_memory = MemoryStorage("/")
    second_memory = MemoryStorage("/")
    assert first_memory.namespace_identity.startswith("memory:sha256:")
    assert second_memory.namespace_identity.startswith("memory:sha256:")
    assert first_memory.namespace_identity != second_memory.namespace_identity
    assert first_memory.display_id.endswith(":/")
