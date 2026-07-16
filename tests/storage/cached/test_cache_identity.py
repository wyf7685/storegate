"""Storage cache identity tests."""

from pathlib import Path

from pydantic import SecretStr

from app.storage.dav import DavConfig, DavStorage
from app.storage.index import IndexStorage
from app.storage.local import LocalStorage
from app.storage.memory import MemoryStorage
from app.storage.s3 import S3Storage
from app.storage.s3.client import S3Config


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


def test_storage_cache_identities_are_stable_and_non_secret(tmp_path: Path) -> None:
    local = LocalStorage(tmp_path / "one")
    assert local.cache_identity == LocalStorage(tmp_path / "one").cache_identity
    assert local.cache_identity != LocalStorage(tmp_path / "two").cache_identity

    s3 = S3Storage(_s3_config(secret_access_key="secret", endpoint_url="minio.example.test:9000"))
    assert (
        s3.cache_identity
        == S3Storage(
            _s3_config(secret_access_key="rotated-secret", endpoint_url="minio.example.test:9000")
        ).cache_identity
    )
    assert (
        s3.cache_identity
        != S3Storage(_s3_config(secret_access_key="secret", endpoint_url="other.example.test:9000")).cache_identity
    )
    assert "secret" not in s3.cache_identity

    dav = DavStorage(_dav_config(password="password", root_prefix="/files"))
    assert (
        dav.cache_identity == DavStorage(_dav_config(password="rotated-password", root_prefix="/files")).cache_identity
    )
    assert dav.cache_identity != DavStorage(_dav_config(password="password", root_prefix="/other")).cache_identity
    assert "password" not in dav.cache_identity

    index = IndexStorage(LocalStorage(tmp_path / "index"), LocalStorage(tmp_path / "chunks"), block_size=1024)
    changed_block_size = IndexStorage(
        LocalStorage(tmp_path / "index"),
        LocalStorage(tmp_path / "chunks"),
        block_size=2048,
    )
    assert index.cache_identity != changed_block_size.cache_identity
    assert MemoryStorage("/").cache_identity is None
