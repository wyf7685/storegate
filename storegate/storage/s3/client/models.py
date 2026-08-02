from __future__ import annotations

import dataclasses
from datetime import datetime
from pathlib import Path
from typing import Literal, Self, TypedDict

from pydantic import BaseModel, Field, SecretStr, model_validator


class S3Config(BaseModel):
    """Configuration for an S3-compatible storage backend.

    Supports AWS S3 (no ``endpoint_url``) and any S3-compatible service
    (MinIO, Alibaba OSS, Tencent COS, ...) via a custom ``endpoint_url``.
    """

    access_key_id: SecretStr
    secret_access_key: SecretStr
    region: str
    bucket: str
    # Custom endpoint host (no scheme), e.g. "cos.ap-shanghai.myqcloud.com" or "localhost:9000".
    # When omitted, AWS S3 virtual-hosted-style addressing is used.
    endpoint_url: str | None = None
    # Use path-style addressing (https://endpoint/bucket/key) instead of
    # virtual-hosted-style (https://bucket.endpoint/key). Required by MinIO
    # and some S3-compatible services.
    path_style: bool = False
    max_concurrency: int = Field(default=8, gt=0)
    session_token: str | None = None
    scheme: Literal["http", "https"] = "https"
    timeout: float = Field(default=30, gt=0)

    @model_validator(mode="after")
    def _validate(self) -> Self:
        region = self.region.strip()
        if not region:
            raise ValueError("region must not be empty")
        if "\x00" in region:
            raise ValueError("region must not contain NUL")
        self.region = region

        bucket = self.bucket.strip()
        if not bucket:
            raise ValueError("bucket must not be empty")
        if "\x00" in bucket:
            raise ValueError("bucket must not contain NUL")
        self.bucket = bucket

        if self.endpoint_url is not None:
            endpoint = self.endpoint_url.strip()
            if not endpoint:
                raise ValueError("endpoint_url must not be empty")
            if "\x00" in endpoint:
                raise ValueError("endpoint_url must not contain NUL")
            if "://" in endpoint:
                raise ValueError("endpoint_url must not include a scheme")
            if "@" in endpoint:
                raise ValueError("endpoint_url must not include userinfo")
            if "/" in endpoint or "?" in endpoint or "#" in endpoint:
                raise ValueError("endpoint_url must not include a path or query")
            self.endpoint_url = endpoint

        return self

    @classmethod
    def from_file(cls, path: str | Path) -> S3Config:
        return cls.model_validate_json(Path(path).read_bytes())


class CompletedPart(TypedDict):
    """A single part of a CompleteMultipartUpload request body (<Part> element)."""

    PartNumber: int
    ETag: str


@dataclasses.dataclass(frozen=True, slots=True)
class HeadObjectOutput:
    """Result of HeadObject (no XML body — fields come from response headers)."""

    content_length: int
    etag: str
    last_modified: datetime


@dataclasses.dataclass(frozen=True, slots=True)
class ListObjectsContents:
    """A <Contents> element from ListObjectsV2 (a single object)."""

    key: str
    size: int
    etag: str
    last_modified: datetime
    is_dir: Literal[False] = False


@dataclasses.dataclass(frozen=True, slots=True)
class ListObjectsCommonPrefix:
    """A <CommonPrefixes> element from ListObjectsV2 (a directory prefix)."""

    prefix: str
    is_dir: Literal[True] = True


@dataclasses.dataclass(frozen=True, slots=True)
class CopyObjectResult:
    """The <CopyObjectResult> element returned by CopyObject.

    Unlike COS, S3 does not return a CRC64 checksum.
    """

    etag: str
    last_modified: datetime


@dataclasses.dataclass(frozen=True, slots=True)
class CopyPartResult:
    """The <CopyPartResult> element returned by UploadPartCopy."""

    etag: str
    last_modified: datetime
