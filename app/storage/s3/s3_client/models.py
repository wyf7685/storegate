import dataclasses
from datetime import datetime
from pathlib import Path
from typing import Literal, TypedDict

from pydantic import BaseModel, SecretStr


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
    max_concurrency: int = 8
    session_token: str | None = None
    scheme: str = "https"
    timeout: float = 30

    @classmethod
    def from_file(cls, path: str | Path) -> S3Config:
        return cls.model_validate_json(Path(path).read_bytes())


class CompletedPart(TypedDict):
    """A single part of a CompleteMultipartUpload request body (<Part> element)."""

    PartNumber: int
    ETag: str


class CompleteMultipartUploadPayload(TypedDict):
    Part: list[CompletedPart]


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
