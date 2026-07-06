import dataclasses
from datetime import datetime
from pathlib import Path
from typing import Literal, TypedDict

from pydantic import BaseModel, SecretStr


class CosConfig(BaseModel):
    secret_id: SecretStr
    secret_key: SecretStr
    region: str
    bucket: str
    is_internal: bool = False
    max_concurrency: int = 8
    token: str | None = None
    scheme: str = "https"
    timeout: float = 30

    @classmethod
    def from_file(cls, path: str | Path) -> CosConfig:
        return cls.model_validate_json(Path(path).read_bytes())


class MultipartUploadPart(TypedDict):
    PartNumber: int
    ETag: str


class CompleteMultipartUploadPayload(TypedDict):
    Part: list[MultipartUploadPart]


@dataclasses.dataclass(frozen=True, slots=True)
class HeadObjectResponse:
    content_length: int
    etag: str


@dataclasses.dataclass(frozen=True, slots=True)
class ListObjectsItem:
    key: str
    size: int
    etag: str
    last_modified: datetime
    is_dir: Literal[False] = False


@dataclasses.dataclass(frozen=True, slots=True)
class ListObjectsDir:
    prefix: str
    is_dir: Literal[True] = True


@dataclasses.dataclass(frozen=True, slots=True)
class CopyObjectResult:
    etag: str
    crc64: int
    last_modified: datetime


@dataclasses.dataclass(frozen=True, slots=True)
class CopyPartResult:
    etag: str
    last_modified: datetime
