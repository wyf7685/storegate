import dataclasses
from datetime import datetime
from typing import Literal, TypedDict


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
