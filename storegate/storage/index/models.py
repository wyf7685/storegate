from __future__ import annotations

from collections.abc import Mapping
from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator

from ..abstract import EntryKind, FileInfo


class FileMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    info: FileInfo
    chunks: list[str]

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_file_info(cls, value: object) -> object:
        if isinstance(value, Mapping):
            info = value.get("info")
            if isinstance(info, Mapping) and "is_dir" in info:
                raise ValueError("Legacy FileInfo is_dir metadata is not supported")
        return value

    @model_validator(mode="after")
    def require_file_kind(self) -> Self:
        if self.info.kind is not EntryKind.FILE:
            raise ValueError("Index FileMeta info must describe a regular file")
        return self
