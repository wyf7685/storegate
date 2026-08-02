"""Serialisers for cached values.

These convert cached values to and from the bytes a persistent backend
stores. They live beside :class:`~.base.Namespace`, which pairs each
namespace with the codec for its value type, so a namespace's wire format is
defined in exactly one place.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from storegate.storage.abstract import EntryKind, FileInfo


def raw_bytes(value: bytes) -> bytes:
    return value


def bool_to_bytes(value: bool) -> bytes:
    return b"1" if value else b"0"


def bool_from_bytes(value: bytes) -> bool:
    return value == b"1"


def file_info_to_json(info: FileInfo) -> bytes:
    return json.dumps(_file_info_to_plain(info), separators=(",", ":")).encode()


def file_info_from_json(raw: bytes) -> FileInfo:
    obj = json.loads(raw.decode())
    if not isinstance(obj, dict):
        raise TypeError("FileInfo JSON must be an object")
    return _file_info_from_plain(obj)


def file_info_list_to_json(infos: list[FileInfo]) -> bytes:
    return json.dumps([_file_info_to_plain(info) for info in infos], separators=(",", ":")).encode()


def file_info_list_from_json(raw: bytes) -> list[FileInfo]:
    items = json.loads(raw.decode())
    if not isinstance(items, list):
        raise TypeError("FileInfo list JSON must be an array")
    if not all(isinstance(item, dict) for item in items):
        raise TypeError("FileInfo list entries must be objects")
    return [_file_info_from_plain(item) for item in items]


def _file_info_to_plain(info: FileInfo) -> dict[str, object]:
    data: dict[str, object] = {
        "path": info.path,
        "name": info.name,
        "kind": info.kind.value,
        "size": info.size,
    }
    if info.modified is not None:
        data["modified"] = info.modified.isoformat()
    if info.created is not None:
        data["created"] = info.created.isoformat()
    return data


def _file_info_from_plain(obj: dict[str, Any]) -> FileInfo:
    kind = EntryKind(obj["kind"])
    modified = datetime.fromisoformat(obj["modified"]) if "modified" in obj else None
    created = datetime.fromisoformat(obj["created"]) if "created" in obj else None
    return FileInfo(
        path=obj.get("path", ""),
        name=obj.get("name", ""),
        kind=kind,
        size=obj.get("size", 0),
        modified=modified,
        created=created,
    )
