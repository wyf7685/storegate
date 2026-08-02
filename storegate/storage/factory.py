from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from storegate.utils import FactoryMode, resolve_object

from .abstract import AbstractStorage


def resolve_storage(spec: dict[str, Any], mode: FactoryMode = FactoryMode.SAFE) -> AbstractStorage:
    obj = resolve_object(spec, mode=mode)
    if not isinstance(obj, AbstractStorage):
        raise TypeError(f"Resolved object is not an AbstractStorage: {obj.__class__.__name__!r}")
    return obj


def resolve_storage_from_file(spec_file: str | Path, mode: FactoryMode = FactoryMode.SAFE) -> AbstractStorage:
    return resolve_storage(json.loads(Path(spec_file).read_bytes()), mode=mode)
