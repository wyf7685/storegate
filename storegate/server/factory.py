from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from storegate.utils import FactoryMode, resolve_object

from .abstract import AbstractServer


def resolve_server(spec: dict[str, Any], mode: FactoryMode = FactoryMode.SAFE) -> AbstractServer:
    obj = resolve_object(spec, mode=mode)
    if not isinstance(obj, AbstractServer):
        raise TypeError(f"Resolved object is not an AbstractServer: {obj.__class__.__name__!r}")
    return obj


def resolve_server_from_file(spec_file: str | Path, mode: FactoryMode = FactoryMode.SAFE) -> AbstractServer:
    return resolve_server(json.loads(Path(spec_file).read_bytes()), mode=mode)
