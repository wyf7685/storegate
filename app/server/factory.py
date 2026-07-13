from pathlib import Path

from app.utils import ObjectSpec

from .abstract import AbstractServer


def resolve_server(spec: ObjectSpec) -> AbstractServer:
    obj = spec.resolve()
    if not isinstance(obj, AbstractServer):
        raise TypeError(f"Resolved object is not an AbstractServer: {obj.__class__.__name__!r}")
    return obj


def resolve_server_from_file(spec_file: str | Path) -> AbstractServer:
    return resolve_server(ObjectSpec.model_validate_json(Path(spec_file).read_bytes()))
