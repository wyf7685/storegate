from pathlib import Path

from app.utils import ObjectSpec

from .abstract import AbstractStorage


def resolve_storage(spec: ObjectSpec) -> AbstractStorage:
    obj = spec.resolve()
    if not isinstance(obj, AbstractStorage):
        raise TypeError(f"Resolved object is not an AbstractStorage: {obj.__class__.__name__!r}")
    return obj


def resolve_storage_from_file(spec_file: str | Path) -> AbstractStorage:
    return resolve_storage(ObjectSpec.model_validate_json(Path(spec_file).read_bytes()))
