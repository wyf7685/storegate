import importlib
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from .abstract import AbstractStorage


def resolve_dot_notation(obj_str: str, prefix: str) -> Any:
    modulename, _, cls = obj_str.partition(":")
    # if prefix and modulename.startswith("~"):
    #     modulename = prefix + modulename[1:]
    if prefix:
        if modulename.startswith("~."):
            modulename = prefix + modulename[1:]
        elif modulename.startswith("~"):
            modulename = prefix + "." + modulename[1:]
    module = importlib.import_module(modulename)
    instance = module
    for attr_str in cls.split("."):
        instance = getattr(instance, attr_str)
    return instance


class ObjectSpec(BaseModel):
    factory: str
    args: dict[str, str | int | float | bool | None | ObjectSpec] | None = None

    def resolve(self) -> Any:
        assert __package__ is not None
        factory = resolve_dot_notation(self.factory, __package__)
        if self.args is None:
            return factory()
        resolved_args: dict[str, Any] = {}
        for key, value in self.args.items():
            if isinstance(value, ObjectSpec):
                resolved_args[key] = value.resolve()
            else:
                resolved_args[key] = value
        return factory(**resolved_args)


def resolve_storage(spec: ObjectSpec) -> AbstractStorage:
    obj = spec.resolve()
    if not isinstance(obj, AbstractStorage):
        raise TypeError(f"Resolved object is not an AbstractStorage: {obj.__class__.__name__!r}")
    return obj


def resolve_storage_from_file(spec_file: str | Path) -> AbstractStorage:
    return resolve_storage(ObjectSpec.model_validate_json(Path(spec_file).read_bytes()))
