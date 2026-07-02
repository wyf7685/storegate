from .abstract import AbstractStorage
from .cos import CosStorage


def get_storage(storage_type: str | None = None) -> AbstractStorage:
    if storage_type == "cos" or storage_type is None:
        return CosStorage()
    raise ValueError(f"Unsupported storage type: {storage_type}")
