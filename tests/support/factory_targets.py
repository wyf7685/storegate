from storegate.storage.abstract import AbstractStorage


def bundle(
    storage: AbstractStorage,
    payload: dict[str, list[int]],
    records: list[dict[str, str]],
) -> tuple[AbstractStorage, dict[str, list[int]], list[dict[str, str]]]:
    """Return arguments for nested factory and coercion tests."""
    return storage, payload, records


def pick_storage(storage: AbstractStorage) -> AbstractStorage:
    """Return a storage passed through a factory specification."""
    return storage


def return_number() -> int:
    """Return a non-storage result for type-guard tests."""
    return 42


NOT_CALLABLE = 123
