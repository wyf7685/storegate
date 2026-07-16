import contextlib
import hashlib
from contextlib import AbstractContextManager

BLOCK_SIZE = 16 * 1024


def _hash_block(data: bytes) -> str:
    """Return the SHA-256 hex digest of a single block."""

    return hashlib.sha256(data).hexdigest()


def hash_to_path_stem(chunk_hash: str) -> str:
    """Convert a chunk hash to its path stem: 'aa/bbbb/rest...'."""
    return f"{chunk_hash[:2]}/{chunk_hash[2:6]}/{chunk_hash[6:]}"


def suppress_exc() -> AbstractContextManager[None]:
    """Return a contextlib.suppress for all exceptions."""
    return contextlib.suppress(Exception)
