from __future__ import annotations

from storegate.storage.cached import CachedStorage
from storegate.storage.cached.backend.base import DOWNLOAD, EXISTS, IS_DIR, IS_FILE, IS_SYMLINK, ITERDIR, LSTAT, STAT


async def _clear_path(cached: CachedStorage, path: str) -> None:
    """Remove all cache entries for *path* (simulates a completely cold cache)."""
    await cached._cache.mdelete(
        (EXISTS, path),
        (IS_FILE, path),
        (IS_DIR, path),
        (IS_SYMLINK, path),
        (STAT, path),
        (LSTAT, path),
        (DOWNLOAD, path),
        (ITERDIR, path),
    )
