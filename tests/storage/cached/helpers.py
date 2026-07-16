from app.storage.cached import CachedStorage


async def _clear_path(cached: CachedStorage, path: str) -> None:
    """Remove all cache entries for *path* (simulates a completely cold cache)."""
    await cached._cache.mdelete(
        ("exists", path),
        ("is_file", path),
        ("is_dir", path),
        ("stat", path),
        ("download", path),
        ("iterdir", path),
    )
