from .backend import CacheBackend as CacheBackend
from .backend.memory import MemoryCacheBackend as MemoryCacheBackend
from .backend.redis import RedisCacheBackend as RedisCacheBackend
from .storage import CachedStorage as CachedStorage

Storage = CachedStorage
