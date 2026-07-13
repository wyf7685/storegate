from .dav_client import DavConfig as DavConfig
from .storage import DavStorage as DavStorage

Storage = DavStorage

__all__ = [
    "DavConfig",
    "DavStorage",
]
