from __future__ import annotations

from .client import DavConfig as DavConfig
from .storage import DavStorage as DavStorage

Storage = DavStorage

__all__ = [
    "DavConfig",
    "DavStorage",
]
