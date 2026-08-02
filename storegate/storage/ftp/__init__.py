from __future__ import annotations

from storegate.utils import requires_extra

requires_extra("aioftp", extra_name="ftp-storage")

from .config import FTPConfig as FTPConfig
from .storage import FTPStorage as FTPStorage

Storage = FTPStorage

__all__ = [
    "FTPConfig",
    "FTPStorage",
]
