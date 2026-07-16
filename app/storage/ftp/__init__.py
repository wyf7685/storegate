from .config import FTPConfig as FTPConfig
from .storage import FTPStorage as FTPStorage

Storage = FTPStorage

__all__ = [
    "FTPConfig",
    "FTPStorage",
]
