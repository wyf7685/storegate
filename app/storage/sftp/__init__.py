from .config import SFTPConfig as SFTPConfig
from .storage import SFTPStorage as SFTPStorage

Storage = SFTPStorage

__all__ = [
    "SFTPConfig",
    "SFTPStorage",
]
