from storegate.utils import requires_extra

requires_extra("asyncssh", extra_name="sftp-storage")

from .config import SFTPConfig as SFTPConfig
from .storage import SFTPStorage as SFTPStorage

Storage = SFTPStorage

__all__ = [
    "SFTPConfig",
    "SFTPStorage",
]
