"""Convenience re-exports for the factory trust-mode system.

Usage:

    from storegate.factory import (
        FactoryMode,
        OFFICIAL_FACTORIES,
        resolve_object,
        resolve_storage,
        resolve_server,
    )
"""

from __future__ import annotations

from storegate.server.factory import resolve_server as resolve_server
from storegate.storage.factory import resolve_storage as resolve_storage
from storegate.utils import (
    OFFICIAL_FACTORIES as OFFICIAL_FACTORIES,
)
from storegate.utils import (
    FactoryMode as FactoryMode,
)
from storegate.utils import (
    resolve_object as resolve_object,
)

__all__ = [
    "OFFICIAL_FACTORIES",
    "FactoryMode",
    "resolve_object",
    "resolve_server",
    "resolve_storage",
]
