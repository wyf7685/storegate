from __future__ import annotations

import uuid


def uid() -> str:
    """Return a short unique identifier for test isolation."""
    return uuid.uuid4().hex[:12]
