"""Global pytest configuration."""

from __future__ import annotations

import pytest

pytest_plugins = (
    "tests.fixtures.protocol_servers",
    "tests.fixtures.storage",
)


@pytest.fixture(autouse=True, scope="session")
def _configure_test_logging() -> None:
    """Configure logging for tests — removes existing sinks first, then adds
    a console sink with ``diagnose=False``."""
    import loguru

    from storegate.log import configure_logging

    loguru.logger.remove()
    configure_logging(console=True, diagnose=False, enqueue=False)
