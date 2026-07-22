"""Global pytest configuration."""

import sys

import pytest

pytest_plugins = (
    "tests.fixtures.protocol_servers",
    "tests.fixtures.storage",
)


@pytest.fixture(autouse=True, scope="session")
def configure_logging() -> None:
    """Configure logging for tests."""
    from storegate.log import log_format, log_level_filter, logger, remove_loguru_sinks

    remove_loguru_sinks()
    logger.add(
        sys.stdout,
        level="DEBUG",
        diagnose=False,
        enqueue=False,
        format=log_format,
        filter=log_level_filter(),
    )
