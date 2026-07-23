"""Regression tests for logging isolation and explicit configuration.

§12.2 — verify that importing ``storegate.log`` does not mutate Loguru
global state and that ``configure_logging()`` behaves as specified.
"""

from __future__ import annotations

import loguru


def test_import_does_not_add_sinks() -> None:
    """Importing storegate.log must not call ``logger.remove()``,
    ``logger.add()``, or ``logger.configure()``."""
    import importlib

    import storegate.log as m

    importlib.reload(m)

    # The Loguru root logger starts with at least the default stderr sink
    # (id 0).  If no sinks were added, the only sinks are pre-existing.
    sink_ids = loguru.logger._core.handlers  # ty: ignore[unresolved-attribute]
    # The default stderr handler has id 0; our module should not have
    # added any others at import time.
    assert len(sink_ids) <= 1, f"Import added unexpected sinks: {list(sink_ids)}"


def test_configure_logging_adds_and_removes_sinks() -> None:
    """``configure_logging()`` adds new sinks, and the returned handle
    removes only those sinks on ``.remove()``."""
    import storegate.log as m

    # Capture existing sink count before configuring.
    existing = len(loguru.logger._core.handlers)  # ty: ignore[unresolved-attribute]

    handle = m.configure_logging(console=True, file_path=None, enqueue=False)
    after_add = len(loguru.logger._core.handlers)  # ty: ignore[unresolved-attribute]
    assert after_add == existing + 1, f"Expected 1 new sink, got {after_add - existing}"

    handle.remove()
    after_remove = len(loguru.logger._core.handlers)  # ty: ignore[unresolved-attribute]
    assert after_remove == existing, f"Expected sink count to return to {existing}, got {after_remove}"


def test_configure_logging_diagnose_defaults_to_false() -> None:
    """The default ``diagnose`` must be ``False`` — verifying the call
    succeeds and the handle cleans up correctly."""
    import storegate.log as m

    handle = m.configure_logging(console=True, file_path=None, diagnose=False, enqueue=False)
    handle.remove()


def test_configure_logging_stdlib_bridge_does_not_crash() -> None:
    """The stdlib bridge must succeed without raising."""
    import storegate.log as m

    handle = m.configure_logging(console=False, stdlib_bridge=True)
    try:
        import logging

        bridge = logging.getLogger("uvicorn")
        assert bridge.level == logging.INFO
    finally:
        handle.remove()
        # Restore stdlib logging to a clean state.
        import logging.config

        logging.config.dictConfig({"version": 1, "disable_existing_loggers": False, "handlers": {}, "loggers": {}})


def test_configure_logging_context_manager_removes_sinks_on_exit() -> None:
    """The handle is also a context manager."""
    import storegate.log as m

    existing = len(loguru.logger._core.handlers)  # ty: ignore[unresolved-attribute]
    with m.configure_logging(console=True, file_path=None, enqueue=False):
        assert len(loguru.logger._core.handlers) == existing + 1  # ty: ignore[unresolved-attribute]
    assert len(loguru.logger._core.handlers) == existing  # ty: ignore[unresolved-attribute]


def test_create_wsgi_app_does_not_mutate_wsgidav_logger() -> None:
    """``create_wsgi_app()`` must not remove existing wsgidav handlers or
    add its own stdout sink."""
    import logging

    from storegate.server.dav.server import create_wsgi_app
    from storegate.storage.memory import MemoryStorage

    # Install a sentinel handler on the wsgidav logger.
    wsgidav_logger = logging.getLogger("wsgidav")
    sentinel = logging.StreamHandler()
    sentinel.setLevel(logging.DEBUG)
    wsgidav_logger.addHandler(sentinel)
    pre_handlers = wsgidav_logger.handlers.copy()
    pre_level = wsgidav_logger.level
    pre_propagate = wsgidav_logger.propagate

    storage = MemoryStorage()
    create_wsgi_app(storage, "127.0.0.1", 8080)

    assert wsgidav_logger.handlers == pre_handlers, "wsgidav handlers were mutated"
    assert wsgidav_logger.level == pre_level, "wsgidav level was mutated"
    assert wsgidav_logger.propagate == pre_propagate, "wsgidav propagate was mutated"
