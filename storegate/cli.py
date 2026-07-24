"""Command-line interface for storegate.

Provides ``storegate serve`` and ``storegate check`` without extra framework
dependencies. Exit codes follow the frozen package-cli contract (§4.13).
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any, NoReturn

from pydantic import ValidationError

from storegate.factory import FactoryMode, resolve_server
from storegate.log import configure_logging, logger
from storegate.server.abstract import AbstractServer

EXIT_OK = 0
EXIT_RUNTIME = 1
EXIT_CONFIG = 2
EXIT_MISSING_EXTRA = 3
EXIT_HEALTH = 4
EXIT_INTERRUPTED = 130


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="storegate",
        description="Asynchronous multi-backend file storage and protocol gateway",
    )
    parser.add_argument(
        "--trusted-factory",
        action="store_true",
        help="Allow non-official factory paths (default: SAFE official registry only)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Log level (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional log file path (default: no file sink)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    serve_p = sub.add_parser("serve", help="Start a protocol server from a config file")
    serve_p.add_argument("config", type=Path, help="Path to server JSON config")

    check_p = sub.add_parser("check", help="Validate config and ping storage without listening")
    check_p.add_argument("config", type=Path, help="Path to server JSON config")

    return parser


def _factory_mode(trusted: bool) -> FactoryMode:
    return FactoryMode.TRUSTED if trusted else FactoryMode.SAFE


def _load_config(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"Failed to read config {path}: {exc}") from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise TypeError(f"Config root must be a JSON object, got {type(data).__name__}")
    return data


def _resolve_server(path: Path, *, trusted: bool) -> AbstractServer:
    return resolve_server(_load_config(path), mode=_factory_mode(trusted))


def _is_missing_extra(exc: BaseException) -> bool:
    if not isinstance(exc, ImportError):
        return False
    message = str(exc)
    return "extra" in message.lower() or "storegate[" in message


def _is_config_error(exc: BaseException) -> bool:
    if isinstance(exc, (argparse.ArgumentError, json.JSONDecodeError, ValidationError, ValueError, TypeError)):
        return True
    # AttributeError from factory resolution (bad factory path) is config error.
    return isinstance(exc, AttributeError)


def _map_exception(exc: BaseException) -> int:
    if isinstance(exc, KeyboardInterrupt):
        return EXIT_INTERRUPTED
    # anyio / asyncio cancellation at CLI boundary
    if isinstance(exc, asyncio.CancelledError) or type(exc).__name__ == "CancelledError":
        return EXIT_INTERRUPTED
    if _is_missing_extra(exc):
        return EXIT_MISSING_EXTRA
    if _is_config_error(exc):
        return EXIT_CONFIG
    return EXIT_RUNTIME


async def _close_storage(server: AbstractServer, *, primary: BaseException | None = None) -> None:
    """Close connected storage; preserve *primary* over close failures (primary-first)."""
    try:
        await server.storage.close()
    except BaseException as close_error:
        if primary is None:
            raise
        logger.opt(exception=close_error).error("Failed to close storage after primary error")
        raise primary from None
    if primary is not None:
        raise primary


async def cmd_check(config: Path, *, trusted: bool) -> int:
    """Resolve config, connect storage, ping, and always close."""
    try:
        server = _resolve_server(config, trusted=trusted)
    except BaseException as exc:
        return _map_exception(exc)

    connected = False
    primary: BaseException | None = None
    exit_code = EXIT_OK
    try:
        try:
            await server.storage.connect()
            connected = True
        except BaseException as exc:
            # Connect failure is a health failure unless already classified
            # as config / missing-extra / interrupt.
            mapped = _map_exception(exc)
            return EXIT_HEALTH if mapped == EXIT_RUNTIME else mapped

        try:
            ok = await server.storage.ping()
        except BaseException as exc:
            primary = exc
            exit_code = _map_exception(exc)
        else:
            if not ok:
                exit_code = EXIT_HEALTH
    finally:
        close_result: int | None = None
        if connected:
            try:
                await _close_storage(server, primary=primary)
            except BaseException as raised:
                close_result = exit_code if primary is not None and raised is primary else _map_exception(raised)

    if close_result is not None:
        return close_result
    return exit_code


async def cmd_serve(config: Path, *, trusted: bool) -> int:
    """Resolve config and run the server until clean stop or failure.

    Non-loopback anonymous opt-in is enforced by server construction; this
    command must not bypass that check.
    """
    try:
        server = _resolve_server(config, trusted=trusted)
    except BaseException as exc:
        return _map_exception(exc)

    try:
        await server.serve()
    except BaseException as exc:
        return _map_exception(exc)
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return EXIT_OK
        if isinstance(code, int):
            # argparse uses 2 for usage errors; keep that as EXIT_CONFIG.
            return EXIT_CONFIG if code == 2 else code
        return EXIT_CONFIG

    # Process entry owns logging: drop Loguru's default stderr sink (and any
    # prior sinks) before installing storegate's format. Library callers of
    # configure_logging() must not wipe host sinks — only CLI does this.
    logger.remove()
    configure_logging(level=args.log_level, file_path=args.log_file)

    try:
        if args.command == "check":
            return asyncio.run(cmd_check(args.config, trusted=args.trusted_factory))
        if args.command == "serve":
            return asyncio.run(cmd_serve(args.config, trusted=args.trusted_factory))
        parser.error(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return EXIT_OK
        if isinstance(code, int):
            return code
        return EXIT_CONFIG
    except BaseException as exc:
        logger.opt(exception=exc).error("Unhandled CLI failure")
        return _map_exception(exc)

    return EXIT_OK


def run() -> NoReturn:
    raise SystemExit(main())


if __name__ == "__main__":
    run()
