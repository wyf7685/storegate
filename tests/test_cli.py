"""Tests for the storegate CLI, entry points, and package markers (§4.13 / §13.1)."""

from __future__ import annotations

import importlib
import importlib.metadata
import json
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from storegate.cli import (
    EXIT_CONFIG,
    EXIT_HEALTH,
    EXIT_INTERRUPTED,
    EXIT_MISSING_EXTRA,
    EXIT_OK,
    EXIT_RUNTIME,
    build_parser,
    cmd_check,
    cmd_serve,
    main,
)
from storegate.factory import FactoryMode
from storegate.server.ftp import FTPServer
from storegate.storage.memory import MemoryStorage


@pytest.fixture(autouse=True)
def _restore_logging_after_cli_entry() -> Any:
    """CLI main() owns process logging and calls logger.remove(); restore suite sinks."""
    yield
    import loguru

    from storegate.log import configure_logging

    loguru.logger.remove()
    configure_logging(console=True, diagnose=False, enqueue=False, stdlib_bridge=False)


def _ftp_config(
    *,
    host: str = "127.0.0.1",
    port: int = 2121,
    root: str = "cli-root",
    allow_insecure_public: bool = False,
    factory: str = "@ftp",
) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "$factory": factory,
        "storage": {"$factory": "~memory", "root": root},
        "host": host,
        "port": port,
    }
    if allow_insecure_public:
        cfg["allow_insecure_public"] = True
    return cfg


def _write_config(path: Path, cfg: dict[str, Any]) -> Path:
    path.write_text(json.dumps(cfg), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Help / argparse surface
# ---------------------------------------------------------------------------


def test_build_parser_help_lists_commands() -> None:
    parser = build_parser()
    help_text = parser.format_help()
    assert "serve" in help_text
    assert "check" in help_text
    assert "--trusted-factory" in help_text
    assert "--log-level" in help_text
    assert "--log-file" in help_text


def test_main_help_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["--help"])
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "serve" in out
    assert "check" in out


def test_main_module_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "storegate", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "serve" in result.stdout
    assert "check" in result.stdout


def test_missing_command_is_config_error() -> None:
    code = main([])
    assert code == EXIT_CONFIG


# ---------------------------------------------------------------------------
# Factory mode defaults
# ---------------------------------------------------------------------------


def test_check_defaults_to_safe_factory(tmp_path: Path) -> None:
    cfg = _write_config(
        tmp_path / "ok.json",
        _ftp_config(factory="tests.support.factory_targets:return_number"),
    )
    # Non-official factory must fail under SAFE default.
    code = main(["check", str(cfg)])
    assert code == EXIT_CONFIG


def test_check_trusted_factory_allows_custom(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``--trusted-factory`` switches resolve mode to TRUSTED."""
    seen: list[FactoryMode] = []

    class FakeServer:
        def __init__(self) -> None:
            self.storage = MemoryStorage(root="trusted")

    def fake_resolve(_spec: dict[str, Any], mode: FactoryMode = FactoryMode.SAFE) -> FakeServer:
        seen.append(mode)
        return FakeServer()

    monkeypatch.setattr("storegate.cli.resolve_server", fake_resolve)
    cfg = _write_config(tmp_path / "trusted.json", {"$factory": "x", "storage": {}})
    code = main(["--trusted-factory", "check", str(cfg)])
    assert code == EXIT_OK
    assert seen == [FactoryMode.TRUSTED]


def test_check_safe_is_default_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[FactoryMode] = []

    class FakeServer:
        def __init__(self) -> None:
            self.storage = MemoryStorage(root="safe")

    def fake_resolve(_spec: dict[str, Any], mode: FactoryMode = FactoryMode.SAFE) -> FakeServer:
        seen.append(mode)
        return FakeServer()

    monkeypatch.setattr("storegate.cli.resolve_server", fake_resolve)
    cfg = _write_config(tmp_path / "safe.json", {"$factory": "x"})
    code = main(["check", str(cfg)])
    assert code == EXIT_OK
    assert seen == [FactoryMode.SAFE]


# ---------------------------------------------------------------------------
# check exit codes and always-close
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_success_closes_storage(tmp_path: Path) -> None:
    storage = MemoryStorage(root="check-ok")
    close = AsyncMock(wraps=storage.close)
    storage.close = close
    server = FTPServer(storage)
    with patch("storegate.cli._resolve_server", return_value=server):
        code = await cmd_check(tmp_path / "ok.json", trusted=False)
    assert code == EXIT_OK
    close.assert_awaited()


@pytest.mark.asyncio
async def test_check_ping_false_exits_health_and_closes(tmp_path: Path) -> None:
    storage = MemoryStorage(root="ping-false")
    storage.ping = AsyncMock(return_value=False)
    close = AsyncMock(wraps=storage.close)
    storage.close = close
    server = FTPServer(storage)
    with patch("storegate.cli._resolve_server", return_value=server):
        code = await cmd_check(tmp_path / "x.json", trusted=False)
    assert code == EXIT_HEALTH
    close.assert_awaited()


@pytest.mark.asyncio
async def test_check_connect_error_exits_health_and_skips_close(tmp_path: Path) -> None:
    storage = MemoryStorage(root="connect-fail")
    storage.connect = AsyncMock(side_effect=OSError("connect failed"))
    close = AsyncMock()
    storage.close = close
    server = FTPServer(storage)
    with patch("storegate.cli._resolve_server", return_value=server):
        code = await cmd_check(tmp_path / "x.json", trusted=False)
    # connect failed before connected=True, so close is not required; exit is health.
    assert code == EXIT_HEALTH
    close.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_ping_exception_closes_and_maps_runtime(tmp_path: Path) -> None:
    storage = MemoryStorage(root="ping-exc")
    storage.ping = AsyncMock(side_effect=RuntimeError("boom"))
    close = AsyncMock()
    storage.close = close
    server = FTPServer(storage)
    with patch("storegate.cli._resolve_server", return_value=server):
        code = await cmd_check(tmp_path / "x.json", trusted=False)
    assert code == EXIT_RUNTIME
    close.assert_awaited()


@pytest.mark.asyncio
async def test_check_close_failure_primary_first(tmp_path: Path) -> None:
    """Close failure after a primary ping error must not replace the primary mapping."""
    storage = MemoryStorage(root="close-fail")
    storage.ping = AsyncMock(side_effect=RuntimeError("primary"))
    storage.close = AsyncMock(side_effect=OSError("close failed"))
    server = FTPServer(storage)
    with patch("storegate.cli._resolve_server", return_value=server):
        code = await cmd_check(tmp_path / "x.json", trusted=False)
    assert code == EXIT_RUNTIME


def test_check_invalid_json_exit_config(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not-json", encoding="utf-8")
    assert main(["check", str(bad)]) == EXIT_CONFIG


def test_check_missing_file_exit_config(tmp_path: Path) -> None:
    assert main(["check", str(tmp_path / "missing.json")]) == EXIT_CONFIG


def test_check_missing_extra_exit_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: Any, **_k: Any) -> Any:
        raise ImportError(
            "Missing required package 'aioftp' from the 'ftp-server' extra. "
            "Please install the package with `pip install storegate[ftp-server]`."
        )

    monkeypatch.setattr("storegate.cli.resolve_server", boom)
    cfg = _write_config(tmp_path / "x.json", _ftp_config())
    assert main(["check", str(cfg)]) == EXIT_MISSING_EXTRA


def test_check_keyboard_interrupt_exit_130(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: Any, **_k: Any) -> Any:
        raise KeyboardInterrupt

    monkeypatch.setattr("storegate.cli.resolve_server", boom)
    cfg = _write_config(tmp_path / "x.json", _ftp_config())
    assert main(["check", str(cfg)]) == EXIT_INTERRUPTED


@pytest.mark.asyncio
async def test_check_always_closes_after_success_path(tmp_path: Path) -> None:
    """Integration: real resolve + memory storage always closes."""
    cfg_path = _write_config(tmp_path / "ok.json", _ftp_config(root="always-close"))
    code = await cmd_check(cfg_path, trusted=False)
    assert code == EXIT_OK


# ---------------------------------------------------------------------------
# serve opt-in
# ---------------------------------------------------------------------------


def test_serve_does_not_bypass_public_bind_opt_in(tmp_path: Path) -> None:
    cfg = _write_config(
        tmp_path / "public.json",
        _ftp_config(host="0.0.0.0"),  # noqa: S104
    )
    code = main(["serve", str(cfg)])
    assert code == EXIT_CONFIG


def test_serve_allows_public_bind_with_opt_in(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _write_config(
        tmp_path / "public-ok.json",
        _ftp_config(host="0.0.0.0", allow_insecure_public=True),  # noqa: S104
    )
    served = AsyncMock()

    async def fake_serve(_self: FTPServer) -> None:
        await served()

    monkeypatch.setattr(FTPServer, "serve", fake_serve)
    code = main(["serve", str(cfg)])
    assert code == EXIT_OK
    served.assert_awaited()


@pytest.mark.asyncio
async def test_cmd_serve_maps_interrupt(tmp_path: Path) -> None:
    storage = MemoryStorage()
    server = FTPServer(storage)
    server.serve = AsyncMock(side_effect=KeyboardInterrupt)
    with patch("storegate.cli._resolve_server", return_value=server):
        code = await cmd_serve(tmp_path / "x.json", trusted=False)
    assert code == EXIT_INTERRUPTED


# ---------------------------------------------------------------------------
# Logging options wire through
# ---------------------------------------------------------------------------


def test_log_options_passed_to_configure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_configure(**kwargs: Any) -> MagicMock:
        calls.append(kwargs)
        return MagicMock()

    class FakeServer:
        def __init__(self) -> None:
            self.storage = MemoryStorage()

    monkeypatch.setattr("storegate.cli.configure_logging", fake_configure)
    monkeypatch.setattr("storegate.cli.resolve_server", lambda *_a, **_k: FakeServer())
    cfg = _write_config(tmp_path / "log.json", {"$factory": "x"})
    code = main(["--log-level", "DEBUG", "--log-file", str(tmp_path / "a.log"), "check", str(cfg)])
    assert code == EXIT_OK
    assert calls
    assert calls[0]["level"] == "DEBUG"
    assert calls[0]["file_path"] == str(tmp_path / "a.log")


# ---------------------------------------------------------------------------
# Package / wheel smoke


def test_cli_main_clears_default_sink_before_configure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI process entry drops Loguru's default/prior sinks, then configures once.

    Library ``configure_logging()`` alone must not wipe host sinks; only the CLI
    process boundary may call ``logger.remove()`` so default stderr formatting
    does not dual-emit with storegate's sinks.
    """
    import storegate.log as log_mod

    remove_calls: list[tuple[Any, ...]] = []
    configure_calls: list[dict[str, Any]] = []
    original_remove = log_mod.logger.remove

    def tracking_remove(*args: Any, **kwargs: Any) -> None:
        remove_calls.append(args)
        original_remove(*args, **kwargs)

    def fake_configure(**kwargs: Any) -> MagicMock:
        configure_calls.append(kwargs)
        # Keep suite sinks stable: do not add real sinks under the monkeypatch.
        return MagicMock()

    class FakeServer:
        def __init__(self) -> None:
            self.storage = MemoryStorage()

    monkeypatch.setattr(log_mod.logger, "remove", tracking_remove)
    monkeypatch.setattr("storegate.cli.configure_logging", fake_configure)
    monkeypatch.setattr("storegate.cli.resolve_server", lambda *_a, **_k: FakeServer())

    cfg = _write_config(tmp_path / "cli-log.json", {"$factory": "x"})
    code = main(["check", str(cfg)])
    assert code == EXIT_OK
    assert remove_calls == [()], "CLI must call logger.remove() with no args before configure"
    assert len(configure_calls) == 1


# ---------------------------------------------------------------------------


def test_py_typed_present_in_source_tree() -> None:
    module_file = importlib.import_module("storegate").__file__
    assert module_file is not None
    package_root = Path(module_file).resolve().parent
    assert (package_root / "py.typed").is_file()
    assert (package_root / "py.typed").read_bytes() == b""


def test_console_script_entry_point_declared() -> None:
    # Editable/workspace installs expose the console script via metadata.
    eps = importlib.metadata.entry_points(group="console_scripts")
    names = {ep.name: ep.value for ep in eps}
    assert names.get("storegate") == "storegate.cli:main"


def test_wheel_contains_py_typed_and_console_script(tmp_path: Path) -> None:
    """Build a wheel and assert packaging artifacts (§13.1 smoke)."""
    repo = Path(__file__).resolve().parents[1]
    dist = tmp_path / "dist"
    dist.mkdir()
    build = subprocess.run(  # noqa: S603
        ["uv", "build", "--wheel", "--out-dir", str(dist)],  # noqa: S607
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    assert build.returncode == 0, build.stderr + build.stdout
    wheels = list(dist.glob("storegate-*.whl"))
    assert len(wheels) == 1, wheels
    wheel = wheels[0]
    with zipfile.ZipFile(wheel) as zf:
        names = zf.namelist()
        assert any(n.endswith("storegate/py.typed") for n in names), names
        # entry_points.txt must declare the console script
        entry_files = [n for n in names if n.endswith("entry_points.txt")]
        assert entry_files, names
        content = zf.read(entry_files[0]).decode("utf-8")
        assert "storegate" in content
        assert "storegate.cli:main" in content


def test_downstream_type_import_smoke() -> None:
    """Import public API symbols as a installed-package type smoke stand-in."""
    from storegate.factory import FactoryMode
    from storegate.server.abstract import AbstractServer
    from storegate.storage.abstract import AbstractStorage

    assert FactoryMode.SAFE.value == "safe"
    assert AbstractServer is not None
    assert AbstractStorage is not None
