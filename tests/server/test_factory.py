"""Tests for server factory type guards and JSON loading."""

import json
from pathlib import Path

import pytest

from storegate.server.factory import resolve_server, resolve_server_from_file
from storegate.server.ftp import FTPServer
from storegate.storage.memory import MemoryStorage


class TestResolveServer:
    def test_accepts_server_instance_with_nested_storage(self) -> None:
        server = resolve_server(
            {
                "$factory": "@ftp",
                "storage": {"$factory": "~memory", "root": "server-root"},
                "host": "127.0.0.1",
                "port": 2021,
            }
        )

        assert isinstance(server, FTPServer)
        assert isinstance(server.storage, MemoryStorage)
        assert server.storage.display_id.endswith(":/server-root")
        assert server.host == "127.0.0.1"
        assert server.port == 2021

    def test_rejects_non_server_result(self) -> None:
        spec = {"$factory": "tests.support.factory_targets:return_number"}

        with pytest.raises(TypeError, match="Resolved object is not an AbstractServer: 'int'"):
            resolve_server(spec)


class TestResolveServerFromFile:
    def test_reads_json_and_resolves_nested_storage(self, tmp_path: Path) -> None:
        spec = {
            "$factory": "@ftp",
            "storage": {"$factory": "~memory", "root": "json-server-root"},
            "host": "127.0.0.2",
            "port": 2122,
        }
        spec_path = tmp_path / "server.json"
        spec_path.write_text(json.dumps(spec))

        server = resolve_server_from_file(spec_path)

        assert isinstance(server, FTPServer)
        assert isinstance(server.storage, MemoryStorage)
        assert server.storage.display_id.endswith(":/json-server-root")
        assert server.host == "127.0.0.2"
        assert server.port == 2122
