from __future__ import annotations

import os
import shutil
import stat
import tempfile
import uuid
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from ..abstract import EntryKind


class _RawKind(StrEnum):
    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"
    JUNCTION = "junction"
    SPECIAL = "special"


@dataclass(slots=True, frozen=True)
class _TreeEntry:
    relative_parts: tuple[str, ...]
    kind: EntryKind
    link_target: str | None = None
    target_is_directory: bool = False


@dataclass(slots=True)
class _MutationJournal:
    root: Path
    created: list[Path]
    backups: list[tuple[Path, Path]]
    backup_root: Path | None = None

    @classmethod
    def create(cls, root: Path) -> _MutationJournal:
        return cls(root=root, created=[], backups=[])

    def record_created(self, path: Path) -> None:
        self.created.append(path)

    def backup(self, path: Path) -> None:
        if self.backup_root is None:
            self.backup_root = Path(tempfile.mkdtemp(prefix=".storegate-local-backup-", dir=self.root))
        backup_path = self.backup_root / uuid.uuid4().hex
        path.replace(backup_path)
        self.backups.append((path, backup_path))

    def rollback(self) -> None:
        for path in reversed(self.created):
            try:
                result = os.lstat(path)
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(result.st_mode) and not stat.S_ISLNK(result.st_mode):
                path.rmdir()
            else:
                path.unlink()
        for original, backup in reversed(self.backups):
            original.parent.mkdir(parents=True, exist_ok=True)
            backup.replace(original)
        self.cleanup()

    def cleanup(self) -> None:
        if self.backup_root is not None:
            shutil.rmtree(self.backup_root, ignore_errors=False)
            self.backup_root = None
