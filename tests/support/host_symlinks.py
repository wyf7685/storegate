"""Host-filesystem symlink-create probes for privilege-gated tests."""

from __future__ import annotations

import tempfile
from pathlib import Path


def probe_host_symlink_create() -> bool:
    """Return whether this process can create a real OS symbolic link.

    Mirrors the SFTP fixture probe: attempt ``Path.symlink_to`` in a temporary
    directory and clean up either outcome. Windows hosts without
    ``SeCreateSymbolicLinkPrivilege`` return ``False``.
    """
    with tempfile.TemporaryDirectory(prefix="storegate_symlink_probe_") as raw_root:
        root = Path(raw_root)
        probe_target = root / "probe-target"
        probe_link = root / "probe-link"
        probe_target.write_bytes(b"")
        try:
            probe_link.symlink_to(probe_target.name)
        except OSError:
            return False
        return True
