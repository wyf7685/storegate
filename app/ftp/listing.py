"""Unix-like directory listing formatters for FTP LIST/NLST commands."""

from datetime import UTC, datetime

from app.storage import FileInfo


def format_list_line(info: FileInfo) -> str:
    """Format a single FileInfo entry as a Unix ``ls -l`` line."""
    mode = "d" if info.is_dir else "-"
    perms = "rwxr-xr-x" if info.is_dir else "rw-r--r--"
    size = info.size or 0

    modified = info.modified or datetime.fromtimestamp(0, tz=UTC)
    timestamp = (
        modified.strftime("%b %d %H:%M")
        if modified.year == datetime.now(tz=UTC).year
        else modified.strftime("%b %d  %Y")
    )

    return f"{mode}{perms} 1 none none {size:>12} {timestamp} {info.name}"


def format_list(items: list[FileInfo]) -> str:
    """Format a list of FileInfo entries as a Unix directory listing."""
    if not items:
        return "\r\n"
    return "\r\n".join(format_list_line(item) for item in items) + "\r\n"


def format_nlst(items: list[FileInfo]) -> str:
    """Format a list of FileInfo entries as plain filenames (NLST)."""
    if not items:
        return "\r\n"
    return "\r\n".join(info.name for info in items) + "\r\n"
