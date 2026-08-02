from __future__ import annotations

import argparse
import ast
import io
import sys
import tokenize
from collections.abc import Iterable, Sequence
from pathlib import Path

FUTURE_IMPORT = "from __future__ import annotations"
EXCLUDED_DIRECTORIES = frozenset(
    {
        ".git",
        ".local",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".tox",
        ".venv",
        ".worktrees",
        "__pycache__",
        "build",
        "dist",
        "venv",
    }
)


def _is_excluded(path: Path) -> bool:
    return any(part in EXCLUDED_DIRECTORIES for part in path.parts)


def _iter_python_files(paths: Iterable[Path]) -> Iterable[Path]:
    seen: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved.is_file():
            candidates = (resolved,) if resolved.suffix == ".py" else ()
        elif resolved.is_dir():
            candidates = resolved.rglob("*.py")
        else:
            msg = f"path does not exist: {path}"
            raise FileNotFoundError(msg)

        for candidate in candidates:
            if _is_excluded(candidate):
                continue
            candidate = candidate.resolve()
            if candidate not in seen:
                seen.add(candidate)
                yield candidate


def _decode_source(data: bytes) -> tuple[str, str]:
    encoding, _ = tokenize.detect_encoding(io.BytesIO(data).readline)
    return data.decode(encoding), encoding


def _has_future_annotations(module: ast.Module) -> bool:
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == "__future__"
        and any(alias.name == "annotations" for alias in node.names)
        for node in module.body
    )


def _insertion_line(module: ast.Module, lines: list[str]) -> tuple[int, bool]:
    if not module.body:
        return len(lines), False

    first = module.body[0]
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
        if first.end_lineno is None:
            msg = "module docstring has no end position"
            raise ValueError(msg)
        return first.end_lineno, True

    return first.lineno - 1, False


def _add_future_import(source: str, path: Path) -> tuple[str, bool]:
    module = ast.parse(source, filename=str(path))
    if _has_future_annotations(module):
        return source, False

    newline = "\r\n" if "\r\n" in source else "\n"
    lines = source.splitlines(keepends=True)
    line_index, follows_docstring = _insertion_line(module, lines)

    prefix = lines[:line_index]
    suffix = lines[line_index:]
    while suffix and not suffix[0].strip():
        suffix.pop(0)

    insertion: list[str] = []
    if follows_docstring and prefix and prefix[-1].strip():
        insertion.append(newline)
    insertion.extend((FUTURE_IMPORT + newline, newline))

    return "".join((*prefix, *insertion, *suffix)), True


def _process_file(path: Path, *, check: bool) -> bool:
    data = path.read_bytes()
    source, encoding = _decode_source(data)
    updated, changed = _add_future_import(source, path)
    if changed and not check:
        path.write_bytes(updated.encode(encoding))
    return changed


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    repository_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Add postponed annotation evaluation to Python source files.")
    parser.add_argument("paths", nargs="*", type=Path, default=[repository_root])
    parser.add_argument(
        "--check",
        action="store_true",
        help="report files missing the import without modifying them",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    changed = [path for path in _iter_python_files(args.paths) if _process_file(path, check=args.check)]

    if args.check:
        for path in changed:
            sys.stdout.write(f"missing: {path}\n")
        return 1 if changed else 0

    sys.stdout.write(f"updated {len(changed)} Python file(s)\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
