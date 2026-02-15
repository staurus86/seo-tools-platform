"""Detect and optionally fix common Cyrillic mojibake (UTF-8 read as cp1251)."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, Tuple


# Most common leading characters in cp1251->utf8 mojibake sequences.
MOJIBAKE_LEAD_CHARS = "\u0420\u0421\u0401\u0403\u0404\u0407\u0406\u0409\u040a\u040b\u040c\u040e\u040f"
MOJIBAKE_RARE_CHARS = "\u0403\u0404\u0407\u0406\u0409\u040a\u040b\u040c\u040e\u040f\u0452\u0453\u0454\u0456\u0457\u0458\u0459\u045a\u045b\u045c\u045e\u045f\u0451"
# Capture broad non-whitespace mojibake segments (including punctuation) to avoid misses.
TOKEN_RE = re.compile(rf"[{MOJIBAKE_LEAD_CHARS}][^\s]{{1,}}")
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")


def iter_files(root: Path, exts: Tuple[str, ...]) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() in exts:
            yield path


def maybe_fix_token(token: str) -> str:
    # Avoid touching normal Cyrillic text; focus on common mojibake signatures.
    if not (("Р" in token and "С" in token) or any(ch in token for ch in MOJIBAKE_RARE_CHARS)):
        return token
    try:
        fixed = token.encode("cp1251").decode("utf-8")
    except Exception:
        return token
    if fixed == token:
        return token
    src_score = len(CYRILLIC_RE.findall(token))
    dst_score = len(CYRILLIC_RE.findall(fixed))
    if dst_score >= src_score:
        return fixed
    return token


def process_text(text: str) -> Tuple[str, int]:
    changed = 0

    def repl(match: re.Match[str]) -> str:
        nonlocal changed
        token = match.group(0)
        fixed = maybe_fix_token(token)
        if fixed != token:
            changed += 1
        return fixed

    return TOKEN_RE.sub(repl, text), changed


def check_or_fix(root: Path, exts: Tuple[str, ...], write: bool) -> int:
    affected_files = 0
    total_changes = 0
    for path in iter_files(root, exts):
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            continue
        new_text, changes = process_text(text)
        if changes <= 0:
            continue
        affected_files += 1
        total_changes += changes
        print(f"{path}: {changes} potential mojibake token(s)")
        if write:
            path.write_text(new_text, encoding="utf-8", newline="")
    print(f"files={affected_files} changes={total_changes} mode={'fix' if write else 'check'}")
    return 1 if (affected_files > 0 and not write) else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Encoding guard for Cyrillic mojibake")
    parser.add_argument("mode", choices=["check", "fix"])
    parser.add_argument("--root", default="app", help="Root folder to scan")
    parser.add_argument(
        "--ext",
        nargs="*",
        default=[".py", ".html", ".js", ".md", ".txt"],
        help="File extensions to scan",
    )
    args = parser.parse_args()
    root = Path(args.root)
    exts = tuple(e.lower() for e in args.ext)
    return check_or_fix(root=root, exts=exts, write=(args.mode == "fix"))


if __name__ == "__main__":
    raise SystemExit(main())
