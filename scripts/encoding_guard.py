"""Detect and optionally fix common Cyrillic mojibake (UTF-8 read as cp1251)."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, Tuple


# Most common leading characters in mojibake sequences:
# - "Р/С..." for utf-8 text decoded as cp1251
# - "Ð/Ñ..." for utf-8 text decoded as latin1/western encodings
MOJIBAKE_LEAD_CHARS = "\u0420\u0421\u00D0\u00D1\u0401\u0403\u0404\u0407\u0406\u0409\u040a\u040b\u040c\u040e\u040f"
MOJIBAKE_RARE_CHARS = "\u0403\u0404\u0407\u0406\u0409\u040a\u040b\u040c\u040e\u040f\u0452\u0453\u0454\u0456\u0457\u0458\u0459\u045a\u045b\u045c\u045e\u045f\u0451"
MOJIBAKE_MARKER_CHARS = set("РСÐÑÃâ")
# Include C1 control range and no-break space - these often appear in broken cp1251/utf-8 chains.
CONTROL_RE = re.compile(r"[\u0080-\u009F]")
# Capture broad mojibake segments, but do not treat NBSP as whitespace.
TOKEN_RE = re.compile(rf"[{MOJIBAKE_LEAD_CHARS}][^\t\r\n\f\v]{{1,}}")
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")


def iter_files(root: Path, exts: Tuple[str, ...]) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() in exts:
            yield path


def maybe_fix_token(token: str) -> str:
    # Avoid touching normal text; focus on common mojibake signatures.
    if not (any(ch in token for ch in MOJIBAKE_LEAD_CHARS) or any(ch in token for ch in MOJIBAKE_RARE_CHARS)):
        return token

    def quality(value: str) -> tuple[int, int, int]:
        # Lower marker count is better; higher readable Cyrillic count is better.
        marker_count = sum(1 for ch in value if ch in MOJIBAKE_MARKER_CHARS)
        cyr_count = len(CYRILLIC_RE.findall(value))
        control_count = len(CONTROL_RE.findall(value))
        return marker_count + (control_count * 2), -cyr_count, control_count

    def cp1251_byte_for_char(ch: str) -> int | None:
        code = ord(ch)
        if code <= 0xFF:
            return code
        if ch == "Ё":
            return 0xA8
        if ch == "ё":
            return 0xB8
        if "А" <= ch <= "я":
            return code - 0x350
        try:
            raw = ch.encode("cp1251", errors="strict")
            if len(raw) == 1:
                return raw[0]
        except Exception:
            return None
        return None

    def decode_mixed_cp1251_utf8(value: str) -> str | None:
        raw = bytearray()
        for ch in value:
            b = cp1251_byte_for_char(ch)
            if b is None:
                return None
            raw.append(b)
        try:
            return raw.decode("utf-8", errors="strict")
        except Exception:
            return None

    variants = [token]
    mixed = decode_mixed_cp1251_utf8(token)
    if mixed and mixed != token:
        variants.append(mixed)
    for src_enc in ("cp1251", "latin1"):
        current = token
        for _ in range(2):
            try:
                candidate = current.encode(src_enc).decode("utf-8")
            except Exception:
                break
            if candidate == current:
                break
            variants.append(candidate)
            current = candidate

    best = min(variants, key=quality)
    if best == token:
        return token

    src_markers, src_neg_cyr, _src_ctrl = quality(token)
    dst_markers, dst_neg_cyr, _dst_ctrl = quality(best)
    src_cyr = -src_neg_cyr
    dst_cyr = -dst_neg_cyr
    if dst_markers < src_markers and dst_cyr >= max(1, src_cyr // 3):
        return best
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
