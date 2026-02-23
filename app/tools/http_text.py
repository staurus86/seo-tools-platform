"""Utilities for robust HTTP text decoding across mixed charset websites."""
from __future__ import annotations

import re
from typing import Any, Dict, List


_CHARSET_RE = re.compile(r"charset\s*=\s*['\"]?([a-zA-Z0-9._-]+)", re.I)


def _headers_of(response: Any) -> Dict[str, Any]:
    headers = getattr(response, "headers", None)
    return dict(headers or {})


def _header_charset(response: Any) -> str:
    headers = _headers_of(response)
    content_type = str(headers.get("Content-Type") or headers.get("content-type") or "")
    m = _CHARSET_RE.search(content_type)
    return (m.group(1).strip() if m else "")


def _unique_non_empty(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        key = (value or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def decode_response_text(response: Any) -> str:
    """Decode HTTP response bytes with stable charset fallbacks.

    Strategy:
    - Prefer UTF-8 first (avoids classic UTF-8 text decoded as cp1251 mojibake).
    - Then honor response-declared charset and apparent charset.
    - Fallback to common Cyrillic and generic encodings.
    """
    if response is None:
        return ""

    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content
    if not content:
        return str(getattr(response, "text", "") or "")

    candidates = _unique_non_empty(
        [
            "utf-8",
            str(getattr(response, "encoding", "") or ""),
            str(getattr(response, "apparent_encoding", "") or ""),
            _header_charset(response),
            "windows-1251",
            "cp1251",
            "latin1",
        ]
    )

    for enc in candidates:
        try:
            return bytes(content).decode(enc)
        except Exception:
            continue

    return bytes(content).decode("utf-8", errors="replace")

