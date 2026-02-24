"""Input parsing helpers for keyword clusterizer."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


def parse_clusterizer_frequency(value: Any) -> Optional[float]:
    raw = ("" if value is None else str(value)).strip().replace(",", ".")
    if not raw:
        return None
    try:
        parsed = float(raw)
    except Exception:
        return None
    if parsed < 0:
        return None
    return min(parsed, 10**9)


def split_clusterizer_text_rows(raw_text: str) -> List[str]:
    text = str(raw_text or "").replace("\r", "\n")
    lines = text.split("\n")
    rows: List[str] = []
    buffer: List[str] = []
    in_quote_block = False

    for line in lines:
        current = str(line or "")
        stripped = current.strip()
        if not stripped and not in_quote_block:
            continue

        if not in_quote_block:
            starts_quote = stripped.startswith('"') or stripped.startswith("'")
            ends_quote = stripped.endswith('"') or stripped.endswith("'")
            if starts_quote and not ends_quote:
                in_quote_block = True
                buffer = [current]
                continue
            rows.append(current)
            continue

        buffer.append(current)
        if stripped.endswith('"') or stripped.endswith("'"):
            rows.append("\n".join(buffer))
            buffer = []
            in_quote_block = False

    if buffer:
        rows.append("\n".join(buffer))
    return rows


def parse_clusterizer_keyword_line(raw_line: str) -> List[Dict[str, Any]]:
    line = str(raw_line or "").strip()
    if not line:
        return []
    normalized_line = line.strip("\"'").replace("\\t", "\t")

    line_unquoted = normalized_line
    if re.fullmatch(r"[+-]?\d+(?:[.,][0-9]+)?", line_unquoted):
        return []

    if "\n" in line:
        block = line
        if (block.startswith('"') and block.endswith('"')) or (block.startswith("'") and block.endswith("'")):
            block = block[1:-1]
        parts = [part.strip().strip("\"'") for part in block.replace("\r", "\n").split("\n") if part.strip()]
        if len(parts) >= 2:
            freq = parse_clusterizer_frequency(parts[-1])
            keyword = " ".join(parts[:-1]).strip()
            if keyword and freq is not None:
                return [{"keyword": keyword, "frequency": freq}]
        compact_keyword = " ".join(parts).strip() if parts else ""
        if compact_keyword:
            return [{"keyword": compact_keyword, "frequency": 1.0}]
        return []

    m = re.match(r"^(.*?)(?:\t+|[;>|:])\s*([0-9]+(?:[.,][0-9]+)?)\s*$", normalized_line)
    if m:
        keyword = str(m.group(1) or "").strip().strip("\"'")
        freq = parse_clusterizer_frequency(m.group(2))
        if keyword and freq is not None:
            return [{"keyword": keyword, "frequency": freq}]

    if "," in normalized_line:
        left, right = normalized_line.rsplit(",", 1)
        freq = parse_clusterizer_frequency(right)
        if freq is not None and str(left or "").strip():
            return [{"keyword": str(left).strip().strip("\"'"), "frequency": freq}]

    if ("\t" not in normalized_line) and not re.search(r"[;>|:]\s*[0-9]+(?:[.,][0-9]+)?\s*$", normalized_line):
        parts = [chunk.strip() for chunk in re.split(r"[;,]+", normalized_line) if chunk.strip()]
        if len(parts) > 1:
            return [{"keyword": part, "frequency": 1.0} for part in parts]

    return [{"keyword": normalized_line, "frequency": 1.0}]


def collect_clusterizer_keyword_rows(keywords: Optional[List[str]], keywords_text: Optional[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for item in keywords or []:
        rows.extend(parse_clusterizer_keyword_line(str(item or "")))

    raw_text = str(keywords_text or "")
    if raw_text.strip():
        for line in split_clusterizer_text_rows(raw_text):
            rows.extend(parse_clusterizer_keyword_line(line))

    dedup: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        keyword = str(row.get("keyword") or "").strip()
        if not keyword:
            continue
        parsed_freq = parse_clusterizer_frequency(row.get("frequency", 1.0))
        freq = 1.0 if parsed_freq is None else float(parsed_freq)
        key = keyword.lower()
        if key not in dedup:
            dedup[key] = {"keyword": keyword, "frequency": 0.0}
        dedup[key]["frequency"] = float(dedup[key]["frequency"]) + float(freq)

    return list(dedup.values())
