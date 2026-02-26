"""Input parsing helpers for keyword clusterizer."""

from __future__ import annotations

import csv
import io
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


# ── File-based input (XLSX / CSV) ────────────────────────────────────────────

# Column header aliases used by popular keyword tools
_KW_HEADER_ALIASES = {
    "keyword", "keywords", "query", "queries", "search term", "search terms",
    "phrase", "phrases", "ключевое слово", "ключевые слова", "ключ", "фраза",
}
_FREQ_HEADER_ALIASES = {
    "volume", "search volume", "frequency", "freq", "показов", "частота",
    "impressions", "avg. monthly searches", "avg monthly searches", "msv",
}


def _detect_column_mapping(headers: List[str]) -> Dict[str, int]:
    """Return {"keyword": col_idx, "frequency": col_idx} based on header names.
    Falls back to first text col / first numeric col if headers are absent/unrecognised.
    """
    kw_idx: Optional[int] = None
    freq_idx: Optional[int] = None
    for i, raw in enumerate(headers):
        h = str(raw or "").strip().lower()
        if kw_idx is None and h in _KW_HEADER_ALIASES:
            kw_idx = i
        if freq_idx is None and h in _FREQ_HEADER_ALIASES:
            freq_idx = i
    # Fallback: keyword = first col, frequency = second col (if exists)
    if kw_idx is None:
        kw_idx = 0
    if freq_idx is None and len(headers) >= 2:
        freq_idx = 1
    return {"keyword": kw_idx, "frequency": freq_idx}


def _rows_to_keyword_dicts(
    headers: List[str],
    data_rows: List[List[Any]],
) -> List[Dict[str, Any]]:
    """Convert raw rows to List[{keyword, frequency}] using auto column mapping."""
    col_map = _detect_column_mapping(headers)
    kw_col = col_map["keyword"]
    freq_col = col_map.get("frequency")

    dedup: Dict[str, Dict[str, Any]] = {}
    for row in data_rows:
        if kw_col >= len(row):
            continue
        keyword = str(row[kw_col] or "").strip()
        if not keyword:
            continue
        freq_raw = row[freq_col] if (freq_col is not None and freq_col < len(row)) else 1.0
        freq = parse_clusterizer_frequency(freq_raw)
        if freq is None:
            freq = 1.0
        key = keyword.lower()
        if key not in dedup:
            dedup[key] = {"keyword": keyword, "frequency": 0.0}
        dedup[key]["frequency"] = float(dedup[key]["frequency"]) + float(freq)
    return list(dedup.values())


def _parse_csv_keywords(file_bytes: bytes) -> List[Dict[str, Any]]:
    """Parse CSV bytes into keyword rows with auto-detected delimiter and columns."""
    for encoding in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
        try:
            text = file_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = file_bytes.decode("utf-8", errors="replace")

    # Auto-detect delimiter
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel  # type: ignore[assignment]

    reader = csv.reader(io.StringIO(text), dialect)
    all_rows: List[List[str]] = [row for row in reader if any(cell.strip() for cell in row)]
    if not all_rows:
        return []

    # First row as headers
    headers = all_rows[0]
    data_rows: List[List[Any]] = all_rows[1:] if len(all_rows) > 1 else []

    # Heuristic: if first row looks like pure data (first cell is numeric), treat as headerless
    first_cell = str(headers[0] or "").strip()
    if re.fullmatch(r"[+-]?\d+(?:[.,]\d+)?", first_cell):
        data_rows = all_rows
        headers = [str(i) for i in range(len(all_rows[0]))]

    return _rows_to_keyword_dicts(headers, data_rows)


def _parse_xlsx_keywords(file_bytes: bytes) -> List[Dict[str, Any]]:
    """Parse XLSX bytes into keyword rows using openpyxl."""
    try:
        import openpyxl
    except ImportError as exc:
        raise RuntimeError("openpyxl is required for XLSX parsing") from exc

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb.active
    all_rows: List[List[Any]] = []
    for row in ws.iter_rows(values_only=True):
        row_list = list(row)
        if any(cell is not None for cell in row_list):
            all_rows.append(row_list)
    wb.close()

    if not all_rows:
        return []

    headers = [str(cell or "").strip() for cell in all_rows[0]]
    data_rows = all_rows[1:] if len(all_rows) > 1 else []

    # Headerless detection: first cell of first row is numeric
    first_cell = str(all_rows[0][0] or "").strip()
    if re.fullmatch(r"[+-]?\d+(?:[.,]\d+)?", first_cell):
        data_rows = all_rows
        headers = [str(i) for i in range(len(all_rows[0]))]

    return _rows_to_keyword_dicts(headers, data_rows)


def parse_clusterizer_file(file_bytes: bytes, filename: str) -> List[Dict[str, Any]]:
    """Auto-detect format (CSV / XLSX) and return List[{keyword, frequency}].

    Supports exports from: Semrush, Ahrefs, Google Ads, KeyCollector, plain CSV/XLSX.
    Column mapping is auto-detected from headers; falls back to col 0 = keyword, col 1 = frequency.
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext == "xlsx":
        return _parse_xlsx_keywords(file_bytes)
    return _parse_csv_keywords(file_bytes)
