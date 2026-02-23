"""Build gap report between legacy seopro analyze_page keys and platform schema."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Set


SYNONYMS: Dict[str, str] = {
    "status": "status_code",
    "description": "meta_description",
    "desc_len": "description_len",
    "words_count": "word_count",
    "dom_nodes": "dom_nodes_count",
    "page_authority": "pagerank",
    "incoming_links_count": "incoming_internal_links",
    "outgoing_links_internal": "outgoing_internal_links",
    "is_orphan": "orphan_page",
    "is_topic_hub": "topic_hub",
    "topic_cluster": "topic_label",
    "title_dup_count": "duplicate_title_count",
    "desc_dup_count": "duplicate_description_count",
    "follow_links": "external_follow_links",
    "nofollow_links": "external_nofollow_links",
    "content_bytes": "html_size_bytes",
    "hreflang": "hreflang_count",
    "schema": "schema_count",
    "canonical_url": "canonical",
    "site_health_score": "health_score",
    "int_links": "total_links",
    "https_ok": "is_https",
    "mobile_friendly": "mobile_friendly_hint",
    "linking_quality_score": "link_quality_score",
}


def extract_analyze_page_keys(seopro_path: Path) -> List[str]:
    text = seopro_path.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()
    start = _find_analyze_page_return_block_start(lines)
    end = min(len(lines), start + 500)
    # Stop at the next method definition on the same or lower indentation level.
    base_indent = _line_indent(lines[start]) if start < len(lines) else 0
    for i in range(start + 1, min(len(lines), start + 500)):
        line = lines[i]
        if re.match(r"^\s*def\s+[a-zA-Z_][a-zA-Z0-9_]*\s*\(", line):
            if _line_indent(line) <= base_indent:
                end = i
                break
    block = "\n".join(lines[start:end])
    return re.findall(r"^\s*'([a-zA-Z0-9_]+)'\s*:", block, flags=re.M)


def _line_indent(line: str) -> int:
    return len(line) - len(line.lstrip())


def _find_analyze_page_return_block_start(lines: List[str]) -> int:
    analyze_start: Optional[int] = next(
        (i for i, line in enumerate(lines) if re.match(r"^\s*def\s+analyze_page\s*\(", line)),
        None,
    )
    if analyze_start is not None:
        next_def = next(
            (
                i
                for i in range(analyze_start + 1, len(lines))
                if re.match(r"^\s*def\s+[a-zA-Z_][a-zA-Z0-9_]*\s*\(", lines[i])
                and _line_indent(lines[i]) <= _line_indent(lines[analyze_start])
            ),
            len(lines),
        )
        return_start = next(
            (i for i in range(analyze_start, next_def) if "return {" in lines[i]),
            None,
        )
        if return_start is not None:
            return return_start
    # Backward-compatible fallback for older snapshots.
    fallback = next(
        (
            i
            for i, line in enumerate(lines)
            if "return {" in line and "analyze_page" in "\n".join(lines[max(0, i - 120):i + 1])
        ),
        None,
    )
    if fallback is not None:
        return fallback
    raise ValueError("Could not locate analyze_page return block in legacy seopro file")


def extract_schema_fields(schema_path: Path) -> Set[str]:
    text = schema_path.read_text(encoding="utf-8", errors="ignore")
    m = re.search(
        r"class\s+NormalizedSiteAuditRow\(BaseModel\):\n([\s\S]*?)\n\nclass\s+NormalizedSiteAuditPayload",
        text,
    )
    if not m:
        return set()
    block = m.group(1)
    return set(re.findall(r"^\s{4}([a-zA-Z0-9_]+):", block, flags=re.M))


def resolve_legacy_seopro_path(root: Path, explicit_legacy_path: str = "") -> Path:
    if explicit_legacy_path:
        explicit = Path(explicit_legacy_path)
        if explicit.exists() and explicit.is_file():
            return explicit
        raise FileNotFoundError(f"Legacy file not found: {explicit}")

    direct_candidates = [
        root / "Py scripts" / "seopro.py",
        root / "seopro.py",
        root / "seopro-Old.py",
    ]
    for candidate in direct_candidates:
        if candidate.exists() and candidate.is_file():
            return candidate

    for search_dir in [root / "Py scripts", root / "scripts", root]:
        if not search_dir.exists():
            continue
        for pattern in ("seopro.py", "seopro-Old.py"):
            found = next(search_dir.rglob(pattern), None)
            if found and found.is_file():
                return found

    raise FileNotFoundError(
        "Legacy seopro file not found. Checked: 'Py scripts/seopro.py', 'seopro.py', and 'seopro-Old.py'."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Site Pro gap report")
    parser.add_argument("--strict", action="store_true", help="Exit with non-zero code when missing keys exist")
    parser.add_argument(
        "--legacy-path",
        default="",
        help="Optional explicit path to legacy seopro source file (e.g. seopro.py or seopro-Old.py)",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    try:
        seopro_path = resolve_legacy_seopro_path(root, explicit_legacy_path=args.legacy_path)
    except FileNotFoundError as exc:
        print(f"[error] {exc}")
        return 2
    schema_path = root / "app" / "tools" / "site_pro" / "schema.py"

    keys = extract_analyze_page_keys(seopro_path)
    schema_fields = extract_schema_fields(schema_path)

    covered: List[str] = []
    missing: List[str] = []
    for k in keys:
        mapped = SYNONYMS.get(k, k)
        if mapped in schema_fields:
            covered.append(k)
        else:
            missing.append(k)

    print(f"seopro_analyze_page_keys={len(keys)}")
    print(f"schema_fields={len(schema_fields)}")
    print(f"covered={len(covered)}")
    print(f"missing={len(missing)}")
    print("missing_keys:")
    for k in missing:
        print(f"- {k}")
    if args.strict and missing:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
