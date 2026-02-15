"""Build gap report between legacy seopro analyze_page keys and platform schema."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Set


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
    # Known return block bounds in legacy file; robust fallback via markers.
    start = next((i for i, line in enumerate(lines) if "return {" in line and "analyze_page" in "\n".join(lines[max(0, i - 120):i + 1])), None)
    if start is None:
        start = 2113
    end = next((i for i in range(start + 1, min(len(lines), start + 300)) if lines[i].startswith("    def _calculate_internal_pagerank")), start + 120)
    block = "\n".join(lines[start:end])
    return re.findall(r"^\s*'([a-zA-Z0-9_]+)'\s*:", block, flags=re.M)


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Site Pro gap report")
    parser.add_argument("--strict", action="store_true", help="Exit with non-zero code when missing keys exist")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    seopro_path = next((root / "Py scripts").rglob("seopro.py"))
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
