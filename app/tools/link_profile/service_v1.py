import csv
import io
import re
from collections import Counter, defaultdict
from datetime import datetime
from statistics import mean
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from openpyxl import load_workbook

ProgressCallback = Optional[Callable[[int, str], None]]


def _parse_keywords(raw: str) -> List[str]:
    if not raw:
        return []
    parts = re.split(r"[,;\n\r\t]+", str(raw))
    return [p.strip().lower() for p in parts if p and p.strip()]


def _normalize_domain(raw: str) -> str:
    value = (raw or "").strip().lower()
    if not value:
        return ""
    if "//" not in value:
        value = f"https://{value}"
    parsed = urlparse(value)
    host = (parsed.netloc or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _extract_domain(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if "//" not in value:
        value = f"https://{value}"
    try:
        parsed = urlparse(value)
        host = (parsed.netloc or "").lower().strip()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def _is_our_target(target_domain: str, our_domain: str) -> bool:
    if not target_domain or not our_domain:
        return False
    return target_domain == our_domain or target_domain.endswith(f".{our_domain}")


def _decode_text(payload: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
        try:
            return payload.decode(enc)
        except Exception:
            continue
    return payload.decode("utf-8", errors="replace")


def _read_csv_rows(payload: bytes) -> List[Dict[str, Any]]:
    text = _decode_text(payload)
    stream = io.StringIO(text, newline="")
    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except Exception:
        dialect = csv.excel
    reader = csv.DictReader(stream, dialect=dialect)
    rows: List[Dict[str, Any]] = []
    for row in reader:
        rows.append(dict(row))
    return rows


def _read_xlsx_rows(payload: bytes) -> List[Dict[str, Any]]:
    wb = load_workbook(io.BytesIO(payload), read_only=True, data_only=True)
    ws = wb.active
    iterator = ws.iter_rows(values_only=True)
    header_row = next(iterator, None)
    if not header_row:
        return []
    headers = [str(h).strip() if h is not None else "" for h in header_row]
    rows: List[Dict[str, Any]] = []
    for values in iterator:
        item: Dict[str, Any] = {}
        for idx, key in enumerate(headers):
            if not key:
                continue
            item[key] = values[idx] if idx < len(values) else None
        rows.append(item)
    return rows


def _read_tabular_rows(filename: str, payload: bytes) -> List[Dict[str, Any]]:
    lower = filename.lower()
    if lower.endswith(".csv"):
        return _read_csv_rows(payload)
    if lower.endswith(".xlsx"):
        return _read_xlsx_rows(payload)
    raise ValueError(f"Unsupported file format: {filename}")


def _pick_value(row: Dict[str, Any], aliases: Sequence[str]) -> Any:
    norm = {str(k).strip().lower(): v for k, v in row.items()}
    for alias in aliases:
        if alias in norm:
            return norm.get(alias)
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    raw = str(value).strip().replace(",", ".")
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _to_follow_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None
    if raw in {"dofollow", "follow", "true", "1", "yes"}:
        return True
    if raw in {"nofollow", "no-follow", "false", "0", "no"}:
        return False
    return None


def _classify_anchor(anchor: str, keywords: Dict[str, List[str]]) -> str:
    text = (anchor or "").lower()
    if not text:
        return "empty"

    def _has_any(items: List[str]) -> bool:
        return any(k and k in text for k in items)

    if _has_any(keywords.get("spam", [])):
        return "spam"
    if _has_any(keywords.get("brand", [])):
        return "brand"
    if _has_any(keywords.get("commercial", [])):
        return "commercial"
    if _has_any(keywords.get("informational", [])):
        return "informational"
    return "other"


def _normalize_backlink_row(row: Dict[str, Any]) -> Dict[str, Any]:
    source = _pick_value(
        row,
        (
            "source_url",
            "referring_url",
            "source",
            "url_from",
            "from",
            "referrer",
            "referring page",
            "source page",
            "url",
        ),
    )
    target = _pick_value(
        row,
        (
            "target_url",
            "target",
            "url_to",
            "to",
            "destination",
            "target page",
            "url target",
        ),
    )
    anchor = _pick_value(
        row,
        ("anchor", "anchor_text", "text", "anchor text"),
    )
    follow = _pick_value(
        row,
        ("follow", "link_type", "type", "rel", "is_dofollow", "dofollow"),
    )
    domain_rating = _pick_value(
        row,
        ("dr", "domain rating", "domain_rating", "ahrefs_dr"),
    )
    traffic = _pick_value(
        row,
        ("traffic", "domain traffic", "organic_traffic", "ahrefs_traffic"),
    )

    source_url = str(source or "").strip()
    target_url = str(target or "").strip()
    return {
        "source_url": source_url,
        "source_domain": _extract_domain(source_url),
        "target_url": target_url,
        "target_domain": _extract_domain(target_url),
        "anchor": str(anchor or "").strip(),
        "follow": _to_follow_bool(follow),
        "dr": _to_float(domain_rating),
        "traffic": _to_float(traffic),
    }


def _normalize_batch_row(row: Dict[str, Any]) -> Tuple[str, Dict[str, Optional[float]]]:
    domain = _pick_value(row, ("domain", "referring_domain", "site", "host"))
    dr = _pick_value(row, ("dr", "domain rating", "domain_rating", "ahrefs_dr"))
    traffic = _pick_value(row, ("traffic", "domain traffic", "organic_traffic", "ahrefs_traffic"))
    d = _normalize_domain(str(domain or ""))
    return d, {"dr": _to_float(dr), "traffic": _to_float(traffic)}


def run_link_profile_audit(
    *,
    our_domain: str,
    backlink_files: List[Tuple[str, bytes]],
    batch_file: Tuple[str, bytes],
    commercial_keywords: str = "",
    informational_keywords: str = "",
    spam_keywords: str = "",
    brand_keywords: str = "",
    progress_callback: ProgressCallback = None,
) -> Dict[str, Any]:
    domain = _normalize_domain(our_domain)
    if not domain:
        raise ValueError("Укажите корректный домен проекта")
    if not backlink_files:
        raise ValueError("Добавьте хотя бы один файл бэклинков")

    keywords = {
        "commercial": _parse_keywords(commercial_keywords),
        "informational": _parse_keywords(informational_keywords),
        "spam": _parse_keywords(spam_keywords),
        "brand": _parse_keywords(brand_keywords),
    }

    if progress_callback:
        progress_callback(15, "Чтение файла batch analysis")

    batch_name, batch_payload = batch_file
    batch_rows = _read_tabular_rows(batch_name, batch_payload)
    batch_metrics: Dict[str, Dict[str, Optional[float]]] = {}
    for row in batch_rows:
        d, metrics = _normalize_batch_row(row)
        if d:
            batch_metrics[d] = metrics

    if progress_callback:
        progress_callback(35, "Чтение файлов бэклинков")

    normalized_rows: List[Dict[str, Any]] = []
    file_summaries: List[Dict[str, Any]] = []
    for filename, payload in backlink_files:
        rows = _read_tabular_rows(filename, payload)
        valid_rows = 0
        for row in rows:
            normalized = _normalize_backlink_row(row)
            if not normalized["source_domain"]:
                continue
            if not normalized["target_domain"]:
                continue
            normalized_rows.append(normalized)
            valid_rows += 1
        file_summaries.append({"file": filename, "rows": len(rows), "valid_rows": valid_rows})

    if not normalized_rows:
        raise ValueError("Не удалось извлечь ссылки из входных файлов")

    if progress_callback:
        progress_callback(60, "Расчет метрик ссылочного профиля")

    source_to_targets: Dict[str, set[str]] = defaultdict(set)
    competitor_counter: Counter[str] = Counter()
    follow_counter: Counter[str] = Counter()
    anchor_type_counter: Counter[str] = Counter()
    anchor_counter: Counter[str] = Counter()

    dr_values: List[float] = []
    traffic_values: List[float] = []

    our_links = 0
    for row in normalized_rows:
        src = row["source_domain"]
        trg = row["target_domain"]
        source_to_targets[src].add(trg)

        if _is_our_target(trg, domain):
            our_links += 1
        else:
            competitor_counter[trg] += 1

        follow = row.get("follow")
        if follow is True:
            follow_counter["dofollow"] += 1
        elif follow is False:
            follow_counter["nofollow"] += 1
        else:
            follow_counter["unknown"] += 1

        anchor = row.get("anchor") or ""
        anchor_type = _classify_anchor(anchor, keywords)
        anchor_type_counter[anchor_type] += 1
        if anchor:
            anchor_counter[anchor] += 1

        if row.get("dr") is not None:
            dr_values.append(float(row["dr"]))
        if row.get("traffic") is not None:
            traffic_values.append(float(row["traffic"]))

    duplicates_with_our: List[Dict[str, Any]] = []
    single_our: List[Dict[str, Any]] = []
    single_competitor: List[Dict[str, Any]] = []

    for src, targets in source_to_targets.items():
        has_our = any(_is_our_target(t, domain) for t in targets)
        competitor_targets = [t for t in targets if not _is_our_target(t, domain)]
        if has_our and competitor_targets:
            duplicates_with_our.append(
                {
                    "domain": src,
                    "targets_count": len(targets),
                    "competitor_targets": ", ".join(sorted(competitor_targets)[:5]),
                }
            )
        elif has_our and not competitor_targets:
            single_our.append({"domain": src, "targets_count": len(targets)})
        elif (not has_our) and len(competitor_targets) == 1:
            single_competitor.append({"domain": src, "competitor": competitor_targets[0]})

    competitor_rows: List[Dict[str, Any]] = []
    for competitor, links_count in competitor_counter.most_common(20):
        metrics = batch_metrics.get(competitor, {})
        competitor_rows.append(
            {
                "competitor_domain": competitor,
                "links": links_count,
                "batch_dr": metrics.get("dr"),
                "batch_traffic": metrics.get("traffic"),
            }
        )

    top_anchors = [{"anchor": a, "count": c} for a, c in anchor_counter.most_common(20)]
    priority_domains = sorted(duplicates_with_our, key=lambda x: x.get("targets_count", 0), reverse=True)[:20]

    if progress_callback:
        progress_callback(85, "Формирование итогового отчета")

    warnings: List[str] = []
    if follow_counter.get("unknown", 0) > 0:
        warnings.append("Часть ссылок без явного признака dofollow/nofollow")
    if not keywords.get("brand"):
        warnings.append("Не задан словарь brand keywords")

    summary = {
        "our_domain": domain,
        "backlink_files": len(backlink_files),
        "batch_file": batch_name,
        "rows_total": len(normalized_rows),
        "our_links": our_links,
        "unique_ref_domains": len(source_to_targets),
        "unique_competitors": len(competitor_counter),
        "avg_dr": round(mean(dr_values), 2) if dr_values else None,
        "avg_traffic": round(mean(traffic_values), 2) if traffic_values else None,
        "dofollow": follow_counter.get("dofollow", 0),
        "nofollow": follow_counter.get("nofollow", 0),
        "unknown_follow": follow_counter.get("unknown", 0),
        "duplicates_with_our_site": len(duplicates_with_our),
    }

    result = {
        "task_type": "link_profile_audit",
        "url": domain,
        "completed_at": datetime.utcnow().isoformat(),
        "results": {
            "summary": summary,
            "warnings": warnings,
            "errors": [],
            "keywords": keywords,
            "source_files": file_summaries,
            "tables": {
                "competitor_analysis": competitor_rows,
                "anchor_analysis": top_anchors,
                "duplicates_with_our_site": duplicates_with_our[:200],
                "single_competitor_domains": single_competitor[:200],
                "single_our_domains": single_our[:200],
                "priority_domains": priority_domains,
            },
            "anchor_breakdown": dict(anchor_type_counter),
            "prompts": {
                "plan": (
                    "Приоритизируйте домены из блока priority_domains, "
                    "увеличьте долю брендовых анкоров и проверьте перекос по nofollow/dofollow."
                )
            },
        },
    }

    if progress_callback:
        progress_callback(100, "Аудит ссылочного профиля завершен")
    return result
