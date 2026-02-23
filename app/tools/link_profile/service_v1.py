import csv
import io
import re
from collections import Counter, defaultdict
from datetime import datetime
from statistics import mean, median
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
    for enc in ("utf-16", "utf-16-le", "utf-8-sig", "utf-8", "cp1251", "latin-1"):
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
    try:
        reader = csv.DictReader(stream, dialect=dialect)
        rows: List[Dict[str, Any]] = []
        for row in reader:
            rows.append(dict(row))
        return rows
    except csv.Error:
        # Fallback for malformed CSV where fields contain raw newlines.
        lines = text.splitlines()
        if not lines:
            return []
        delim = "\t" if lines[0].count("\t") >= lines[0].count(",") else ","
        headers = [h.strip() for h in lines[0].split(delim)]
        rows: List[Dict[str, Any]] = []
        for line in lines[1:]:
            if not line.strip():
                continue
            parts = line.split(delim)
            if len(parts) < len(headers):
                parts = parts + [""] * (len(headers) - len(parts))
            elif len(parts) > len(headers):
                parts = parts[: len(headers) - 1] + [delim.join(parts[len(headers) - 1 :])]
            rows.append({headers[i]: parts[i] for i in range(len(headers))})
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


def _worksheet_to_rows(ws) -> List[Dict[str, Any]]:
    iterator = ws.iter_rows(values_only=True)
    header_row = next(iterator, None)
    if not header_row:
        return []
    headers = [str(h).strip() if h is not None else "" for h in header_row]
    rows: List[Dict[str, Any]] = []
    for values in iterator:
        if values is None:
            continue
        item: Dict[str, Any] = {}
        non_empty = False
        for idx, key in enumerate(headers):
            if not key:
                continue
            value = values[idx] if idx < len(values) else None
            if value not in (None, ""):
                non_empty = True
            item[key] = value
        if non_empty:
            rows.append(item)
    return rows


def _read_test_links_pack(payload: bytes) -> Dict[str, List[Dict[str, Any]]]:
    wb = load_workbook(io.BytesIO(payload), read_only=True, data_only=True)
    sheet_map = {str(n): wb[n] for n in wb.sheetnames}
    backlinks_rows: List[Dict[str, Any]] = []
    batch_rows: List[Dict[str, Any]] = []
    priority_rows: List[Dict[str, Any]] = []

    backlink_sheets = ["Ссылки с конкурентов", "Дубли без нашего сайта", "Ссылки с главных", "Ссылки с редиректов"]
    for s in backlink_sheets:
        ws = sheet_map.get(s)
        if not ws:
            continue
        for row in _worksheet_to_rows(ws):
            row["__sheet__"] = s
            backlinks_rows.append(row)

    for s in ("RU", "Все"):
        ws = sheet_map.get(s)
        if not ws:
            continue
        batch_rows.extend(_worksheet_to_rows(ws))

    ws_prio = sheet_map.get("Приоритетные доноры")
    if ws_prio:
        priority_rows.extend(_worksheet_to_rows(ws_prio))

    return {
        "backlinks_rows": backlinks_rows,
        "batch_rows": batch_rows,
        "priority_rows": priority_rows,
    }


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


def _derive_brand_keywords(domain: str) -> List[str]:
    host = (domain or "").lower().strip()
    if not host:
        return []
    host = host.split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    main = host.split(".")[0]
    chunks = re.split(r"[^a-z0-9а-яё]+", main, flags=re.IGNORECASE)
    out = [host, main]
    out.extend(chunks)
    seen = set()
    result: List[str] = []
    for item in out:
        token = (item or "").strip().lower()
        if len(token) < 2 or token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def _brand_token_from_domain(domain: str) -> str:
    host = _normalize_domain(domain)
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) >= 2:
        token = parts[-2]
    else:
        token = parts[0]
    token = re.sub(r"[^a-z0-9а-яё]+", "", token.lower(), flags=re.IGNORECASE)
    return token


def _extract_zone(domain: str) -> str:
    parts = (domain or "").split(".")
    if len(parts) < 2:
        return "other"
    zone = parts[-1].lower()
    return zone if 1 < len(zone) <= 10 else "other"


def _dr_bucket(dr: Optional[float]) -> str:
    if dr is None:
        return "unknown"
    if dr < 10:
        return "0-9"
    if dr < 30:
        return "10-29"
    if dr < 50:
        return "30-49"
    if dr < 70:
        return "50-69"
    return "70+"


def _anchor_words(anchor: str) -> List[str]:
    return [w.lower() for w in re.findall(r"[A-Za-zА-Яа-яЁё0-9]{3,}", anchor or "")]


def _has_redirect_301(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    return "301" in text


def _is_homepage_url(url: str) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        path = (parsed.path or "").strip()
        return path in ("", "/")
    except Exception:
        return False


def _flatten_breakdown_rows(rows: List[Dict[str, Any]], *, dim: str, value_key: str, row_limit: int = 500) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    grouped: Dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        comp = str(row.get("target_domain") or "")
        if not comp:
            continue
        value = str(row.get(value_key) or "").strip().lower()
        if not value:
            value = "unknown"
        grouped[comp][value] += 1
    for comp, cnt in grouped.items():
        total = sum(cnt.values()) or 1
        for v, c in cnt.items():
            out.append(
                {
                    "competitor_domain": comp,
                    dim: v,
                    "count": c,
                    "pct": round((c / total) * 100, 2),
                }
            )
    out.sort(key=lambda x: (x.get("competitor_domain", ""), -(x.get("count", 0))))
    return out[:row_limit]


def _normalize_backlink_row(row: Dict[str, Any]) -> Dict[str, Any]:
    source = _pick_value(
        row,
        (
            "source_url",
            "referring page url",
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
            "target url",
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
    nofollow = _pick_value(
        row,
        ("nofollow",),
    )
    redirect_status_codes = _pick_value(
        row,
        ("redirect chain status codes", "redirect_status_codes", "redirect status", "status codes"),
    )
    link_type = _pick_value(
        row,
        ("type", "link type"),
    )
    http_code = _pick_value(
        row,
        ("referring page http code", "http code", "status"),
    )
    language = _pick_value(
        row,
        ("language", "lang"),
    )
    ur_value = _pick_value(
        row,
        ("ur", "url rating"),
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
    follow_flag = _to_follow_bool(follow)
    nofollow_flag = _to_follow_bool(nofollow)
    if follow_flag is None and nofollow_flag is not None:
        follow_flag = not nofollow_flag
    return {
        "source_url": source_url,
        "source_domain": _extract_domain(source_url),
        "source_is_homepage": _is_homepage_url(source_url),
        "target_url": target_url,
        "target_domain": _extract_domain(target_url),
        "target_is_homepage": _is_homepage_url(target_url),
        "anchor": str(anchor or "").strip(),
        "follow": follow_flag,
        "has_redirect_301": _has_redirect_301(redirect_status_codes),
        "link_type": str(link_type or "").strip().lower(),
        "http_code": str(http_code or "").strip(),
        "language": str(language or "").strip().lower(),
        "ur": _to_float(ur_value),
        "dr": _to_float(domain_rating),
        "traffic": _to_float(traffic),
    }


def _normalize_batch_row(row: Dict[str, Any]) -> Tuple[str, Dict[str, Optional[float]]]:
    domain = _pick_value(row, ("domain", "target", "referring_domain", "site", "host"))
    dr = _pick_value(row, ("dr", "domain rating", "domain_rating", "ahrefs_dr"))
    traffic = _pick_value(row, ("traffic", "organic / traffic", "domain traffic", "organic_traffic", "ahrefs_traffic"))
    d = _normalize_domain(str(domain or ""))
    return d, {"dr": _to_float(dr), "traffic": _to_float(traffic)}


def run_link_profile_audit(
    *,
    our_domain: str,
    backlink_files: List[Tuple[str, bytes]],
    batch_file: Optional[Tuple[str, bytes]] = None,
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
    auto_brand_tokens: List[str] = _derive_brand_keywords(domain)
    for fname, _ in backlink_files:
        base = re.sub(r"-backlinks.*$", "", str(fname or ""), flags=re.IGNORECASE)
        auto_brand_tokens.extend(_derive_brand_keywords(base))
        token = _brand_token_from_domain(base)
        if token:
            auto_brand_tokens.append(token)

    if progress_callback:
        progress_callback(15, "Чтение файла batch analysis")

    batch_name = ""
    batch_rows: List[Dict[str, Any]] = []
    auto_batch_rows: List[Dict[str, Any]] = []
    precomputed_priority_rows: List[Dict[str, Any]] = []

    if batch_file:
        batch_name, batch_payload = batch_file
        batch_rows = _read_tabular_rows(batch_name, batch_payload)
    batch_metrics: Dict[str, Dict[str, Optional[float]]] = {}
    batch_targets: List[str] = []

    if progress_callback:
        progress_callback(35, "Чтение файлов бэклинков")

    normalized_rows: List[Dict[str, Any]] = []
    file_summaries: List[Dict[str, Any]] = []
    for filename, payload in backlink_files:
        rows: List[Dict[str, Any]] = []
        if str(filename or "").lower().endswith(".xlsx"):
            pack = _read_test_links_pack(payload)
            if pack.get("backlinks_rows"):
                rows = pack.get("backlinks_rows", [])
                auto_batch_rows.extend(pack.get("batch_rows", []))
                precomputed_priority_rows.extend(pack.get("priority_rows", []))
            else:
                rows = _read_tabular_rows(filename, payload)
        else:
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

    if (not batch_rows) and auto_batch_rows:
        batch_rows = auto_batch_rows
        batch_name = "from_test_links_pack"

    for row in batch_rows:
        d, metrics = _normalize_batch_row(row)
        if d:
            batch_metrics[d] = metrics
            batch_targets.append(d)

    for target_domain in batch_targets:
        auto_brand_tokens.extend(_derive_brand_keywords(target_domain))
        token = _brand_token_from_domain(target_domain)
        if token:
            auto_brand_tokens.append(token)
    derived_brand_keywords = list(dict.fromkeys([x for x in auto_brand_tokens if x and len(x) >= 2]))
    brand_keywords_used = list(dict.fromkeys((keywords.get("brand") or []) + derived_brand_keywords))
    keywords["brand"] = brand_keywords_used

    if not normalized_rows:
        raise ValueError("Не удалось извлечь ссылки из входных файлов")

    if progress_callback:
        progress_callback(60, "Расчет метрик ссылочного профиля")

    source_to_targets: Dict[str, set[str]] = defaultdict(set)
    source_to_competitors: Dict[str, set[str]] = defaultdict(set)
    competitor_counter: Counter[str] = Counter()
    competitor_ref_domains: Dict[str, set[str]] = defaultdict(set)
    our_source_counter: Counter[str] = Counter()
    follow_counter: Counter[str] = Counter()
    follow_our_counter: Counter[str] = Counter()
    follow_comp_counter: Counter[str] = Counter()
    anchor_type_counter: Counter[str] = Counter()
    anchor_counter: Counter[str] = Counter()
    anchor_word_counter: Counter[str] = Counter()
    dr_bucket_counter: Counter[str] = Counter()
    zone_counter: Counter[str] = Counter()
    redirect_301_counter: Counter[str] = Counter()
    homepage_donor_counter: Counter[str] = Counter()
    source_domain_stats: Dict[str, Dict[str, float]] = defaultdict(lambda: {"count": 0.0, "dr_sum": 0.0, "dr_n": 0.0, "ur_sum": 0.0, "ur_n": 0.0, "traffic_sum": 0.0, "traffic_n": 0.0, "nofollow_n": 0.0})
    target_agg: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"total": 0, "follow": 0, "nofollow": 0, "unknown": 0, "target_home": 0, "home_follow": 0, "home_nofollow": 0, "int_follow": 0, "int_nofollow": 0, "dr_counts": Counter(), "zone_counts": Counter()})

    dr_values: List[float] = []
    traffic_values: List[float] = []
    our_dr_values: List[float] = []
    our_traffic_values: List[float] = []
    comp_dr_values: List[float] = []
    comp_traffic_values: List[float] = []

    our_links = 0
    for row in normalized_rows:
        src = row["source_domain"]
        trg = row["target_domain"]
        source_to_targets[src].add(trg)
        zone_counter[_extract_zone(src)] += 1
        if row.get("has_redirect_301"):
            redirect_301_counter[src] += 1
        if row.get("source_is_homepage"):
            homepage_donor_counter[src] += 1
        sd = source_domain_stats[src]
        sd["count"] += 1
        if row.get("dr") is not None:
            sd["dr_sum"] += float(row.get("dr"))
            sd["dr_n"] += 1
        if row.get("ur") is not None:
            sd["ur_sum"] += float(row.get("ur"))
            sd["ur_n"] += 1
        if row.get("traffic") is not None:
            sd["traffic_sum"] += float(row.get("traffic"))
            sd["traffic_n"] += 1

        if _is_our_target(trg, domain):
            our_links += 1
            our_source_counter[src] += 1
        else:
            competitor_counter[trg] += 1
            competitor_ref_domains[trg].add(src)
            source_to_competitors[src].add(trg)

        follow = row.get("follow")
        if follow is True:
            follow_counter["dofollow"] += 1
            if _is_our_target(trg, domain):
                follow_our_counter["dofollow"] += 1
            else:
                follow_comp_counter["dofollow"] += 1
        elif follow is False:
            follow_counter["nofollow"] += 1
            sd["nofollow_n"] += 1
            if _is_our_target(trg, domain):
                follow_our_counter["nofollow"] += 1
            else:
                follow_comp_counter["nofollow"] += 1
        else:
            follow_counter["unknown"] += 1
            if _is_our_target(trg, domain):
                follow_our_counter["unknown"] += 1
            else:
                follow_comp_counter["unknown"] += 1

        anchor = row.get("anchor") or ""
        anchor_type = _classify_anchor(anchor, keywords)
        anchor_type_counter[anchor_type] += 1
        if anchor:
            anchor_counter[anchor] += 1
            for token in _anchor_words(anchor):
                anchor_word_counter[token] += 1

        row_dr = row.get("dr")
        row_traffic = row.get("traffic")
        source_metrics = batch_metrics.get(src, {})
        effective_dr = row_dr if row_dr is not None else source_metrics.get("dr")
        effective_traffic = row_traffic if row_traffic is not None else source_metrics.get("traffic")
        if effective_dr is not None:
            dr_values.append(float(effective_dr))
            dr_bucket_counter[_dr_bucket(float(effective_dr))] += 1
            if _is_our_target(trg, domain):
                our_dr_values.append(float(effective_dr))
            else:
                comp_dr_values.append(float(effective_dr))
        if effective_traffic is not None:
            traffic_values.append(float(effective_traffic))
            if _is_our_target(trg, domain):
                our_traffic_values.append(float(effective_traffic))
            else:
                comp_traffic_values.append(float(effective_traffic))
        ta = target_agg[trg]
        ta["total"] += 1
        is_home = bool(row.get("target_is_homepage"))
        if is_home:
            ta["target_home"] += 1
        if follow is True:
            ta["follow"] += 1
            if is_home:
                ta["home_follow"] += 1
            else:
                ta["int_follow"] += 1
        elif follow is False:
            ta["nofollow"] += 1
            if is_home:
                ta["home_nofollow"] += 1
            else:
                ta["int_nofollow"] += 1
        else:
            ta["unknown"] += 1
        if row.get("dr") is not None:
            ta["dr_counts"][_dr_bucket(row.get("dr"))] += 1
        ta["zone_counts"][_extract_zone(src)] += 1

    duplicates_with_our: List[Dict[str, Any]] = []
    duplicates_without_our: List[Dict[str, Any]] = []
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
                    "competitors_count": len(competitor_targets),
                    "competitor_targets": ", ".join(sorted(competitor_targets)[:5]),
                }
            )
        elif has_our and not competitor_targets:
            single_our.append({"domain": src, "targets_count": len(targets)})
        elif (not has_our) and len(competitor_targets) == 1:
            single_competitor.append({"domain": src, "competitor": competitor_targets[0]})
        elif (not has_our) and len(competitor_targets) >= 2:
            duplicates_without_our.append(
                {
                    "domain": src,
                    "competitors_count": len(competitor_targets),
                    "competitor_targets": ", ".join(sorted(competitor_targets)[:5]),
                }
            )

    per_target_metrics: Dict[str, Dict[str, Any]] = {}
    for target_domain, agg in target_agg.items():
        total = int(agg.get("total", 0))
        follow = int(agg.get("follow", 0))
        nofollow = int(agg.get("nofollow", 0))
        unknown = int(agg.get("unknown", 0))
        home = int(agg.get("target_home", 0))
        internal = total - home
        home_follow = int(agg.get("home_follow", 0))
        home_nofollow = int(agg.get("home_nofollow", 0))
        int_follow = int(agg.get("int_follow", 0))
        int_nofollow = int(agg.get("int_nofollow", 0))
        dr_counts = agg.get("dr_counts", Counter())
        zone_counts = agg.get("zone_counts", Counter())
        per_target_metrics[target_domain] = {
            "total_links": total,
            "homepage_pct": round((home / max(1, total)) * 100, 2),
            "internal_pct": round((internal / max(1, total)) * 100, 2),
            "follow_pct": round((follow / max(1, follow + nofollow + unknown)) * 100, 2),
            "nofollow_pct": round((nofollow / max(1, follow + nofollow + unknown)) * 100, 2),
            "homepage_follow_pct": round((home_follow / max(1, follow)) * 100, 2),
            "homepage_nofollow_pct": round((home_nofollow / max(1, nofollow)) * 100, 2),
            "internal_follow_pct": round((int_follow / max(1, follow)) * 100, 2),
            "internal_nofollow_pct": round((int_nofollow / max(1, nofollow)) * 100, 2),
            "dr_counts": dr_counts,
            "zone_counts": zone_counts,
        }

    competitors_list = [d for d in per_target_metrics.keys() if not _is_our_target(str(d), domain)]
    our_metrics = per_target_metrics.get(domain) or next((v for k, v in per_target_metrics.items() if _is_our_target(str(k), domain)), {})
    benchmark_rows: List[Dict[str, Any]] = []
    if competitors_list:
        keys = [
            "homepage_pct",
            "internal_pct",
            "follow_pct",
            "nofollow_pct",
            "homepage_follow_pct",
            "homepage_nofollow_pct",
            "internal_follow_pct",
            "internal_nofollow_pct",
        ]
        for key in keys:
            vals = [float(per_target_metrics[d].get(key, 0.0)) for d in competitors_list]
            avg_v = round(mean(vals), 2) if vals else 0.0
            med_v = round(median(vals), 2) if vals else 0.0
            our_v = round(float(our_metrics.get(key, 0.0)), 2) if our_metrics else 0.0
            benchmark_rows.append(
                {
                    "metric": key,
                    "our_site_pct": our_v,
                    "competitors_avg_pct": avg_v,
                    "competitors_median_pct": med_v,
                    "delta_vs_avg": round(our_v - avg_v, 2),
                    "delta_vs_median": round(our_v - med_v, 2),
                }
            )

    link_types_by_competitor = _flatten_breakdown_rows(normalized_rows, dim="link_type", value_key="link_type", row_limit=1000)
    http_codes_by_competitor = _flatten_breakdown_rows(normalized_rows, dim="http_code", value_key="http_code", row_limit=1000)
    languages_by_competitor = _flatten_breakdown_rows(normalized_rows, dim="language", value_key="language", row_limit=1000)

    our_ref_domains = set(our_source_counter.keys())
    our_site_rows: List[Dict[str, Any]] = []
    for src, links_count in our_source_counter.most_common(50):
        metrics = batch_metrics.get(src, {})
        our_site_rows.append(
            {
                "ref_domain": src,
                "links_to_our_site": links_count,
                "batch_dr": metrics.get("dr"),
                "batch_traffic": metrics.get("traffic"),
            }
        )

    competitor_rows: List[Dict[str, Any]] = []
    for competitor, links_count in competitor_counter.most_common(20):
        metrics = batch_metrics.get(competitor, {})
        comp_refs = competitor_ref_domains.get(competitor, set())
        shared = len(comp_refs & our_ref_domains)
        competitor_rows.append(
            {
                "competitor_domain": competitor,
                "links": links_count,
                "ref_domains": len(comp_refs),
                "shared_with_our_site": shared,
                "batch_dr": metrics.get("dr"),
                "batch_traffic": metrics.get("traffic"),
            }
        )

    comparison_rows: List[Dict[str, Any]] = []
    for row in competitor_rows:
        competitor = str(row.get("competitor_domain") or "")
        comp_refs = competitor_ref_domains.get(competitor, set())
        shared = len(comp_refs & our_ref_domains)
        comp_only = len(comp_refs - our_ref_domains)
        our_only = len(our_ref_domains - comp_refs)
        overlap_pct = round((shared / max(1, len(comp_refs))) * 100, 2)
        comparison_rows.append(
            {
                "competitor_domain": competitor,
                "shared_ref_domains": shared,
                "competitor_only_domains": comp_only,
                "our_only_domains": our_only,
                "overlap_pct": overlap_pct,
            }
        )

    top_anchors = [{"anchor": a, "count": c} for a, c in anchor_counter.most_common(30)]
    anchor_word_rows = [{"word": w, "count": c} for w, c in anchor_word_counter.most_common(30)]
    priority_domains = sorted(
        duplicates_with_our,
        key=lambda x: (x.get("competitors_count", 0), x.get("targets_count", 0)),
        reverse=True,
    )[:30]
    candidate_domains = set([x.get("domain") for x in duplicates_without_our if x.get("domain")] + [x.get("domain") for x in single_competitor if x.get("domain")])
    priority_score_rows: List[Dict[str, Any]] = []
    for d in candidate_domains:
        stats = source_domain_stats.get(str(d), {})
        n = max(1.0, float(stats.get("count", 0.0)))
        avg_dr = float(stats.get("dr_sum", 0.0)) / max(1.0, float(stats.get("dr_n", 0.0)))
        avg_ur = float(stats.get("ur_sum", 0.0)) / max(1.0, float(stats.get("ur_n", 0.0)))
        avg_traffic = float(stats.get("traffic_sum", 0.0)) / max(1.0, float(stats.get("traffic_n", 0.0)))
        nofollow_rate = float(stats.get("nofollow_n", 0.0)) / n
        if avg_dr <= 5:
            continue
        priority_score = (avg_dr * 0.5) + (avg_ur * 0.3) + ((avg_traffic or 0.0) * 0.2 / 1000.0)
        priority_score_rows.append(
            {
                "domain": d,
                "avg_dr": round(avg_dr, 2),
                "avg_ur": round(avg_ur, 2),
                "avg_traffic": round(avg_traffic, 2),
                "nofollow_rate": round(nofollow_rate, 4),
                "priority_score": round(priority_score, 4),
            }
        )
    strict_rows = [r for r in priority_score_rows if r["avg_dr"] > 40 and r["nofollow_rate"] < 0.5]
    if not strict_rows:
        strict_rows = [r for r in priority_score_rows if r["avg_dr"] > 30 and r["nofollow_rate"] < 0.7]
    priority_score_domains = sorted(strict_rows, key=lambda x: x["priority_score"], reverse=True)[:300]
    if precomputed_priority_rows:
        imported: List[Dict[str, Any]] = []
        for row in precomputed_priority_rows:
            d = _pick_value(row, ("domain",))
            dr = _to_float(_pick_value(row, ("domain rating", "dr")))
            ur = _to_float(_pick_value(row, ("ur", "url rating")))
            tr = _to_float(_pick_value(row, ("domain traffic", "organic / traffic", "traffic")))
            nf = _to_follow_bool(_pick_value(row, ("nofollow",)))
            ps = _to_float(_pick_value(row, ("priority score",)))
            domain_key = _normalize_domain(str(d or ""))
            if not domain_key:
                continue
            imported.append(
                {
                    "domain": domain_key,
                    "avg_dr": dr,
                    "avg_ur": ur,
                    "avg_traffic": tr,
                    "nofollow_rate": 1.0 if nf is True else 0.0 if nf is False else None,
                    "priority_score": ps,
                }
            )
        if imported:
            idx = {str(x.get("domain")): x for x in priority_score_domains}
            for row in imported:
                idx[str(row.get("domain"))] = row
            priority_score_domains = sorted(
                idx.values(),
                key=lambda x: float(x.get("priority_score") or 0.0),
                reverse=True,
            )[:500]
    dr_bucket_rows = [{"dr_bucket": k, "links": v} for k, v in sorted(dr_bucket_counter.items(), key=lambda x: str(x[0]))]
    dr_bucket_our_rows = []
    dr_bucket_comp_rows = []
    for bucket in ["0-9", "10-29", "30-49", "50-69", "70+", "unknown"]:
        dr_bucket_our_rows.append({"dr_bucket": bucket, "links": sum(1 for x in our_dr_values if _dr_bucket(x) == bucket) if bucket != "unknown" else 0})
        dr_bucket_comp_rows.append({"dr_bucket": bucket, "links": sum(1 for x in comp_dr_values if _dr_bucket(x) == bucket) if bucket != "unknown" else 0})
    zone_rows = [{"zone": z, "links": c} for z, c in zone_counter.most_common(20)]
    redirect_301_rows = [{"domain": d, "links_with_301": c} for d, c in redirect_301_counter.most_common(200)]
    homepage_donor_rows = [{"domain": d, "links_from_homepage": c} for d, c in homepage_donor_counter.most_common(200)]
    follow_rows = [
        {"type": "dofollow", "count": follow_counter.get("dofollow", 0)},
        {"type": "nofollow", "count": follow_counter.get("nofollow", 0)},
        {"type": "unknown", "count": follow_counter.get("unknown", 0)},
    ]
    follow_detail_rows = [
        {
            "segment": "our_site",
            "dofollow": follow_our_counter.get("dofollow", 0),
            "nofollow": follow_our_counter.get("nofollow", 0),
            "unknown": follow_our_counter.get("unknown", 0),
            "nofollow_share_pct": round(
                (follow_our_counter.get("nofollow", 0) / max(1, sum(follow_our_counter.values()))) * 100,
                2,
            ),
        },
        {
            "segment": "competitors",
            "dofollow": follow_comp_counter.get("dofollow", 0),
            "nofollow": follow_comp_counter.get("nofollow", 0),
            "unknown": follow_comp_counter.get("unknown", 0),
            "nofollow_share_pct": round(
                (follow_comp_counter.get("nofollow", 0) / max(1, sum(follow_comp_counter.values()))) * 100,
                2,
            ),
        },
        {
            "segment": "all",
            "dofollow": follow_counter.get("dofollow", 0),
            "nofollow": follow_counter.get("nofollow", 0),
            "unknown": follow_counter.get("unknown", 0),
            "nofollow_share_pct": round(
                (follow_counter.get("nofollow", 0) / max(1, sum(follow_counter.values()))) * 100,
                2,
            ),
        },
    ]
    dr_stats_rows = [
        {
            "segment": "our_site",
            "links_with_dr": len(our_dr_values),
            "avg_dr": round(mean(our_dr_values), 2) if our_dr_values else None,
            "median_dr": round(median(our_dr_values), 2) if our_dr_values else None,
            "min_dr": min(our_dr_values) if our_dr_values else None,
            "max_dr": max(our_dr_values) if our_dr_values else None,
        },
        {
            "segment": "competitors",
            "links_with_dr": len(comp_dr_values),
            "avg_dr": round(mean(comp_dr_values), 2) if comp_dr_values else None,
            "median_dr": round(median(comp_dr_values), 2) if comp_dr_values else None,
            "min_dr": min(comp_dr_values) if comp_dr_values else None,
            "max_dr": max(comp_dr_values) if comp_dr_values else None,
        },
        {
            "segment": "all",
            "links_with_dr": len(dr_values),
            "avg_dr": round(mean(dr_values), 2) if dr_values else None,
            "median_dr": round(median(dr_values), 2) if dr_values else None,
            "min_dr": min(dr_values) if dr_values else None,
            "max_dr": max(dr_values) if dr_values else None,
        },
    ]
    brand_rows = [{"keyword": k} for k in brand_keywords_used[:100]]

    if progress_callback:
        progress_callback(85, "Формирование итогового отчета")

    warnings: List[str] = []
    if follow_counter.get("unknown", 0) > 0:
        warnings.append("Часть ссылок без явного признака dofollow/nofollow")
    if not _parse_keywords(brand_keywords):
        warnings.append("Словарь brand keywords не передан, использованы автоматически выведенные брендовые токены")

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
        "duplicates_without_our_site": len(duplicates_without_our),
        "priority_domains_scored": len(priority_score_domains),
        "our_unique_ref_domains": len(our_ref_domains),
        "donors_with_301": len(redirect_301_counter),
        "donors_homepage": len(homepage_donor_counter),
        "avg_our_dr": round(mean(our_dr_values), 2) if our_dr_values else None,
        "avg_our_traffic": round(mean(our_traffic_values), 2) if our_traffic_values else None,
    }

    prompts = {
        "ourSite": (
            f"У сайта {domain} {summary['our_unique_ref_domains']} уникальных доноров. "
            "Сфокусируйтесь на доменах из priority_domains и увеличьте долю dofollow-ссылок."
        ),
        "competitors": (
            f"Обнаружено {summary['unique_competitors']} конкурентных доменов в ссылочном профиле. "
            "Приоритет: конкуренты с высоким shared_ref_domains и высоким batch_dr."
        ),
        "comparison": (
            "Используйте таблицу comparison для поиска donor-gap: competitor_only_domains "
            "показывает зоны быстрого расширения профиля."
        ),
        "plan": (
            "План на 90 дней: 1) закрыть 20 приоритетных доменов, 2) выровнять anchor mix (brand/commercial), "
            "3) снизить долю unknown follow, 4) ежемесячно пересчитывать overlap с конкурентами."
        ),
    }

    result = {
        "task_type": "link_profile_audit",
        "url": domain,
        "completed_at": datetime.utcnow().isoformat(),
        "results": {
            "summary": summary,
            "warnings": warnings,
            "errors": [],
            "keywords": {
                **keywords,
                "derivedBrandKeywords": derived_brand_keywords,
                "brandKeywordsUsed": brand_keywords_used,
            },
            "source_files": file_summaries,
            "outputs": {
                "competitorAnalysis": {"rows": competitor_rows},
                "anchorAnalysis": {"rows": top_anchors},
                "duplicates": {"rows": duplicates_with_our[:200]},
                "additionalMetrics": {"rows": dr_bucket_rows},
                "combinedOutput": {"rows": []},
            },
            "tables": {
                "competitor_analysis": competitor_rows,
                "anchor_analysis": top_anchors,
                "anchor_word_analysis": anchor_word_rows,
                "duplicates_with_our_site": duplicates_with_our[:200],
                "duplicates_with_two_competitors": duplicates_without_our[:200],
                "single_competitor_domains": single_competitor[:200],
                "single_our_domains": single_our[:200],
                "priority_domains": priority_domains,
                "priority_score_domains": priority_score_domains,
                "our_site_overview": our_site_rows,
                "comparison_overview": comparison_rows,
                "benchmark_overview": benchmark_rows,
                "dr_stats": dr_stats_rows,
                "dr_buckets": dr_bucket_rows,
                "dr_buckets_our_site": dr_bucket_our_rows,
                "dr_buckets_competitors": dr_bucket_comp_rows,
                "zones": zone_rows,
                "follow_types": follow_rows,
                "follow_types_detailed": follow_detail_rows,
                "donors_with_redirect_301": redirect_301_rows,
                "donors_homepage": homepage_donor_rows,
                "brand_keywords_auto": brand_rows,
                "source_files": file_summaries,
                "link_types_by_competitor": link_types_by_competitor,
                "http_codes_by_competitor": http_codes_by_competitor,
                "languages_by_competitor": languages_by_competitor,
                "ourSiteTables": [
                    {"title": "Наш сайт: доноры", "rows": our_site_rows},
                ],
                "competitorTables": [
                    {"title": "Конкуренты", "rows": competitor_rows},
                ],
                "comparisonTables": [
                    {"title": "Сравнение с конкурентами", "rows": comparison_rows},
                    {"title": "Бенчмарк avg/median по конкурентам", "rows": benchmark_rows},
                ],
                "additionalTables": [
                    {"title": "DR статистика", "rows": dr_stats_rows},
                    {"title": "DR buckets", "rows": dr_bucket_rows},
                    {"title": "DR buckets: наш сайт", "rows": dr_bucket_our_rows},
                    {"title": "DR buckets: конкуренты", "rows": dr_bucket_comp_rows},
                    {"title": "Domain zones", "rows": zone_rows},
                    {"title": "Follow / Nofollow", "rows": follow_rows},
                    {"title": "Follow / Nofollow detailed", "rows": follow_detail_rows},
                    {"title": "Donors with redirect 301", "rows": redirect_301_rows},
                    {"title": "Donors from homepage", "rows": homepage_donor_rows},
                    {"title": "Link types by competitor", "rows": link_types_by_competitor},
                    {"title": "HTTP codes by competitor", "rows": http_codes_by_competitor},
                    {"title": "Languages by competitor", "rows": languages_by_competitor},
                    {"title": "Auto brand keywords", "rows": brand_rows},
                    {"title": "Source files", "rows": file_summaries},
                ],
                "duplicatesTables": [
                    {"title": "Duplicates with our site", "rows": duplicates_with_our[:200]},
                    {"title": "Duplicates with two competitors", "rows": duplicates_without_our[:200]},
                    {"title": "Single competitor domains", "rows": single_competitor[:200]},
                    {"title": "Single our domains", "rows": single_our[:200]},
                    {"title": "Priority domains", "rows": priority_domains},
                    {"title": "Priority score domains", "rows": priority_score_domains},
                    {"title": "Competitors analysis", "rows": competitor_rows},
                ],
            },
            "anchor_breakdown": dict(anchor_type_counter),
            "prompts": prompts,
        },
    }

    if progress_callback:
        progress_callback(100, "Аудит ссылочного профиля завершен")
    return result
