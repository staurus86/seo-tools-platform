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


_MULTI_PART_TLDS = {
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
    "co.jp",
    "ne.jp",
    "or.jp",
    "com.au",
    "net.au",
    "org.au",
    "co.nz",
    "com.br",
    "com.tr",
    "co.kr",
    "com.mx",
    "co.in",
}


def _to_registrable_domain(host: str) -> str:
    value = (host or "").strip().lower().rstrip(".")
    if not value:
        return ""
    if ":" in value:
        value = value.split(":", 1)[0]
    if value.startswith("www."):
        value = value[4:]
    if re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", value):
        return value
    parts = [p for p in value.split(".") if p]
    if len(parts) <= 2:
        return value
    tail2 = ".".join(parts[-2:])
    tail3 = ".".join(parts[-3:])
    if tail2 in _MULTI_PART_TLDS and len(parts) >= 3:
        return tail3
    if re.fullmatch(r"[a-z]{2}", parts[-1]) and parts[-2] in {"co", "com", "net", "org", "gov", "edu", "ac"} and len(parts) >= 3:
        return tail3
    return tail2


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
    return _to_registrable_domain(host)


def _extract_domain(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if "//" not in value:
        value = f"https://{value}"
    try:
        parsed = urlparse(value)
        host = (parsed.netloc or "").lower().strip()
        return _to_registrable_domain(host)
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


def _dr_bucket_decile(dr: Optional[float]) -> str:
    if dr is None:
        return "DR unknown"
    value = max(0.0, min(100.0, float(dr)))
    low = int(value // 10) * 10
    if low >= 90:
        return "DR 90-100"
    return f"DR {low}-{low + 9}"


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


def _pct(part: float, total: float) -> float:
    return round((float(part) / max(1.0, float(total))) * 100.0, 2)


def _counter_to_pct_rows(counter: Counter[str], *, value_key: str, limit: int = 50) -> List[Dict[str, Any]]:
    total = sum(counter.values()) or 1
    rows: List[Dict[str, Any]] = []
    for k, v in counter.most_common(limit):
        rows.append(
            {
                value_key: k,
                "count": int(v),
                "pct": _pct(v, total),
            }
        )
    return rows


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
    lost_status = _pick_value(
        row,
        ("lost status", "drop reason", "discovered status"),
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
        "lost_status": str(lost_status or "").strip().lower(),
        "ur": _to_float(ur_value),
        "dr": _to_float(domain_rating),
        "traffic": _to_float(traffic),
    }


def _normalize_batch_row(row: Dict[str, Any]) -> Tuple[str, Dict[str, Optional[float]]]:
    domain = _pick_value(row, ("domain", "target", "referring_domain", "site", "host"))
    dr = _pick_value(row, ("dr", "domain rating", "domain_rating", "ahrefs_dr"))
    traffic = _pick_value(row, ("traffic", "organic / traffic", "domain traffic", "organic_traffic", "ahrefs_traffic"))
    backlinks_all = _pick_value(row, ("backlinks / all", "backlinks_all", "total backlinks"))
    backlinks_followed = _pick_value(row, ("backlinks / followed", "backlinks_followed"))
    backlinks_not_followed = _pick_value(row, ("backlinks / not followed", "backlinks_not_followed"))
    backlinks_redirects = _pick_value(row, ("backlinks / redirects", "backlinks_redirects"))
    backlinks_internal = _pick_value(row, ("backlinks / internal", "backlinks_internal"))
    ref_domains_all = _pick_value(row, ("ref. domains / all", "ref domains all", "ref_domains_all"))
    ref_domains_followed = _pick_value(row, ("ref. domains / followed", "ref_domains_followed"))
    ref_domains_not_followed = _pick_value(row, ("ref. domains / not followed", "ref_domains_not_followed"))
    organic_keywords_total = _pick_value(row, ("organic / total keywords", "organic_keywords_total"))
    organic_keywords_top3 = _pick_value(row, ("organic / keywords (top 3)", "organic_keywords_top3"))
    organic_keywords_4_10 = _pick_value(row, ("organic / keywords (4-10)", "organic_keywords_4_10"))
    organic_keywords_11_20 = _pick_value(row, ("organic / keywords (11-20)", "organic_keywords_11_20"))
    paid_keywords = _pick_value(row, ("paid / keywords", "paid_keywords"))
    paid_traffic = _pick_value(row, ("paid / traffic", "paid_traffic"))
    outgoing_domains_followed = _pick_value(row, ("outgoing domains / followed", "outgoing_domains_followed"))
    outgoing_domains_all_time = _pick_value(row, ("outgoing domains / all time", "outgoing_domains_all_time"))
    outgoing_links_followed = _pick_value(row, ("outgoing links / followed", "outgoing_links_followed"))
    outgoing_links_all_time = _pick_value(row, ("outgoing links / all time", "outgoing_links_all_time"))
    d = _normalize_domain(str(domain or ""))
    return d, {
        "dr": _to_float(dr),
        "traffic": _to_float(traffic),
        "backlinks_all": _to_float(backlinks_all),
        "backlinks_followed": _to_float(backlinks_followed),
        "backlinks_not_followed": _to_float(backlinks_not_followed),
        "backlinks_redirects": _to_float(backlinks_redirects),
        "backlinks_internal": _to_float(backlinks_internal),
        "ref_domains_all": _to_float(ref_domains_all),
        "ref_domains_followed": _to_float(ref_domains_followed),
        "ref_domains_not_followed": _to_float(ref_domains_not_followed),
        "organic_keywords_total": _to_float(organic_keywords_total),
        "organic_keywords_top3": _to_float(organic_keywords_top3),
        "organic_keywords_4_10": _to_float(organic_keywords_4_10),
        "organic_keywords_11_20": _to_float(organic_keywords_11_20),
        "paid_keywords": _to_float(paid_keywords),
        "paid_traffic": _to_float(paid_traffic),
        "outgoing_domains_followed": _to_float(outgoing_domains_followed),
        "outgoing_domains_all_time": _to_float(outgoing_domains_all_time),
        "outgoing_links_followed": _to_float(outgoing_links_followed),
        "outgoing_links_all_time": _to_float(outgoing_links_all_time),
    }


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
    raw_homepage_links_rows: List[Dict[str, Any]] = []
    raw_redirect_links_rows: List[Dict[str, Any]] = []
    raw_duplicates_without_our_rows: List[Dict[str, Any]] = []
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
            sheet_name = str(row.get("__sheet__") or "").strip()
            if sheet_name == "Ссылки с главных":
                raw_homepage_links_rows.append(
                    {
                        "Referring page URL": _pick_value(row, ("referring page url",)) or "",
                        "Target URL": _pick_value(row, ("target url",)) or "",
                        "Anchor": _pick_value(row, ("anchor",)) or "",
                        "Domain Rating": _pick_value(row, ("domain rating", "dr")) or "",
                        "UR": _pick_value(row, ("ur", "url rating")) or "",
                        "Domain traffic": _pick_value(row, ("domain traffic", "traffic")) or "",
                        "Nofollow": _pick_value(row, ("nofollow",)) or "",
                        "Lost status": _pick_value(row, ("lost status",)) or "",
                    }
                )
            elif sheet_name == "Ссылки с редиректов":
                raw_redirect_links_rows.append(
                    {
                        "Referring page URL": _pick_value(row, ("referring page url",)) or "",
                        "Referring page HTTP code": _pick_value(row, ("referring page http code", "http code", "status")) or "",
                        "Domain rating": _pick_value(row, ("domain rating", "dr")) or "",
                        "UR": _pick_value(row, ("ur", "url rating")) or "",
                        "Domain traffic": _pick_value(row, ("domain traffic", "traffic")) or "",
                        "Target URL": _pick_value(row, ("target url",)) or "",
                    }
                )
            elif sheet_name == "Дубли без нашего сайта":
                raw_duplicates_without_our_rows.append(
                    {
                        "Referring page URL": _pick_value(row, ("referring page url",)) or "",
                        "Target URL": _pick_value(row, ("target url",)) or "",
                        "Anchor": _pick_value(row, ("anchor",)) or "",
                        "Domain Rating": _pick_value(row, ("domain rating", "dr")) or "",
                        "UR": _pick_value(row, ("ur", "url rating")) or "",
                        "Domain traffic": _pick_value(row, ("domain traffic", "traffic")) or "",
                        "Nofollow": _pick_value(row, ("nofollow",)) or "",
                        "Lost status": _pick_value(row, ("lost status",)) or "",
                    }
                )
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
    lost_status_counter: Counter[str] = Counter()
    http_class_counter: Counter[str] = Counter()
    language_counter: Counter[str] = Counter()
    link_type_counter: Counter[str] = Counter()
    source_domain_stats: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {
            "count": 0.0,
            "dr_sum": 0.0,
            "dr_n": 0.0,
            "ur_sum": 0.0,
            "ur_n": 0.0,
            "traffic_sum": 0.0,
            "traffic_n": 0.0,
            "nofollow_n": 0.0,
            "lost_n": 0.0,
            "http2xx_n": 0.0,
        }
    )
    target_agg: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "total": 0,
            "follow": 0,
            "nofollow": 0,
            "unknown": 0,
            "target_home": 0,
            "home_follow": 0,
            "home_nofollow": 0,
            "int_follow": 0,
            "int_nofollow": 0,
            "lost": 0,
            "dr_counts": Counter(),
            "dr10_counts": Counter(),
            "zone_counts": Counter(),
            "http_class": Counter(),
            "link_type": Counter(),
            "language": Counter(),
            "anchor_type": Counter(),
        }
    )
    source_follow_profile: Dict[str, Counter[str]] = defaultdict(Counter)

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
        sd = source_domain_stats[src]
        source_to_targets[src].add(trg)
        zone_counter[_extract_zone(src)] += 1
        if row.get("has_redirect_301"):
            redirect_301_counter[src] += 1
        if row.get("source_is_homepage"):
            homepage_donor_counter[src] += 1
        if row.get("lost_status"):
            lost_status_counter[row.get("lost_status")] += 1
            sd["lost_n"] += 1
        code = str(row.get("http_code") or "")
        if code.startswith("2"):
            http_class_counter["2xx"] += 1
            sd["http2xx_n"] += 1
        elif code.startswith("3"):
            http_class_counter["3xx"] += 1
        elif code.startswith("4"):
            http_class_counter["4xx"] += 1
        elif code.startswith("5"):
            http_class_counter["5xx"] += 1
        else:
            http_class_counter["unknown"] += 1
        link_type_counter[str(row.get("link_type") or "unknown")] += 1
        language_counter[str(row.get("language") or "unknown")] += 1
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
            source_follow_profile[src]["dofollow"] += 1
            if _is_our_target(trg, domain):
                follow_our_counter["dofollow"] += 1
            else:
                follow_comp_counter["dofollow"] += 1
        elif follow is False:
            follow_counter["nofollow"] += 1
            sd["nofollow_n"] += 1
            source_follow_profile[src]["nofollow"] += 1
            if _is_our_target(trg, domain):
                follow_our_counter["nofollow"] += 1
            else:
                follow_comp_counter["nofollow"] += 1
        else:
            follow_counter["unknown"] += 1
            source_follow_profile[src]["unknown"] += 1
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
        if effective_dr is not None:
            ta["dr10_counts"][_dr_bucket_decile(effective_dr)] += 1
        ta["zone_counts"][_extract_zone(src)] += 1
        if row.get("lost_status"):
            ta["lost"] += 1
        ta["http_class"]["2xx" if str(row.get("http_code") or "").startswith("2") else "3xx" if str(row.get("http_code") or "").startswith("3") else "4xx" if str(row.get("http_code") or "").startswith("4") else "5xx" if str(row.get("http_code") or "").startswith("5") else "unknown"] += 1
        ta["link_type"][str(row.get("link_type") or "unknown")] += 1
        ta["language"][str(row.get("language") or "unknown")] += 1
        ta["anchor_type"][anchor_type] += 1

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

    # Fallback: if source sheets are not present, build raw tables from normalized rows.
    if not raw_homepage_links_rows:
        for row in normalized_rows:
            if not row.get("source_is_homepage"):
                continue
            raw_homepage_links_rows.append(
                {
                    "Referring page URL": row.get("source_url") or "",
                    "Target URL": row.get("target_url") or "",
                    "Anchor": row.get("anchor") or "",
                    "Domain Rating": row.get("dr"),
                    "UR": row.get("ur"),
                    "Domain traffic": row.get("traffic"),
                    "Nofollow": "nofollow" if row.get("follow") is False else "dofollow" if row.get("follow") is True else "",
                    "Lost status": row.get("lost_status") or "",
                }
            )
    if not raw_redirect_links_rows:
        for row in normalized_rows:
            code = str(row.get("http_code") or "")
            if not row.get("has_redirect_301") and not code.startswith("3"):
                continue
            raw_redirect_links_rows.append(
                {
                    "Referring page URL": row.get("source_url") or "",
                    "Referring page HTTP code": row.get("http_code") or "",
                    "Domain rating": row.get("dr"),
                    "UR": row.get("ur"),
                    "Domain traffic": row.get("traffic"),
                    "Target URL": row.get("target_url") or "",
                }
            )
    if not raw_duplicates_without_our_rows:
        dup_wo_domains = {str(x.get("domain") or "") for x in duplicates_without_our}
        for row in normalized_rows:
            src = str(row.get("source_domain") or "")
            trg = str(row.get("target_domain") or "")
            if src not in dup_wo_domains:
                continue
            if _is_our_target(trg, domain):
                continue
            raw_duplicates_without_our_rows.append(
                {
                    "Referring page URL": row.get("source_url") or "",
                    "Target URL": row.get("target_url") or "",
                    "Anchor": row.get("anchor") or "",
                    "Domain Rating": row.get("dr"),
                    "UR": row.get("ur"),
                    "Domain traffic": row.get("traffic"),
                    "Nofollow": "nofollow" if row.get("follow") is False else "dofollow" if row.get("follow") is True else "",
                    "Lost status": row.get("lost_status") or "",
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
            "lost_pct": round((int(agg.get("lost", 0)) / max(1, total)) * 100, 2),
            "dr_counts": dr_counts,
            "dr10_counts": agg.get("dr10_counts", Counter()),
            "zone_counts": zone_counts,
            "http_class": agg.get("http_class", Counter()),
            "link_type": agg.get("link_type", Counter()),
            "language": agg.get("language", Counter()),
            "anchor_type": agg.get("anchor_type", Counter()),
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
            "lost_pct",
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

    link_types_by_competitor = _flatten_breakdown_rows(normalized_rows, dim="link_type", value_key="link_type", row_limit=3000)
    http_codes_by_competitor = _flatten_breakdown_rows(normalized_rows, dim="http_code", value_key="http_code", row_limit=3000)
    languages_by_competitor = _flatten_breakdown_rows(normalized_rows, dim="language", value_key="language", row_limit=3000)

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
    for competitor, links_count in competitor_counter.most_common(200):
        metrics = batch_metrics.get(competitor, {})
        comp_refs = competitor_ref_domains.get(competitor, set())
        shared = len(comp_refs & our_ref_domains)
        backlinks_all = float(metrics.get("backlinks_all") or 0.0)
        backlinks_followed = float(metrics.get("backlinks_followed") or 0.0)
        backlinks_not_followed = float(metrics.get("backlinks_not_followed") or 0.0)
        backlinks_redirects = float(metrics.get("backlinks_redirects") or 0.0)
        backlinks_internal = float(metrics.get("backlinks_internal") or 0.0)
        ref_domains_all = float(metrics.get("ref_domains_all") or 0.0)
        ref_domains_followed = float(metrics.get("ref_domains_followed") or 0.0)
        ref_domains_not_followed = float(metrics.get("ref_domains_not_followed") or 0.0)
        competitor_rows.append(
            {
                "competitor_domain": competitor,
                "links": links_count,
                "ref_domains": len(comp_refs),
                "shared_with_our_site": shared,
                "batch_dr": metrics.get("dr"),
                "batch_traffic": metrics.get("traffic"),
                "batch_backlinks_all": backlinks_all,
                "batch_ref_domains_all": ref_domains_all,
                "batch_backlinks_followed_pct": round((backlinks_followed / max(1.0, backlinks_all)) * 100, 2) if backlinks_all else None,
                "batch_backlinks_nofollow_pct": round((backlinks_not_followed / max(1.0, backlinks_all)) * 100, 2) if backlinks_all else None,
                "batch_backlinks_redirect_pct": round((backlinks_redirects / max(1.0, backlinks_all)) * 100, 2) if backlinks_all else None,
                "batch_backlinks_internal_pct": round((backlinks_internal / max(1.0, backlinks_all)) * 100, 2) if backlinks_all else None,
                "batch_ref_domains_followed_pct": round((ref_domains_followed / max(1.0, ref_domains_all)) * 100, 2) if ref_domains_all else None,
                "batch_ref_domains_nofollow_pct": round((ref_domains_not_followed / max(1.0, ref_domains_all)) * 100, 2) if ref_domains_all else None,
                "batch_organic_keywords_total": metrics.get("organic_keywords_total"),
            }
        )

    competitor_rank_rows: List[Dict[str, Any]] = []
    for row in competitor_rows:
        dr_v = float(row.get("batch_dr") or 0.0)
        backlinks_v = float(row.get("batch_backlinks_all") or 0.0)
        follow_v = float(row.get("batch_backlinks_followed_pct") or 0.0)
        score = round((dr_v * 0.45) + (min(backlinks_v, 200000.0) / 2000.0 * 0.3) + (follow_v * 0.25), 2)
        competitor_rank_rows.append(
            {
                "competitor_domain": row.get("competitor_domain"),
                "batch_dr": row.get("batch_dr"),
                "batch_backlinks_all": row.get("batch_backlinks_all"),
                "batch_backlinks_followed_pct": row.get("batch_backlinks_followed_pct"),
                "rank_score": score,
            }
        )
    competitor_rank_rows.sort(key=lambda x: float(x.get("rank_score") or 0.0), reverse=True)
    for idx, item in enumerate(competitor_rank_rows, start=1):
        item["rank"] = idx

    comparison_rows: List[Dict[str, Any]] = []
    our_batch_dr = _to_float((batch_metrics.get(domain) or {}).get("dr"))
    if our_batch_dr is None:
        for d_key, m in batch_metrics.items():
            if _is_our_target(str(d_key), domain):
                our_batch_dr = _to_float((m or {}).get("dr"))
                if our_batch_dr is not None:
                    break
    our_follow_pct = float(our_metrics.get("follow_pct", 0.0) or 0.0)
    our_lost_pct = float(our_metrics.get("lost_pct", 0.0) or 0.0)
    our_http2xx_pct = _pct((our_metrics.get("http_class", Counter()) or Counter()).get("2xx", 0), max(1, int((our_metrics.get("total_links", 0) or 0))))
    for row in competitor_rows:
        competitor = str(row.get("competitor_domain") or "")
        comp_refs = competitor_ref_domains.get(competitor, set())
        shared = len(comp_refs & our_ref_domains)
        comp_only = len(comp_refs - our_ref_domains)
        our_only = len(our_ref_domains - comp_refs)
        overlap_pct = round((shared / max(1, len(comp_refs))) * 100, 2)
        comp_metrics = per_target_metrics.get(competitor, {})
        comp_follow_pct = float(comp_metrics.get("follow_pct", 0.0) or 0.0)
        comp_lost_pct = float(comp_metrics.get("lost_pct", 0.0) or 0.0)
        comp_http2xx_pct = _pct((comp_metrics.get("http_class", Counter()) or Counter()).get("2xx", 0), max(1, int((comp_metrics.get("total_links", 0) or 0))))
        comp_batch_dr = _to_float((batch_metrics.get(competitor) or {}).get("dr"))
        dr_gap_pct = None
        if (our_batch_dr is not None) and (comp_batch_dr is not None):
            dr_gap_pct = _pct(comp_batch_dr - our_batch_dr, max(1.0, our_batch_dr))
        comparison_rows.append(
            {
                "competitor_domain": competitor,
                "shared_ref_domains": shared,
                "competitor_only_domains": comp_only,
                "our_only_domains": our_only,
                "overlap_pct": overlap_pct,
                "coverage_of_our_ref_domains_pct": round((shared / max(1, len(our_ref_domains))) * 100, 2),
                "donor_gap_pct": round((comp_only / max(1, len(comp_refs))) * 100, 2),
                "competitor_follow_pct": comp_follow_pct,
                "our_follow_pct": our_follow_pct,
                "follow_gap_pp": round(comp_follow_pct - our_follow_pct, 2),
                "competitor_lost_pct": comp_lost_pct,
                "our_lost_pct": our_lost_pct,
                "lost_gap_pp": round(comp_lost_pct - our_lost_pct, 2),
                "competitor_http_2xx_pct": comp_http2xx_pct,
                "our_http_2xx_pct": our_http2xx_pct,
                "http_2xx_gap_pp": round(comp_http2xx_pct - our_http2xx_pct, 2),
                "our_batch_dr": our_batch_dr,
                "competitor_batch_dr": comp_batch_dr,
                "dr_gap_pct_vs_our": dr_gap_pct,
            }
        )

    competitor_quality_rows: List[Dict[str, Any]] = []
    for row in competitor_rows:
        comp = str(row.get("competitor_domain") or "")
        comp_metrics = per_target_metrics.get(comp, {})
        total_links = int(comp_metrics.get("total_links", 0) or 0)
        http2xx_pct = _pct((comp_metrics.get("http_class", Counter()) or Counter()).get("2xx", 0), max(1, total_links))
        follow_pct = float(comp_metrics.get("follow_pct", 0.0) or 0.0)
        lost_pct = float(comp_metrics.get("lost_pct", 0.0) or 0.0)
        homepage_pct = float(comp_metrics.get("homepage_pct", 0.0) or 0.0)
        quality_score = round(
            (follow_pct * 0.35)
            + (http2xx_pct * 0.25)
            + ((100.0 - lost_pct) * 0.25)
            + ((100.0 - homepage_pct) * 0.15),
            2,
        )
        competitor_quality_rows.append(
            {
                "competitor_domain": comp,
                "links_in_dataset": total_links,
                "follow_pct": round(follow_pct, 2),
                "lost_pct": round(lost_pct, 2),
                "http_2xx_pct": round(http2xx_pct, 2),
                "homepage_target_pct": round(homepage_pct, 2),
                "quality_score_0_100": quality_score,
            }
        )
    competitor_quality_rows.sort(key=lambda x: float(x.get("quality_score_0_100", 0.0)), reverse=True)

    dr_deciles = [
        "DR 0-9",
        "DR 10-19",
        "DR 20-29",
        "DR 30-39",
        "DR 40-49",
        "DR 50-59",
        "DR 60-69",
        "DR 70-79",
        "DR 80-89",
        "DR 90-100",
    ]
    dr_distribution_matrix_rows: List[Dict[str, Any]] = []
    matrix_domains = [x.get("competitor_domain") for x in competitor_rows if x.get("competitor_domain")]
    for comp in matrix_domains:
        m = per_target_metrics.get(str(comp), {})
        total_links = max(1, int(m.get("total_links", 0) or 0))
        dr10 = m.get("dr10_counts", Counter()) or Counter()
        row: Dict[str, Any] = {"Домен": comp}
        for bucket in dr_deciles:
            row[bucket] = _pct(dr10.get(bucket, 0), total_links)
        dr_distribution_matrix_rows.append(row)
    if dr_distribution_matrix_rows:
        avg_row: Dict[str, Any] = {"Домен": "Средние"}
        med_row: Dict[str, Any] = {"Домен": "Медиана"}
        for bucket in dr_deciles:
            vals = [float(x.get(bucket, 0.0) or 0.0) for x in dr_distribution_matrix_rows]
            avg_row[bucket] = round(mean(vals), 2) if vals else 0.0
            med_row[bucket] = round(median(vals), 2) if vals else 0.0
        dr_distribution_matrix_rows.append(avg_row)
        dr_distribution_matrix_rows.append(med_row)
    our_dr10 = (our_metrics or {}).get("dr10_counts", Counter()) or Counter()
    our_total = max(1, int((our_metrics or {}).get("total_links", 0) or 0))
    our_row: Dict[str, Any] = {"Домен": domain}
    for bucket in dr_deciles:
        our_row[bucket] = _pct(our_dr10.get(bucket, 0), our_total)
    dr_distribution_matrix_rows.append(our_row)

    def _build_mix_benchmark(
        value_getter: Callable[[Dict[str, Any]], Counter[str]],
        categories: List[str],
        title_prefix: str = "",
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for cat in categories:
            comp_vals: List[float] = []
            for comp in competitors_list:
                m = per_target_metrics.get(comp, {})
                cnt = value_getter(m) or Counter()
                total_links = max(1, int(m.get("total_links", 0) or 0))
                comp_vals.append(_pct(cnt.get(cat, 0), total_links))
            our_cnt = value_getter(our_metrics or {}) or Counter()
            our_val = _pct(our_cnt.get(cat, 0), max(1, int((our_metrics or {}).get("total_links", 0) or 0)))
            rows.append(
                {
                    "metric": f"{title_prefix}{cat}",
                    "our_site_pct": our_val,
                    "competitors_avg_pct": round(mean(comp_vals), 2) if comp_vals else 0.0,
                    "competitors_median_pct": round(median(comp_vals), 2) if comp_vals else 0.0,
                }
            )
        return rows

    anchor_mix_benchmark_rows = _build_mix_benchmark(
        lambda m: m.get("anchor_type", Counter()) or Counter(),
        ["empty", "brand", "commercial", "informational", "spam", "other"],
        "anchor:",
    )
    link_type_categories = sorted(set(link_type_counter.keys()))[:20]
    link_type_benchmark_rows = _build_mix_benchmark(
        lambda m: m.get("link_type", Counter()) or Counter(),
        link_type_categories,
        "type:",
    )
    http_categories = ["2xx", "3xx", "4xx", "5xx", "unknown"]
    http_benchmark_rows = _build_mix_benchmark(
        lambda m: m.get("http_class", Counter()) or Counter(),
        http_categories,
        "http:",
    )
    follow_home_internal_benchmark_rows = benchmark_rows[:]

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
    opportunity_domains_rows: List[Dict[str, Any]] = []
    total_competitors = max(1, len(competitor_counter))
    for d in candidate_domains:
        stats = source_domain_stats.get(str(d), {})
        competitors_hit = len(source_to_competitors.get(str(d), set()))
        total = float(stats.get("count", 0.0))
        if total <= 0:
            continue
        avg_dr = float(stats.get("dr_sum", 0.0)) / max(1.0, float(stats.get("dr_n", 0.0)))
        avg_traffic = float(stats.get("traffic_sum", 0.0)) / max(1.0, float(stats.get("traffic_n", 0.0)))
        nofollow_rate = float(stats.get("nofollow_n", 0.0)) / total
        lost_rate = float(stats.get("lost_n", 0.0)) / total
        http2xx_rate = float(stats.get("http2xx_n", 0.0)) / total
        coverage_pct = _pct(competitors_hit, total_competitors)
        opportunity_score = round(
            (coverage_pct * 0.35)
            + (avg_dr * 0.35)
            + (min(avg_traffic, 10000.0) / 100.0 * 0.15)
            + ((1.0 - nofollow_rate) * 100.0 * 0.1)
            + ((1.0 - lost_rate) * 100.0 * 0.05),
            2,
        )
        opportunity_domains_rows.append(
            {
                "domain": d,
                "competitors_covered": competitors_hit,
                "competitors_covered_pct": coverage_pct,
                "links_in_dataset": int(total),
                "avg_dr": round(avg_dr, 2),
                "avg_traffic": round(avg_traffic, 2),
                "follow_pct": round((1.0 - nofollow_rate) * 100.0, 2),
                "lost_pct": round(lost_rate * 100.0, 2),
                "http_2xx_pct": round(http2xx_rate * 100.0, 2),
                "opportunity_score": opportunity_score,
            }
        )
    opportunity_domains_rows.sort(key=lambda x: (float(x.get("opportunity_score", 0.0)), float(x.get("competitors_covered", 0))), reverse=True)
    opportunity_domains_rows = opportunity_domains_rows[:500]
    ready_buy_rows = [
        x
        for x in opportunity_domains_rows
        if float(x.get("avg_dr") or 0.0) >= 30.0
        and float(x.get("avg_traffic") or 0.0) >= 50.0
        and float(x.get("follow_pct") or 0.0) >= 60.0
        and float(x.get("lost_pct") or 100.0) <= 40.0
    ][:300]
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
    follow_mix_rows = _counter_to_pct_rows(follow_counter, value_key="type", limit=10)
    follow_domain_class_counter: Counter[str] = Counter()
    for d, fc in source_follow_profile.items():
        has_do = int(fc.get("dofollow", 0)) > 0
        has_no = int(fc.get("nofollow", 0)) > 0
        has_un = int(fc.get("unknown", 0)) > 0
        if has_do and not has_no:
            follow_domain_class_counter["follow_only_domains"] += 1
        elif has_no and not has_do:
            follow_domain_class_counter["nofollow_only_domains"] += 1
        elif has_do and has_no:
            follow_domain_class_counter["mixed_follow_nofollow_domains"] += 1
        elif has_un:
            follow_domain_class_counter["unknown_only_domains"] += 1
    follow_domain_mix_rows = _counter_to_pct_rows(follow_domain_class_counter, value_key="segment", limit=10)
    lost_status_rows = _counter_to_pct_rows(lost_status_counter, value_key="lost_status", limit=20)
    http_class_rows = _counter_to_pct_rows(http_class_counter, value_key="http_class", limit=10)
    link_type_mix_rows = _counter_to_pct_rows(link_type_counter, value_key="link_type", limit=20)
    language_mix_rows = _counter_to_pct_rows(language_counter, value_key="language", limit=20)
    anchor_mix_rows = _counter_to_pct_rows(anchor_type_counter, value_key="anchor_type", limit=20)
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
        "dofollow_pct": _pct(follow_counter.get("dofollow", 0), sum(follow_counter.values())),
        "nofollow_pct": _pct(follow_counter.get("nofollow", 0), sum(follow_counter.values())),
        "lost_links_pct": _pct(sum(lost_status_counter.values()), len(normalized_rows)),
        "http_2xx_pct": _pct(http_class_counter.get("2xx", 0), sum(http_class_counter.values())),
    }

    avg_comp_follow = round(mean([float(x.get("follow_pct") or 0.0) for x in competitor_quality_rows]), 2) if competitor_quality_rows else 0.0
    avg_comp_lost = round(mean([float(x.get("lost_pct") or 0.0) for x in competitor_quality_rows]), 2) if competitor_quality_rows else 0.0
    avg_comp_quality = round(mean([float(x.get("quality_score_0_100") or 0.0) for x in competitor_quality_rows]), 2) if competitor_quality_rows else 0.0
    avg_comp_http2xx = round(mean([float(x.get("http_2xx_pct") or 0.0) for x in competitor_quality_rows]), 2) if competitor_quality_rows else 0.0

    executive_kpi_rows = [
        {
            "Показатель": "Доля dofollow ссылок, %",
            "Наш сайт": summary.get("dofollow_pct"),
            "Среднее конкурентов": avg_comp_follow,
            "Разница (п.п.)": round(float(summary.get("dofollow_pct") or 0.0) - avg_comp_follow, 2),
        },
        {
            "Показатель": "Доля потерянных ссылок, %",
            "Наш сайт": summary.get("lost_links_pct"),
            "Среднее конкурентов": avg_comp_lost,
            "Разница (п.п.)": round(float(summary.get("lost_links_pct") or 0.0) - avg_comp_lost, 2),
        },
        {
            "Показатель": "Доля 2xx доноров, %",
            "Наш сайт": summary.get("http_2xx_pct"),
            "Среднее конкурентов": avg_comp_http2xx,
            "Разница (п.п.)": round(float(summary.get("http_2xx_pct") or 0.0) - avg_comp_http2xx, 2),
        },
        {
            "Показатель": "Средний DR доноров",
            "Наш сайт": summary.get("avg_our_dr"),
            "Среднее конкурентов": round(mean([float(x) for x in comp_dr_values]), 2) if comp_dr_values else None,
            "Разница (п.п.)": round(float(summary.get("avg_our_dr") or 0.0) - (round(mean([float(x) for x in comp_dr_values]), 2) if comp_dr_values else 0.0), 2),
        },
        {
            "Показатель": "Индекс качества профиля (0-100)",
            "Наш сайт": round(
                (float(summary.get("dofollow_pct") or 0.0) * 0.35)
                + (float(summary.get("http_2xx_pct") or 0.0) * 0.25)
                + ((100.0 - float(summary.get("lost_links_pct") or 0.0)) * 0.25)
                + ((100.0 - float(our_metrics.get("homepage_pct", 0.0) or 0.0)) * 0.15),
                2,
            ),
            "Среднее конкурентов": avg_comp_quality,
            "Разница (п.п.)": round(
                round(
                    (float(summary.get("dofollow_pct") or 0.0) * 0.35)
                    + (float(summary.get("http_2xx_pct") or 0.0) * 0.25)
                    + ((100.0 - float(summary.get("lost_links_pct") or 0.0)) * 0.25)
                    + ((100.0 - float(our_metrics.get("homepage_pct", 0.0) or 0.0)) * 0.15),
                    2,
                )
                - avg_comp_quality,
                2,
            ),
        },
    ]

    profile_structure_rows = [
        {"Метрика": "Анкоры: безанкорные, %", "Значение": _pct(anchor_type_counter.get("empty", 0), sum(anchor_type_counter.values()))},
        {"Метрика": "Анкоры: брендовые, %", "Значение": _pct(anchor_type_counter.get("brand", 0), sum(anchor_type_counter.values()))},
        {"Метрика": "Анкоры: коммерческие, %", "Значение": _pct(anchor_type_counter.get("commercial", 0), sum(anchor_type_counter.values()))},
        {"Метрика": "Анкоры: спам, %", "Значение": _pct(anchor_type_counter.get("spam", 0), sum(anchor_type_counter.values()))},
        {"Метрика": "Follow-ссылки на главную, %", "Значение": round(float(our_metrics.get("homepage_follow_pct", 0.0) or 0.0), 2)},
        {"Метрика": "Follow-ссылки на внутренние, %", "Значение": round(float(our_metrics.get("internal_follow_pct", 0.0) or 0.0), 2)},
    ]

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
                "duplicates": {"rows": duplicates_with_our},
                "additionalMetrics": {"rows": dr_bucket_rows},
                "combinedOutput": {"rows": []},
            },
            "tables": {
                "competitor_analysis": competitor_rows,
                "anchor_analysis": top_anchors,
                "anchor_word_analysis": anchor_word_rows,
                "duplicates_with_our_site": duplicates_with_our,
                "duplicates_with_two_competitors": duplicates_without_our,
                "single_competitor_domains": single_competitor,
                "single_our_domains": single_our,
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
                "follow_mix_pct": follow_mix_rows,
                "follow_domain_mix_pct": follow_domain_mix_rows,
                "follow_types_detailed": follow_detail_rows,
                "lost_status_mix": lost_status_rows,
                "http_class_mix": http_class_rows,
                "link_type_mix": link_type_mix_rows,
                "language_mix": language_mix_rows,
                "anchor_mix_pct": anchor_mix_rows,
                "donors_with_redirect_301": redirect_301_rows,
                "donors_homepage": homepage_donor_rows,
                "brand_keywords_auto": brand_rows,
                "source_files": file_summaries,
                "link_types_by_competitor": link_types_by_competitor,
                "http_codes_by_competitor": http_codes_by_competitor,
                "languages_by_competitor": languages_by_competitor,
                "competitor_quality": competitor_quality_rows,
                "competitor_ranking": competitor_rank_rows,
                "opportunity_domains": opportunity_domains_rows,
                "ready_buy_domains": ready_buy_rows,
                "dr_distribution_matrix": dr_distribution_matrix_rows,
                "raw_homepage_links": raw_homepage_links_rows,
                "raw_redirect_links": raw_redirect_links_rows,
                "raw_duplicates_without_our": raw_duplicates_without_our_rows,
                "executive_kpi": executive_kpi_rows,
                "profile_structure": profile_structure_rows,
                "ourSiteTables": [
                    {"title": "KPI по нашему сайту vs среднее конкурентов", "rows": executive_kpi_rows},
                    {"title": "Структура ссылочного профиля (наш сайт)", "rows": profile_structure_rows},
                    {"title": "Наш сайт: доноры", "rows": our_site_rows},
                    {"title": "Ссылки с главных (raw)", "rows": raw_homepage_links_rows},
                    {"title": "Ссылки с редиректов (raw)", "rows": raw_redirect_links_rows},
                    {"title": "Дубликаты без нашего сайта (raw)", "rows": raw_duplicates_without_our_rows},
                ],
                "competitorTables": [
                    {"title": "Конкуренты", "rows": competitor_rows},
                    {"title": "Рейтинг конкурентов (DR / Backlinks / Follow%)", "rows": competitor_rank_rows},
                    {"title": "Качество профиля конкурентов (0-100)", "rows": competitor_quality_rows},
                ],
                "comparisonTables": [
                    {"title": "Сравнение с конкурентами", "rows": comparison_rows},
                    {"title": "Бенчмарк avg/median по конкурентам", "rows": benchmark_rows},
                    {"title": "Бенчмарк по анкорам (avg/median)", "rows": anchor_mix_benchmark_rows},
                    {"title": "Бенчмарк по типам ссылок (avg/median)", "rows": link_type_benchmark_rows},
                    {"title": "Бенчмарк по HTTP кодам (avg/median)", "rows": http_benchmark_rows},
                    {"title": "Бенчмарк follow/home/internal (avg/median)", "rows": follow_home_internal_benchmark_rows},
                    {"title": "DR распределение доноров по доменам (%)", "rows": dr_distribution_matrix_rows},
                    {"title": "Матрица возможностей доноров", "rows": opportunity_domains_rows},
                    {"title": "Ready-to-buy доноры (GGL/Miralinks)", "rows": ready_buy_rows},
                ],
                "additionalTables": [
                    {"title": "DR статистика", "rows": dr_stats_rows},
                    {"title": "DR buckets", "rows": dr_bucket_rows},
                    {"title": "DR buckets: наш сайт", "rows": dr_bucket_our_rows},
                    {"title": "DR buckets: конкуренты", "rows": dr_bucket_comp_rows},
                    {"title": "Domain zones", "rows": zone_rows},
                    {"title": "Follow / Nofollow", "rows": follow_rows},
                    {"title": "Follow / Nofollow %", "rows": follow_mix_rows},
                    {"title": "Follow/Nofollow по доменам %", "rows": follow_domain_mix_rows},
                    {"title": "Follow / Nofollow detailed", "rows": follow_detail_rows},
                    {"title": "Anchor mix %", "rows": anchor_mix_rows},
                    {"title": "Lost status mix", "rows": lost_status_rows},
                    {"title": "HTTP class mix", "rows": http_class_rows},
                    {"title": "Link type mix", "rows": link_type_mix_rows},
                    {"title": "Language mix", "rows": language_mix_rows},
                    {"title": "Donors with redirect 301", "rows": redirect_301_rows},
                    {"title": "Donors from homepage", "rows": homepage_donor_rows},
                    {"title": "Link types by competitor", "rows": link_types_by_competitor},
                    {"title": "HTTP codes by competitor", "rows": http_codes_by_competitor},
                    {"title": "Languages by competitor", "rows": languages_by_competitor},
                    {"title": "Auto brand keywords", "rows": brand_rows},
                    {"title": "Source files", "rows": file_summaries},
                ],
                "duplicatesTables": [
                    {"title": "Duplicates with our site", "rows": duplicates_with_our},
                    {"title": "Duplicates with two competitors", "rows": duplicates_without_our},
                    {"title": "Single competitor domains", "rows": single_competitor},
                    {"title": "Single our domains", "rows": single_our},
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
