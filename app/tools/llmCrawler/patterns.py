"""Pattern libraries for AI-oriented block detection."""
from __future__ import annotations

from typing import Any, Dict, List, Set
import re


SCHEMA_TYPE_GROUPS: Dict[str, Set[str]] = {
    "organization": {
        "organization",
        "corporation",
        "ngo",
        "governmentorganization",
        "educationalorganization",
        "localbusiness",
        "brand",
        "publisher",
    },
    "person": {"person"},
    "article": {
        "article",
        "newsarticle",
        "blogposting",
        "techarticle",
        "analysisnewsarticle",
        "report",
        "liveblogposting",
        "scholarlyarticle",
    },
    "product": {"product", "productmodel", "individualproduct", "service"},
    "offer": {"offer", "aggregateoffer", "demand"},
    "review": {"review", "aggregaterating", "rating", "criticreview", "userreview"},
    "faq": {"faqpage", "qapage", "question", "answer"},
    "breadcrumb": {"breadcrumblist", "listitem"},
    "itemlist": {"itemlist", "collectionpage"},
    "howto": {"howto", "howtosection", "howtostep"},
    "event": {"event", "sportsEvent".lower(), "musicevent"},
    "job": {"jobposting"},
    "video": {"videoobject"},
    "recipe": {"recipe"},
    "website": {"website", "webpage"},
}


BLOCK_PATTERN_LIBRARY: Dict[str, Dict[str, Any]] = {
    "main_content_block": {
        "label": "Main content block",
        "category": "content",
        "critical": True,
        "critical_for": ["any"],
        "selectors": ["main", "article", "[role='main']", ".article-content", ".post-content", ".content-body"],
        "regex": [r"\b(introduction|overview|summary|описание|обзор|руководство)\b"],
        "schema_types": ["Article", "WebPage"],
        "page_types": ["any"],
    },
    "author_block": {
        "label": "Author block",
        "category": "trust",
        "critical": True,
        "critical_for": ["article", "review", "news", "docs"],
        "selectors": ["meta[name=author]", "[itemprop='author']", ".author", ".byline", ".post-author", "[rel='author']"],
        "regex": [r"\b(author|written by|edited by|reviewed by|by\s+[A-Z][a-z]+|автор|редактор)\b"],
        "schema_types": ["Person", "Article"],
        "page_types": ["article", "review", "news", "docs", "mixed"],
    },
    "date_block": {
        "label": "Publish date",
        "category": "trust",
        "critical": True,
        "critical_for": ["article", "review", "news"],
        "selectors": ["time[datetime]", "meta[property='article:published_time']", "meta[name='publish_date']", ".published", ".updated"],
        "regex": [r"\b(updated|published|last modified|дата публикации|обновлено)\b"],
        "schema_types": ["Article", "NewsArticle", "Review"],
        "page_types": ["article", "review", "news", "mixed"],
    },
    "organization_block": {
        "label": "Organization identity",
        "category": "trust",
        "critical": True,
        "critical_for": ["service", "product", "homepage", "category", "review", "article"],
        "selectors": ["meta[property='og:site_name']", ".organization", ".company", ".publisher", "[itemprop='publisher']", "[itemprop='brand']"],
        "regex": [r"\b(company|organization|publisher|about us|о компании|контакты компании)\b"],
        "schema_types": ["Organization", "LocalBusiness", "Brand"],
        "page_types": ["service", "product", "homepage", "category", "review", "article", "mixed"],
    },
    "contact_block": {
        "label": "Contact info",
        "category": "trust",
        "critical": True,
        "critical_for": ["service", "product", "homepage", "category", "review"],
        "selectors": [".contact", "a[href^='mailto:']", "a[href^='tel:']", "[itemprop='address']", "[itemprop='telephone']", ".footer-contacts"],
        "regex": [r"@[\w.-]+\.[A-Za-z]{2,}", r"\+\d[\d\-\s]{6,}", r"\b(contact|support|phone|email|контакты|связаться|поддержка)\b"],
        "schema_types": ["Organization", "LocalBusiness"],
        "page_types": ["service", "product", "homepage", "category", "review", "mixed"],
    },
    "policy_block": {
        "label": "Policy/legal links",
        "category": "trust",
        "critical": False,
        "selectors": ["a[href*='privacy']", "a[href*='terms']", "footer"],
        "regex": [r"\b(privacy|terms|gdpr|cookie|политика|условия|оферта)\b"],
        "schema_types": [],
        "page_types": ["any"],
    },
    "faq_block": {
        "label": "FAQ block",
        "category": "content",
        "critical": False,
        "selectors": [".faq", "[data-faq]", ".accordion", "[itemtype*='FAQPage']"],
        "regex": [r"\b(faq|frequently asked|вопросы|вопрос\s*и\s*ответ)\b"],
        "schema_types": ["FAQPage", "QAPage", "Question", "Answer"],
        "page_types": ["service", "product", "article", "docs", "mixed", "faq"],
    },
    "howto_block": {
        "label": "How-to steps",
        "category": "content",
        "critical": False,
        "selectors": [".how-to", ".steps", "ol li", "[itemtype*='HowTo']"],
        "regex": [r"\b(step\s+\d+|how to|инструкция|шаг\s+\d+)\b"],
        "schema_types": ["HowTo"],
        "page_types": ["article", "docs", "service", "mixed"],
    },
    "itemlist_block": {
        "label": "Listing/feed structure",
        "category": "navigation",
        "critical": False,
        "selectors": [".listing", ".feed", ".catalog", ".grid", ".cards", "[itemtype*='ItemList']"],
        "regex": [r"\b(latest|news|feed|catalog|collection|archive|лента|каталог|раздел)\b"],
        "schema_types": ["ItemList", "CollectionPage"],
        "page_types": ["listing", "homepage", "category", "mixed", "news"],
    },
    "pricing_block": {
        "label": "Pricing block",
        "category": "commercial",
        "critical": False,
        "selectors": [".pricing", ".price", "[data-price]", "[itemprop='price']", "[itemtype*='Offer']"],
        "regex": [r"\b(price|pricing|cost|usd|\$|eur|руб|цена|тариф)\b"],
        "schema_types": ["Offer", "AggregateOffer", "Product", "Service"],
        "page_types": ["product", "service", "category", "homepage", "mixed"],
    },
    "cta_block": {
        "label": "Call to action",
        "category": "commercial",
        "critical": False,
        "selectors": [".cta", ".btn-primary", ".button-primary", "button", "a[class*='cta']"],
        "regex": [r"\b(start now|book demo|get started|buy now|try free|subscribe|купить|заказать|запросить)\b"],
        "schema_types": [],
        "page_types": ["service", "product", "homepage", "category", "mixed"],
    },
    "product_specs": {
        "label": "Product specs",
        "category": "commercial",
        "critical": False,
        "selectors": ["table", ".specs", ".technical-specs", ".product-attributes", "[itemprop='additionalProperty']"],
        "regex": [r"\b(specification|dimensions|weight|voltage|характеристики|спецификация)\b"],
        "schema_types": ["Product"],
        "page_types": ["product", "review", "category", "mixed"],
    },
    "breadcrumb_block": {
        "label": "Breadcrumbs",
        "category": "navigation",
        "critical": False,
        "selectors": [".breadcrumb", "nav[aria-label='breadcrumb']", "[itemtype*='BreadcrumbList']"],
        "regex": [r"\b(home\s*/|главная\s*/)\b"],
        "schema_types": ["BreadcrumbList"],
        "page_types": ["any"],
    },
    "review_block": {
        "label": "Reviews / testimonials",
        "category": "trust",
        "critical": False,
        "selectors": [".review", ".testimonial", "[itemprop='review']", "[itemtype*='Review']"],
        "regex": [r"\b(review|testimonial|rating|score|отзыв|рейтинг)\b"],
        "schema_types": ["Review", "AggregateRating", "Rating"],
        "page_types": ["review", "product", "service", "mixed"],
    },
    "event_block": {
        "label": "Event/live info",
        "category": "content",
        "critical": False,
        "selectors": [".event", ".match", ".live", "[itemtype*='Event']"],
        "regex": [r"\b(live|match|schedule|ticket|event|турнир|матч|расписание)\b"],
        "schema_types": ["Event", "SportsEvent"],
        "page_types": ["news", "listing", "mixed", "event"],
    },
    "job_block": {
        "label": "Job posting info",
        "category": "content",
        "critical": False,
        "selectors": [".job", ".vacancy", "[itemtype*='JobPosting']"],
        "regex": [r"\b(vacancy|salary|job|remote|full-time|вакансия|зарплата)\b"],
        "schema_types": ["JobPosting"],
        "page_types": ["listing", "category", "mixed"],
    },
    "video_block": {
        "label": "Video/media block",
        "category": "content",
        "critical": False,
        "selectors": ["video", "iframe[src*='youtube']", "iframe[src*='vimeo']", "[itemtype*='VideoObject']"],
        "regex": [r"\b(video|watch|stream|видео|трансляция)\b"],
        "schema_types": ["VideoObject"],
        "page_types": ["article", "news", "listing", "mixed"],
    },
    "code_block": {
        "label": "Code examples",
        "category": "technical",
        "critical": False,
        "selectors": ["pre", "code", ".code-block", ".highlight"],
        "regex": [r"\b(api|endpoint|curl|json|python|javascript|sdk|graphql)\b"],
        "schema_types": [],
        "page_types": ["docs", "article", "service", "mixed"],
    },
}


PAGE_TYPE_BLOCK_PROFILES: Dict[str, Dict[str, Set[str]]] = {
    "article": {
        "boost": {"main_content_block", "author_block", "date_block", "organization_block"},
        "critical": {"main_content_block", "author_block", "date_block", "organization_block"},
    },
    "news": {
        "boost": {"main_content_block", "author_block", "date_block", "event_block"},
        "critical": {"main_content_block", "author_block", "date_block"},
    },
    "listing": {
        "boost": {"itemlist_block", "main_content_block", "breadcrumb_block"},
        "critical": {"main_content_block"},
    },
    "homepage": {
        "boost": {"organization_block", "contact_block", "itemlist_block", "cta_block"},
        "critical": {"organization_block", "contact_block", "main_content_block"},
    },
    "category": {
        "boost": {"itemlist_block", "breadcrumb_block", "main_content_block"},
        "critical": {"main_content_block", "breadcrumb_block"},
    },
    "service": {
        "boost": {"organization_block", "contact_block", "pricing_block", "cta_block"},
        "critical": {"main_content_block", "organization_block", "contact_block"},
    },
    "product": {
        "boost": {"product_specs", "pricing_block", "review_block", "organization_block"},
        "critical": {"main_content_block", "organization_block", "pricing_block"},
    },
    "review": {
        "boost": {"review_block", "author_block", "date_block", "organization_block"},
        "critical": {"main_content_block", "review_block", "author_block"},
    },
    "docs": {
        "boost": {"code_block", "howto_block", "main_content_block"},
        "critical": {"main_content_block"},
    },
    "faq": {
        "boost": {"faq_block", "main_content_block"},
        "critical": {"main_content_block", "faq_block"},
    },
    "mixed": {"boost": {"main_content_block", "itemlist_block"}, "critical": {"main_content_block"}},
    "unknown": {"boost": {"main_content_block"}, "critical": {"main_content_block"}},
}


DIRECTIVE_RESTRICTIVE_TOKENS = [
    "noindex",
    "none",
    "nosnippet",
    "max-snippet:0",
    "noai",
    "noimageai",
]


def _normalize_schema_type(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "://" in raw or "/" in raw or "#" in raw:
        raw = re.split(r"[#?/]", raw)[-1]
    if ":" in raw:
        prefix, rest = raw.split(":", 1)
        if prefix.lower() in {"schema", "http", "https"} and rest:
            raw = rest
    return re.sub(r"[^A-Za-z0-9]", "", raw).lower()


def _schema_index(schema_types: List[str]) -> Set[str]:
    normalized: Set[str] = set()
    for item in (schema_types or []):
        n = _normalize_schema_type(item)
        if n:
            normalized.add(n)
    return normalized


def _schema_group_candidates(token: str) -> Set[str]:
    norm = _normalize_schema_type(token)
    if not norm:
        return set()
    if norm in SCHEMA_TYPE_GROUPS:
        return set(SCHEMA_TYPE_GROUPS[norm]) | {norm}
    return {norm}


def _schema_hits(schema_idx: Set[str], expected: List[str]) -> List[str]:
    hits: List[str] = []
    for item in (expected or []):
        variants = _schema_group_candidates(item)
        if variants and (schema_idx & variants):
            hits.append(str(item))
    return hits


def _selector_hits(soup: Any, selectors: List[str]) -> List[str]:
    hits: List[str] = []
    for selector in selectors:
        try:
            if soup.select(selector):
                hits.append(selector)
        except Exception:
            continue
    return hits


def _regex_hits(text: str, patterns: List[str]) -> List[str]:
    hits: List[str] = []
    for pattern in patterns:
        try:
            if re.search(pattern, text, flags=re.I):
                hits.append(pattern)
        except Exception:
            continue
    return hits


def _infer_page_type(text: str, schema_idx: Set[str]) -> str:
    raw = str(text or "").lower()
    if schema_idx & SCHEMA_TYPE_GROUPS["product"]:
        return "product"
    if schema_idx & SCHEMA_TYPE_GROUPS["review"]:
        return "review"
    if schema_idx & SCHEMA_TYPE_GROUPS["faq"]:
        return "faq"
    if schema_idx & SCHEMA_TYPE_GROUPS["article"]:
        return "article"
    if schema_idx & SCHEMA_TYPE_GROUPS["itemlist"]:
        return "listing"
    if any(tok in raw for tok in ("news", "latest", "feed", "лента", "новости")):
        return "news"
    if any(tok in raw for tok in ("docs", "documentation", "api", "руководство", "документац")):
        return "docs"
    if any(tok in raw for tok in ("service", "services", "agency", "consulting", "услуги")):
        return "service"
    if any(tok in raw for tok in ("catalog", "category", "категор", "products")):
        return "category"
    return "unknown"


def detect_ai_blocks(
    *,
    soup: Any,
    main_text: str,
    full_text: str,
    schema_types: List[str],
    page_type: str | None = None,
) -> Dict[str, Any]:
    text = (str(main_text or "") + " " + str(full_text or "")[:14000]).strip()
    schema_idx = _schema_index(schema_types or [])
    inferred_page_type = str(page_type or _infer_page_type(text, schema_idx) or "unknown").lower()
    profile = PAGE_TYPE_BLOCK_PROFILES.get(inferred_page_type, PAGE_TYPE_BLOCK_PROFILES["unknown"])

    detected: List[Dict[str, Any]] = []
    missing_critical: List[str] = []
    category_total: Dict[str, int] = {}
    category_hits: Dict[str, int] = {}

    for block_id, cfg in BLOCK_PATTERN_LIBRARY.items():
        category = str(cfg.get("category") or "other")
        category_total[category] = category_total.get(category, 0) + 1

        allowed_types = {str(x).lower() for x in (cfg.get("page_types") or ["any"])}
        relevant = ("any" in allowed_types) or (inferred_page_type in allowed_types)
        relevance_factor = 1.0 if relevant else 0.78

        sel_hits = _selector_hits(soup, list(cfg.get("selectors") or []))
        rx_hits = _regex_hits(text, list(cfg.get("regex") or []))
        schema_hits = _schema_hits(schema_idx, list(cfg.get("schema_types") or []))

        score = 0.0
        evidence: List[str] = []
        if sel_hits:
            score += 0.52
            evidence.append(f"DOM selectors: {', '.join(sel_hits[:3])}")
        if rx_hits:
            score += min(0.28, 0.1 * len(rx_hits))
            evidence.append(f"Text patterns: {len(rx_hits)}")
        if schema_hits:
            score += min(0.34, 0.16 + (0.08 * len(schema_hits)))
            evidence.append(f"Schema types: {', '.join(schema_hits[:3])}")
        if block_id in (profile.get("boost") or set()):
            score += 0.08
            evidence.append(f"Page profile boost: {inferred_page_type}")

        score *= relevance_factor
        confidence = round(min(1.0, score), 3)
        threshold = 0.42 if bool(cfg.get("critical")) else 0.46

        if confidence >= threshold:
            category_hits[category] = category_hits.get(category, 0) + 1
            detected.append(
                {
                    "id": block_id,
                    "label": cfg.get("label"),
                    "category": category,
                    "confidence": confidence,
                    "evidence": evidence[:4],
                    "page_relevant": bool(relevant),
                }
            )
        else:
            base_critical = bool(cfg.get("critical"))
            critical_for = {str(x).lower() for x in (cfg.get("critical_for") or [])}
            profile_critical = block_id in (profile.get("critical") or set())
            required_for_type = ("any" in critical_for) or (inferred_page_type in critical_for) or profile_critical
            if base_critical and required_for_type:
                missing_critical.append(str(cfg.get("label") or block_id))

    scores = {
        category: round((category_hits.get(category, 0) / max(1, total)) * 100, 2)
        for category, total in category_total.items()
    }
    coverage = round((len(detected) / max(1, len(BLOCK_PATTERN_LIBRARY))) * 100, 2)
    return {
        "coverage_percent": coverage,
        "detected": sorted(detected, key=lambda x: float(x.get("confidence", 0)), reverse=True),
        "missing_critical": list(dict.fromkeys(missing_critical)),
        "category_scores": scores,
        "page_type_profile": inferred_page_type,
        "pattern_version": "std-2.0",
        "schema_index_size": len(schema_idx),
    }
