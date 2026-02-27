"""Pattern libraries for AI-oriented block detection."""
from __future__ import annotations

from typing import Any, Dict, List
import re


BLOCK_PATTERN_LIBRARY: Dict[str, Dict[str, Any]] = {
    "author_block": {
        "label": "Author block",
        "category": "trust",
        "critical": True,
        "selectors": ["meta[name=author]", "[itemprop='author']", ".author", ".byline", ".post-author"],
        "regex": [r"\b(author|written by|by\s+[A-Z][a-z]+|автор|редактор)\b"],
        "schema_types": ["Person", "Article"],
    },
    "contact_block": {
        "label": "Contact info",
        "category": "trust",
        "critical": True,
        "selectors": [".contact", "a[href^='mailto:']", "a[href^='tel:']", "[itemprop='address']"],
        "regex": [r"@[\w.-]+\.[A-Za-z]{2,}", r"\+\d[\d\-\s]{6,}", r"\b(contact|support|контакты|связаться)\b"],
        "schema_types": ["Organization", "LocalBusiness"],
    },
    "faq_block": {
        "label": "FAQ block",
        "category": "content",
        "critical": False,
        "selectors": [".faq", "[data-faq]", ".accordion"],
        "regex": [r"\b(faq|frequently asked|вопросы|вопрос\s*и\s*ответ)\b"],
        "schema_types": ["FAQPage"],
    },
    "howto_block": {
        "label": "How-to steps",
        "category": "content",
        "critical": False,
        "selectors": [".how-to", ".steps", "ol li"],
        "regex": [r"\b(step\s+\d+|how to|инструкция|шаг\s+\d+)\b"],
        "schema_types": ["HowTo"],
    },
    "pricing_block": {
        "label": "Pricing block",
        "category": "commercial",
        "critical": False,
        "selectors": [".pricing", ".price", "[data-price]"],
        "regex": [r"\b(price|pricing|cost|usd|\$|руб|цена|тариф)\b"],
        "schema_types": ["Offer", "Product"],
    },
    "cta_block": {
        "label": "Call to action",
        "category": "commercial",
        "critical": False,
        "selectors": [".cta", ".btn-primary", "button"],
        "regex": [r"\b(start now|book demo|get started|buy now|купить|заказать|запросить)\b"],
        "schema_types": [],
    },
    "product_specs": {
        "label": "Product specs",
        "category": "commercial",
        "critical": False,
        "selectors": ["table", ".specs", ".technical-specs"],
        "regex": [r"\b(specification|dimensions|weight|voltage|характеристики|спецификация)\b"],
        "schema_types": ["Product"],
    },
    "breadcrumb_block": {
        "label": "Breadcrumbs",
        "category": "navigation",
        "critical": False,
        "selectors": [".breadcrumb", "nav[aria-label='breadcrumb']"],
        "regex": [r"\b(home\s*/|главная\s*/)\b"],
        "schema_types": ["BreadcrumbList"],
    },
    "review_block": {
        "label": "Reviews / testimonials",
        "category": "trust",
        "critical": False,
        "selectors": [".review", ".testimonial", "[itemprop='review']"],
        "regex": [r"\b(review|testimonial|rating|отзыв|рейтинг)\b"],
        "schema_types": ["Review", "AggregateRating"],
    },
    "code_block": {
        "label": "Code examples",
        "category": "technical",
        "critical": False,
        "selectors": ["pre", "code"],
        "regex": [r"\b(api|endpoint|curl|json|python|javascript)\b"],
        "schema_types": [],
    },
}


DIRECTIVE_RESTRICTIVE_TOKENS = [
    "noindex",
    "none",
    "nosnippet",
    "max-snippet:0",
    "noai",
    "noimageai",
]


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


def detect_ai_blocks(
    *,
    soup: Any,
    main_text: str,
    full_text: str,
    schema_types: List[str],
) -> Dict[str, Any]:
    text = (str(main_text or "") + " " + str(full_text or "")[:12000]).strip()
    schema_set = {str(s) for s in (schema_types or [])}
    detected: List[Dict[str, Any]] = []
    missing_critical: List[str] = []
    category_total: Dict[str, int] = {}
    category_hits: Dict[str, int] = {}

    for block_id, cfg in BLOCK_PATTERN_LIBRARY.items():
        category = str(cfg.get("category") or "other")
        category_total[category] = category_total.get(category, 0) + 1

        sel_hits = _selector_hits(soup, list(cfg.get("selectors") or []))
        rx_hits = _regex_hits(text, list(cfg.get("regex") or []))
        schema_hits = [s for s in list(cfg.get("schema_types") or []) if s in schema_set]

        score = 0.0
        evidence: List[str] = []
        if sel_hits:
            score += 0.55
            evidence.append(f"DOM selectors: {', '.join(sel_hits[:2])}")
        if rx_hits:
            score += min(0.3, 0.12 * len(rx_hits))
            evidence.append("Text patterns matched")
        if schema_hits:
            score += 0.25
            evidence.append(f"Schema types: {', '.join(schema_hits[:2])}")
        confidence = round(min(1.0, score), 3)

        if confidence >= 0.45:
            category_hits[category] = category_hits.get(category, 0) + 1
            detected.append(
                {
                    "id": block_id,
                    "label": cfg.get("label"),
                    "category": category,
                    "confidence": confidence,
                    "evidence": evidence[:3],
                }
            )
        elif bool(cfg.get("critical")):
            missing_critical.append(str(cfg.get("label") or block_id))

    scores = {
        category: round((category_hits.get(category, 0) / max(1, total)) * 100, 2)
        for category, total in category_total.items()
    }
    coverage = round((len(detected) / max(1, len(BLOCK_PATTERN_LIBRARY))) * 100, 2)
    return {
        "coverage_percent": coverage,
        "detected": sorted(detected, key=lambda x: float(x.get("confidence", 0)), reverse=True),
        "missing_critical": missing_critical,
        "category_scores": scores,
        "pattern_version": "std-1.0",
    }

