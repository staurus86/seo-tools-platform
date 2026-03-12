"""Graph algorithms for Site Audit Pro (PageRank, TF-IDF, semantic linking)."""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Any, Dict, List, Set, Tuple

from .schema import NormalizedSiteAuditRow
from .text_analysis import _tokenize_long


def _compute_pagerank(graph: Dict[str, Set[str]]) -> Dict[str, float]:
    nodes = list(graph.keys())
    n = len(nodes)
    if n == 0:
        return {}
    damping = 0.85
    scores = {u: 1.0 / n for u in nodes}
    for _ in range(20):
        new_scores = {u: (1.0 - damping) / n for u in nodes}
        dangling_sum = 0.0
        for u in nodes:
            outgoing = graph[u]
            if outgoing:
                share = scores[u] / len(outgoing)
                for v in outgoing:
                    new_scores[v] += damping * share
            else:
                dangling_sum += scores[u]
        dangling_share = damping * dangling_sum / n
        for u in nodes:
            new_scores[u] += dangling_share
        scores = new_scores
    max_score = max(scores.values()) if scores else 1.0
    return {u: round((s / max_score) * 100.0, 2) for u, s in scores.items()}


def _compute_tfidf_scores(page_texts: Dict[str, str], top_n: int = 10) -> Dict[str, Dict[str, float]]:
    if not page_texts:
        return {}
    word_doc_count: Counter = Counter()
    for text in page_texts.values():
        words = set(_tokenize_long(text, min_len=4))
        word_doc_count.update(words)

    total_docs = len(page_texts)
    result: Dict[str, Dict[str, float]] = {}
    for page_url, text in page_texts.items():
        words = _tokenize_long(text, min_len=4)
        if not words:
            result[page_url] = {}
            continue
        word_counts = Counter(words)
        tf_idf: Dict[str, float] = {}
        for word, freq in word_counts.most_common(50):
            tf = freq / len(words) if words else 0.0
            doc_freq = word_doc_count.get(word, 0)
            idf = math.log(total_docs / max(1, doc_freq)) if total_docs > 0 else 0.0
            score = tf * idf
            if score > 0.0001:
                tf_idf[word] = round(score, 6)
        sorted_terms = dict(sorted(tf_idf.items(), key=lambda x: x[1], reverse=True)[:top_n])
        result[page_url] = sorted_terms
    return result


def _build_semantic_linking_map(
    rows: List[NormalizedSiteAuditRow],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[str]]]:
    if not rows:
        return {}, {}
    keyword_map: Dict[str, Set[str]] = {}
    for row in rows:
        if row.tf_idf_keywords:
            keyword_map[row.url] = set(row.tf_idf_keywords.keys())
        else:
            keyword_map[row.url] = set(row.top_keywords or [])

    topic_clusters: Dict[str, List[str]] = defaultdict(list)
    semantic_by_url: Dict[str, List[Dict[str, Any]]] = {}
    for src in rows:
        src_keywords = keyword_map.get(src.url, set())
        semantic: List[Dict[str, Any]] = []
        for tgt in rows:
            if tgt.url == src.url:
                continue
            common = src_keywords & keyword_map.get(tgt.url, set())
            if common:
                common_sorted = sorted(common)
                semantic.append(
                    {
                        "target_url": tgt.url,
                        "target_title": tgt.title or "",
                        "matching_keywords": common_sorted,
                        "relevance_score": len(common_sorted),
                        "suggested_anchor": common_sorted[0] if common_sorted else "",
                    }
                )
        semantic_sorted = sorted(semantic, key=lambda x: x["relevance_score"], reverse=True)[:20]
        semantic_by_url[src.url] = semantic_sorted
        src.topic_hub = len(semantic_sorted) >= 3
        if semantic_sorted:
            cluster = (semantic_sorted[0].get("matching_keywords") or ["misc"])[0]
        elif src.tf_idf_keywords:
            cluster = next(iter(src.tf_idf_keywords.keys()), "misc")
        else:
            cluster = src.top_keywords[0] if src.top_keywords else "misc"
        src.topic_label = cluster
        topic_clusters[cluster].append(src.url)
    return semantic_by_url, topic_clusters


def _apply_linking_scores(
    rows: List[NormalizedSiteAuditRow],
    incoming_counts: Counter,
) -> None:
    if not rows:
        return
    for row in rows:
        row.orphan_page = int(incoming_counts.get(row.url, 0)) == 0
        pa = float(row.pagerank or 0.0)
        sem_count = len(row.semantic_links or [])
        incoming = int(incoming_counts.get(row.url, 0))
        link_authority = int(min(100, pa * 20 + sem_count * 10 + min(50, incoming * 2)))
        row.link_authority_score = link_authority

        outgoing_internal = int(row.outgoing_internal_links or 0)
        score = 0.0
        score += min(45.0, pa * 45.0)
        score += min(25.0, link_authority * 0.25)
        score += min(15.0, incoming * 2.0)
        score += min(10.0, sem_count * 2.0)
        if row.orphan_page:
            score -= 10.0
        if outgoing_internal == 0:
            score -= 5.0
        row.link_quality_score = float(int(max(0.0, min(100.0, score))))
