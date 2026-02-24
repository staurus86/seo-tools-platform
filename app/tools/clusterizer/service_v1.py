"""Basic keyword clustering by token similarity."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import re
from typing import Any, Callable, Dict, List, Optional, Sequence, Set

SimilarityMethod = str
ProgressCallback = Optional[Callable[[int, str], None]]

_STOP_WORDS: Set[str] = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "how",
    "in",
    "is",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "with",
    "и",
    "в",
    "во",
    "на",
    "по",
    "под",
    "от",
    "до",
    "к",
    "ко",
    "из",
    "за",
    "у",
    "о",
    "об",
    "для",
    "как",
    "или",
    "это",
    "что",
}


def _normalize_keyword(value: str) -> str:
    text = str(value or "").strip().lower().replace("ё", "е")
    text = re.sub(r"[_/\\|]+", " ", text)
    text = re.sub(r"[^\w\s-]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _tokenize(normalized_keyword: str) -> Set[str]:
    raw_tokens = [token for token in re.split(r"\s+", normalized_keyword) if token]
    filtered = [token for token in raw_tokens if len(token) > 1 and token not in _STOP_WORDS]
    if filtered:
        return set(filtered)
    return set(raw_tokens)


def _calc_similarity(tokens_a: Set[str], tokens_b: Set[str], method: SimilarityMethod) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    intersection_size = len(tokens_a & tokens_b)
    if intersection_size == 0:
        return 0.0
    if method == "overlap":
        return intersection_size / float(min(len(tokens_a), len(tokens_b)))
    if method == "dice":
        return (2.0 * intersection_size) / float(len(tokens_a) + len(tokens_b))
    union_size = len(tokens_a | tokens_b)
    return intersection_size / float(union_size) if union_size else 0.0


def run_keyword_clusterizer(
    *,
    keywords: Sequence[str],
    method: SimilarityMethod = "jaccard",
    similarity_threshold: float = 0.35,
    min_cluster_size: int = 2,
    progress_callback: ProgressCallback = None,
) -> Dict[str, Any]:
    method_normalized = str(method or "jaccard").strip().lower()
    if method_normalized not in {"jaccard", "overlap", "dice"}:
        method_normalized = "jaccard"
    threshold = max(0.01, min(1.0, float(similarity_threshold or 0.35)))
    min_size = max(1, int(min_cluster_size or 2))

    if progress_callback:
        progress_callback(5, "Подготовка ключевых фраз")

    normalized_to_display: Dict[str, str] = {}
    normalized_order: List[str] = []
    normalized_freq: Counter[str] = Counter()
    total_input_keywords = 0

    for raw_keyword in keywords or []:
        clean_keyword = str(raw_keyword or "").strip()
        if not clean_keyword:
            continue
        total_input_keywords += 1
        normalized = _normalize_keyword(clean_keyword)
        if not normalized:
            continue
        normalized_freq[normalized] += 1
        if normalized not in normalized_to_display:
            normalized_to_display[normalized] = clean_keyword
            normalized_order.append(normalized)

    unique_keywords_count = len(normalized_order)
    display_keywords = [normalized_to_display[norm] for norm in normalized_order]
    token_sets = [_tokenize(norm) for norm in normalized_order]
    adjacency: List[Set[int]] = [set() for _ in range(unique_keywords_count)]

    comparisons_total = (unique_keywords_count * (unique_keywords_count - 1)) // 2
    comparisons_done = 0

    if progress_callback:
        progress_callback(10, "Сравнение ключей")

    for i in range(unique_keywords_count):
        tokens_i = token_sets[i]
        for j in range(i + 1, unique_keywords_count):
            comparisons_done += 1
            tokens_j = token_sets[j]
            if not tokens_i or not tokens_j or tokens_i.isdisjoint(tokens_j):
                continue
            similarity = _calc_similarity(tokens_i, token_sets[j], method_normalized)
            if similarity >= threshold:
                adjacency[i].add(j)
                adjacency[j].add(i)
        if progress_callback and comparisons_total > 0 and (i % 25 == 0 or i == unique_keywords_count - 1):
            progress_pct = 10 + int((comparisons_done / float(comparisons_total)) * 70.0)
            progress_callback(min(85, progress_pct), f"Сравнено пар: {comparisons_done}/{comparisons_total}")

    if progress_callback:
        progress_callback(88, "Формирование кластеров")

    visited = [False] * unique_keywords_count
    components: List[List[int]] = []

    for start_idx in range(unique_keywords_count):
        if visited[start_idx]:
            continue
        stack = [start_idx]
        visited[start_idx] = True
        component: List[int] = []
        while stack:
            current_idx = stack.pop()
            component.append(current_idx)
            for neighbor_idx in adjacency[current_idx]:
                if not visited[neighbor_idx]:
                    visited[neighbor_idx] = True
                    stack.append(neighbor_idx)
        component.sort()
        components.append(component)

    components.sort(key=lambda items: (-len(items), items[0] if items else 0))

    clusters: List[Dict[str, Any]] = []
    unclustered_keywords: List[str] = []

    for cluster_pos, component in enumerate(components, start=1):
        component_set = set(component)
        keywords_in_cluster = [display_keywords[idx] for idx in component]
        token_counter: Counter[str] = Counter()
        for idx in component:
            token_counter.update(token_sets[idx])
        top_tokens = [token for token, _ in token_counter.most_common(8)]
        representative_idx = sorted(
            component,
            key=lambda idx: (-len(token_sets[idx]), len(display_keywords[idx]), idx),
        )[0]
        edge_count = sum(len(adjacency[idx] & component_set) for idx in component) // 2
        max_edges = (len(component) * (len(component) - 1)) // 2
        density = (edge_count / float(max_edges)) if max_edges else 0.0
        duplicates_in_cluster = sum(normalized_freq.get(normalized_order[idx], 1) for idx in component)

        cluster_payload = {
            "cluster_id": cluster_pos,
            "size": len(component),
            "size_with_duplicates": duplicates_in_cluster,
            "representative": display_keywords[representative_idx],
            "top_tokens": top_tokens,
            "edge_count": edge_count,
            "density": round(density, 4),
            "keywords": keywords_in_cluster,
        }
        clusters.append(cluster_payload)
        if len(component) == 1:
            unclustered_keywords.append(keywords_in_cluster[0])

    primary_clusters = [cluster for cluster in clusters if int(cluster.get("size", 0)) >= min_size]
    singleton_clusters = sum(1 for cluster in clusters if int(cluster.get("size", 0)) == 1)
    multi_keyword_clusters = max(0, len(clusters) - singleton_clusters)
    biggest_cluster_size = max([int(cluster.get("size", 0)) for cluster in clusters], default=0)
    avg_cluster_size = (
        round(unique_keywords_count / float(len(clusters)), 2)
        if clusters
        else 0.0
    )

    if progress_callback:
        progress_callback(100, "Кластеризация завершена")

    return {
        "task_type": "clusterizer",
        "url": "keywords://clusterizer",
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "results": {
            "engine": "keyword-clusterizer-v1",
            "settings": {
                "method": method_normalized,
                "similarity_threshold": round(threshold, 4),
                "similarity_threshold_pct": int(round(threshold * 100)),
                "min_cluster_size": min_size,
            },
            "summary": {
                "keywords_input_total": total_input_keywords,
                "keywords_unique_total": unique_keywords_count,
                "duplicates_removed": max(0, total_input_keywords - unique_keywords_count),
                "clusters_total": len(clusters),
                "primary_clusters_total": len(primary_clusters),
                "multi_keyword_clusters": multi_keyword_clusters,
                "singleton_clusters": singleton_clusters,
                "biggest_cluster_size": biggest_cluster_size,
                "avg_cluster_size": avg_cluster_size,
                "comparisons_total": comparisons_total,
            },
            "clusters": clusters,
            "primary_clusters": primary_clusters,
            "unclustered_keywords": unclustered_keywords,
        },
    }
