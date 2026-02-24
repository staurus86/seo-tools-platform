"""Basic keyword clustering by textual similarity."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import re
from typing import Any, Callable, Dict, List, Optional, Sequence, Set

SimilarityMethod = str
ClusteringMode = str
ProgressCallback = Optional[Callable[[int, str], None]]

_STOP_WORDS: Set[str] = {
    "a", "an", "and", "as", "at", "be", "by", "for", "from", "how", "in", "is", "it", "of", "on", "or", "that", "the", "to", "with",
    "и", "в", "во", "на", "по", "под", "от", "до", "к", "ко", "из", "за", "у", "о", "об", "для", "как", "или", "это", "что",
}

_RU_SUFFIXES = (
    "иями", "ями", "ами", "ого", "его", "ому", "ему", "ыми", "ими", "иями", "ьями",
    "иях", "иях", "иях", "иях", "ах", "ях", "ов", "ев", "ий", "ый", "ой", "ая", "ое", "ые",
    "ам", "ям", "ом", "ем", "ую", "юю", "ия", "ья", "ие", "ье", "а", "я", "ы", "и", "е", "о", "у", "ю",
)
_EN_SUFFIXES = ("ingly", "edly", "ing", "edly", "edly", "ed", "ies", "ers", "er", "es", "s", "ly")

_MODE_OFFSETS = {
    "strict": 0.08,
    "balanced": 0.0,
    "broad": -0.07,
}


def _normalize_keyword(value: str) -> str:
    text = str(value or "").strip().lower().replace("ё", "е")
    text = re.sub(r"[_/\\|]+", " ", text)
    text = re.sub(r"[^\w\s-]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _stem_token(token: str) -> str:
    value = str(token or "").strip().lower()
    if len(value) <= 3:
        return value
    for suffix in _RU_SUFFIXES:
        if value.endswith(suffix) and len(value) - len(suffix) >= 3:
            value = value[: -len(suffix)]
            break
    for suffix in _EN_SUFFIXES:
        if value.endswith(suffix) and len(value) - len(suffix) >= 3:
            value = value[: -len(suffix)]
            break
    return value


def _tokenize(normalized_keyword: str) -> Dict[str, Set[str]]:
    raw_tokens = [token for token in re.split(r"\s+", normalized_keyword) if token]
    filtered = [token for token in raw_tokens if len(token) > 1 and token not in _STOP_WORDS]
    tokens = filtered if filtered else raw_tokens
    stems = {_stem_token(token) for token in tokens if _stem_token(token)}
    return {
        "tokens": set(tokens),
        "stems": stems if stems else set(tokens),
    }


def _char_ngrams(normalized_keyword: str) -> Set[str]:
    compact = re.sub(r"\s+", "", normalized_keyword)
    if not compact:
        return set()
    n = 3 if len(compact) >= 5 else 2
    if len(compact) < n:
        return {compact}
    return {compact[i : i + n] for i in range(len(compact) - n + 1)}


def _set_similarity(set_a: Set[str], set_b: Set[str], method: SimilarityMethod) -> float:
    if not set_a or not set_b:
        return 0.0
    intersection_size = len(set_a & set_b)
    if intersection_size == 0:
        return 0.0
    if method == "overlap":
        return intersection_size / float(min(len(set_a), len(set_b)))
    if method == "dice":
        return (2.0 * intersection_size) / float(len(set_a) + len(set_b))
    union_size = len(set_a | set_b)
    return intersection_size / float(union_size) if union_size else 0.0


def _hybrid_similarity(entry_a: Dict[str, Any], entry_b: Dict[str, Any], method: SimilarityMethod) -> float:
    stem_score = _set_similarity(entry_a["stems"], entry_b["stems"], method)
    char_score = _set_similarity(entry_a["ngrams"], entry_b["ngrams"], "jaccard")
    if stem_score <= 0.0 and char_score < 0.72:
        return 0.0
    score = (stem_score * 0.82) + (char_score * 0.18)
    if stem_score > 0.0 and char_score > 0.0:
        score += 0.03
    return max(0.0, min(1.0, score))


def _cluster_quality_label(avg_similarity: float) -> str:
    if avg_similarity >= 0.78:
        return "high"
    if avg_similarity >= 0.62:
        return "medium"
    return "low"


def run_keyword_clusterizer(
    *,
    keywords: Sequence[str],
    method: SimilarityMethod = "jaccard",
    similarity_threshold: float = 0.35,
    min_cluster_size: int = 2,
    clustering_mode: ClusteringMode = "balanced",
    progress_callback: ProgressCallback = None,
) -> Dict[str, Any]:
    method_normalized = str(method or "jaccard").strip().lower()
    if method_normalized not in {"jaccard", "overlap", "dice"}:
        method_normalized = "jaccard"
    mode = str(clustering_mode or "balanced").strip().lower()
    if mode not in _MODE_OFFSETS:
        mode = "balanced"

    requested_threshold = max(0.01, min(1.0, float(similarity_threshold or 0.35)))
    effective_threshold = max(0.05, min(0.95, requested_threshold + _MODE_OFFSETS[mode]))
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

    entries: List[Dict[str, Any]] = []
    for idx, normalized in enumerate(normalized_order):
        tokens_data = _tokenize(normalized)
        entries.append(
            {
                "idx": idx,
                "normalized": normalized,
                "display": normalized_to_display[normalized],
                "tokens": tokens_data["tokens"],
                "stems": tokens_data["stems"],
                "ngrams": _char_ngrams(normalized),
            }
        )

    unique_keywords_count = len(entries)
    comparisons_total_potential = (unique_keywords_count * (unique_keywords_count - 1)) // 2

    if progress_callback:
        progress_callback(10, "Формирование кластеров")

    sorted_entries = sorted(
        entries,
        key=lambda item: (-len(item["stems"]), -len(item["tokens"]), len(item["display"]), item["idx"]),
    )
    clusters_state: List[Dict[str, Any]] = []
    similarity_checks = 0

    for pos, entry in enumerate(sorted_entries, start=1):
        best_cluster_idx: Optional[int] = None
        best_score = 0.0
        best_common_stems = 0
        for cluster_idx, cluster in enumerate(clusters_state):
            rep_entry = entries[cluster["rep_idx"]]
            similarity_checks += 1
            rep_sim = _hybrid_similarity(entry, rep_entry, method_normalized)
            common_stems = len(entry["stems"] & rep_entry["stems"])
            top_cluster_stems = {token for token, _ in cluster["stem_counter"].most_common(8)}
            centroid_overlap = (
                len(entry["stems"] & top_cluster_stems) / float(max(1, len(entry["stems"])))
            )
            score = (rep_sim * 0.75) + (centroid_overlap * 0.25)
            if common_stems >= 2:
                score += 0.05
            score = min(1.0, score)
            if score > best_score:
                best_score = score
                best_cluster_idx = cluster_idx
                best_common_stems = common_stems

        should_attach = best_cluster_idx is not None and best_score >= effective_threshold
        if should_attach and len(entry["stems"]) >= 3 and best_common_stems == 0 and best_score < (effective_threshold + 0.12):
            should_attach = False

        if should_attach and best_cluster_idx is not None:
            target_cluster = clusters_state[best_cluster_idx]
            target_cluster["members"].append(entry["idx"])
            target_cluster["stem_counter"].update(entry["stems"])
            target_cluster["token_counter"].update(entry["tokens"])
            rep_entry = entries[target_cluster["rep_idx"]]
            candidate_score = _hybrid_similarity(entry, rep_entry, method_normalized)
            if (
                len(entry["stems"]) > len(rep_entry["stems"])
                and candidate_score >= (effective_threshold + 0.08)
            ):
                target_cluster["rep_idx"] = entry["idx"]
        else:
            clusters_state.append(
                {
                    "members": [entry["idx"]],
                    "rep_idx": entry["idx"],
                    "stem_counter": Counter(entry["stems"]),
                    "token_counter": Counter(entry["tokens"]),
                }
            )

        if progress_callback and (pos % 50 == 0 or pos == unique_keywords_count):
            progress = 10 + int((pos / float(max(1, unique_keywords_count))) * 60.0)
            progress_callback(min(74, progress), f"Обработано ключей: {pos}/{unique_keywords_count}")

    if progress_callback:
        progress_callback(78, "Уточнение кластеров")

    merged = True
    while merged:
        merged = False
        for i in range(len(clusters_state)):
            if merged:
                break
            cluster_a = clusters_state[i]
            rep_a = entries[cluster_a["rep_idx"]]
            for j in range(i + 1, len(clusters_state)):
                cluster_b = clusters_state[j]
                rep_b = entries[cluster_b["rep_idx"]]
                similarity_checks += 1
                rep_sim = _hybrid_similarity(rep_a, rep_b, method_normalized)
                common_stems = len(rep_a["stems"] & rep_b["stems"])
                merge_threshold = effective_threshold + (0.12 if mode == "strict" else 0.08)
                can_merge = rep_sim >= merge_threshold and common_stems >= 1
                if not can_merge and rep_sim >= 0.92:
                    can_merge = True
                if not can_merge:
                    continue
                if len(cluster_a["members"]) > 25 and len(cluster_b["members"]) > 25 and rep_sim < 0.9:
                    continue
                cluster_a["members"].extend(cluster_b["members"])
                cluster_a["stem_counter"].update(cluster_b["stem_counter"])
                cluster_a["token_counter"].update(cluster_b["token_counter"])
                del clusters_state[j]
                merged = True
                break

    if progress_callback:
        progress_callback(85, "Расчет метрик кластеров")

    for cluster in clusters_state:
        cluster["members"] = sorted(set(cluster["members"]))

    clusters_state.sort(
        key=lambda cluster: (-len(cluster["members"]), min(cluster["members"]) if cluster["members"] else 0),
    )

    clusters: List[Dict[str, Any]] = []
    unclustered_keywords: List[str] = []
    flat_keywords_rows: List[Dict[str, Any]] = []
    cohesion_scores: List[float] = []
    low_confidence_keywords = 0

    for cluster_pos, cluster in enumerate(clusters_state, start=1):
        member_indexes = cluster["members"]
        rep_idx = cluster["rep_idx"]
        rep_entry = entries[rep_idx]
        top_tokens = [token for token, _ in cluster["stem_counter"].most_common(8)]

        edge_count = 0
        pair_count = 0
        sim_sum = 0.0
        keywords_detailed: List[Dict[str, Any]] = []
        for idx in member_indexes:
            entry = entries[idx]
            score_to_rep = _hybrid_similarity(entry, rep_entry, method_normalized)
            duplicates_count = int(normalized_freq.get(entry["normalized"], 1))
            if len(member_indexes) > 1 and score_to_rep < (effective_threshold + 0.05):
                low_confidence_keywords += 1
            keywords_detailed.append(
                {
                    "keyword": entry["display"],
                    "normalized_keyword": entry["normalized"],
                    "score_to_representative": round(score_to_rep, 4),
                    "duplicates_count": duplicates_count,
                }
            )

        keywords_detailed.sort(
            key=lambda item: (-float(item.get("score_to_representative", 0.0)), len(str(item.get("keyword", "")))),
        )

        for i in range(len(member_indexes)):
            for j in range(i + 1, len(member_indexes)):
                pair_count += 1
                similarity_checks += 1
                sim = _hybrid_similarity(entries[member_indexes[i]], entries[member_indexes[j]], method_normalized)
                sim_sum += sim
                if sim >= effective_threshold:
                    edge_count += 1

        max_edges = (len(member_indexes) * (len(member_indexes) - 1)) // 2
        density = (edge_count / float(max_edges)) if max_edges else 0.0
        avg_similarity = (sim_sum / float(pair_count)) if pair_count else 1.0
        cohesion_scores.append(avg_similarity)
        duplicates_in_cluster = sum(int(normalized_freq.get(entries[idx]["normalized"], 1)) for idx in member_indexes)
        keywords_in_cluster = [entries[idx]["display"] for idx in member_indexes]

        cluster_payload = {
            "cluster_id": cluster_pos,
            "size": len(member_indexes),
            "size_with_duplicates": duplicates_in_cluster,
            "representative": rep_entry["display"],
            "top_tokens": top_tokens,
            "edge_count": edge_count,
            "density": round(density, 4),
            "avg_similarity": round(avg_similarity, 4),
            "cohesion": _cluster_quality_label(avg_similarity),
            "keywords": keywords_in_cluster,
            "keywords_detailed": keywords_detailed,
        }
        clusters.append(cluster_payload)

        for row in keywords_detailed:
            flat_keywords_rows.append(
                {
                    "cluster_id": cluster_pos,
                    "cluster_size": len(member_indexes),
                    "representative": rep_entry["display"],
                    "keyword": row["keyword"],
                    "score_to_representative": row["score_to_representative"],
                    "duplicates_count": row["duplicates_count"],
                }
            )

        if len(member_indexes) == 1:
            unclustered_keywords.append(keywords_in_cluster[0])

    primary_clusters = [cluster for cluster in clusters if int(cluster.get("size", 0)) >= min_size]
    singleton_clusters = sum(1 for cluster in clusters if int(cluster.get("size", 0)) == 1)
    multi_keyword_clusters = max(0, len(clusters) - singleton_clusters)
    biggest_cluster_size = max([int(cluster.get("size", 0)) for cluster in clusters], default=0)
    avg_cluster_size = round(unique_keywords_count / float(len(clusters)), 2) if clusters else 0.0
    avg_cluster_cohesion = round(sum(cohesion_scores) / float(len(cohesion_scores)), 4) if cohesion_scores else 0.0
    high_quality_clusters = sum(1 for cluster in clusters if str(cluster.get("cohesion")) == "high")

    if progress_callback:
        progress_callback(100, "Кластеризация завершена")

    return {
        "task_type": "clusterizer",
        "url": "keywords://clusterizer",
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "results": {
            "engine": "keyword-clusterizer-v2",
            "settings": {
                "method": method_normalized,
                "clustering_mode": mode,
                "similarity_threshold_requested": round(requested_threshold, 4),
                "similarity_threshold": round(effective_threshold, 4),
                "similarity_threshold_pct": int(round(effective_threshold * 100)),
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
                "avg_cluster_cohesion": avg_cluster_cohesion,
                "high_quality_clusters": high_quality_clusters,
                "low_confidence_keywords": low_confidence_keywords,
                "comparisons_total": similarity_checks,
                "comparisons_total_potential": comparisons_total_potential,
            },
            "clusters": clusters,
            "primary_clusters": primary_clusters,
            "cluster_keywords_flat": flat_keywords_rows,
            "unclustered_keywords": unclustered_keywords,
        },
    }
