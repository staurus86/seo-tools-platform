"""Basic keyword clustering by textual similarity."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import math
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

_INTENT_TOKENS: Dict[str, Set[str]] = {
    "commercial": {
        "куп", "цена", "стоим", "заказ", "заказать", "достав", "shop", "buy", "price", "discount", "deal", "sale",
    },
    "informational": {
        "как", "что", "почему", "гайд", "инструкц", "обзор", "сравнен", "review", "guide", "how", "what", "best",
    },
    "navigational": {
        "официальн", "сайт", "вход", "контакт", "login", "official", "site", "homepage",
    },
    "brand": {
        "iphone", "samsung", "apple", "xiaomi", "huawei", "google", "yandex", "ozon", "wildberries", "dns",
    },
}

_REP_NOISE_STEMS: Set[str] = {
    "скачать",
    "скачат",
    "бесплатн",
    "download",
    "watch",
    "pdf",
    "torrent",
    "youtube",
    "ютуб",
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


def _safe_frequency(value: Any) -> float:
    try:
        freq = float(str(value).replace(",", "."))
        if freq < 0:
            return 0.0
        return min(freq, 10**9)
    except Exception:
        pass
    return 1.0


def _build_cluster_label(top_tokens: List[str], intent: str) -> str:
    tokens = [str(token or "").strip() for token in top_tokens if str(token or "").strip()]
    if not tokens:
        return str(intent or "mixed")
    base = " / ".join(tokens[:3])
    if intent and intent not in {"mixed", "unknown"}:
        return f"{intent}: {base}"
    return base


def _detect_intent_from_stems(stems: Set[str]) -> str:
    if not stems:
        return "unknown"
    scores: Dict[str, int] = {}
    for label, tokens in _INTENT_TOKENS.items():
        score = 0
        for stem in stems:
            if stem in tokens:
                score += 2
                continue
            if any(stem.startswith(token) or token.startswith(stem) for token in tokens):
                score += 1
        if score > 0:
            scores[label] = score
    if not scores:
        return "mixed"
    best_label = sorted(scores.items(), key=lambda item: (-item[1], item[0]))[0][0]
    return best_label


def _representative_penalty(entry: Dict[str, Any]) -> float:
    token_count = len(entry.get("tokens") or set())
    stems = set(entry.get("stems") or set())
    display = str(entry.get("display", "") or "")
    char_len = len(display)
    penalty = 0.0
    if token_count <= 1:
        penalty += 0.28
    elif token_count >= 8:
        penalty += min(0.45, 0.06 * float(token_count - 7))
    if char_len >= 70:
        penalty += min(0.35, (char_len - 70) / 90.0)
    if stems & _REP_NOISE_STEMS and token_count >= 4:
        penalty += 0.08
    return penalty


def run_keyword_clusterizer(
    *,
    keywords: Optional[Sequence[str]] = None,
    keyword_rows: Optional[Sequence[Dict[str, Any]]] = None,
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
    normalized_occurrence: Counter[str] = Counter()
    normalized_demand: Counter[str] = Counter()
    total_input_keywords = 0
    total_input_demand = 0.0

    input_rows: List[Dict[str, Any]] = []
    for raw_keyword in keywords or []:
        input_rows.append({"keyword": raw_keyword, "frequency": 1.0})
    for row in keyword_rows or []:
        if isinstance(row, dict):
            input_rows.append({"keyword": row.get("keyword", ""), "frequency": row.get("frequency", 1.0)})

    for raw_row in input_rows:
        clean_keyword = str(raw_row.get("keyword", "") or "").strip()
        if not clean_keyword:
            continue
        freq = _safe_frequency(raw_row.get("frequency", 1.0))
        total_input_keywords += 1
        total_input_demand += freq
        normalized = _normalize_keyword(clean_keyword)
        if not normalized:
            continue
        normalized_occurrence[normalized] += 1
        normalized_demand[normalized] += freq
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
                "frequency": float(normalized_demand.get(normalized, 1.0)),
            }
        )
    entries_by_normalized: Dict[str, Dict[str, Any]] = {entry["normalized"]: entry for entry in entries}

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
    unique_demand_total = float(sum(normalized_demand.values()))

    for cluster_pos, cluster in enumerate(clusters_state, start=1):
        member_indexes = cluster["members"]
        rep_idx = cluster["rep_idx"]
        rep_entry = entries[rep_idx]
        top_tokens = [token for token, _ in cluster["stem_counter"].most_common(8)]
        core_stems = set(top_tokens[:4])
        cluster_stems: Set[str] = set()
        cluster_demand = 0.0

        edge_count = 0
        pair_count = 0
        sim_sum = 0.0
        keywords_detailed: List[Dict[str, Any]] = []
        for idx in member_indexes:
            entry = entries[idx]
            cluster_stems.update(entry["stems"])
            score_to_rep = _hybrid_similarity(entry, rep_entry, method_normalized)
            duplicates_count = int(normalized_occurrence.get(entry["normalized"], 1))
            keyword_demand = float(normalized_demand.get(entry["normalized"], 1.0))
            core_overlap = (
                len(entry["stems"] & core_stems) / float(max(1, len(core_stems)))
                if core_stems
                else 0.0
            )
            cluster_support = (
                sum(int(cluster["stem_counter"].get(stem, 0)) for stem in entry["stems"])
                / float(max(1, len(member_indexes) * max(1, len(entry["stems"]))))
            )
            cluster_demand += keyword_demand
            if len(member_indexes) > 1 and score_to_rep < (effective_threshold + 0.05):
                low_confidence_keywords += 1
            keywords_detailed.append(
                {
                    "keyword": entry["display"],
                    "normalized_keyword": entry["normalized"],
                    "score_to_representative": round(score_to_rep, 4),
                    "duplicates_count": duplicates_count,
                    "demand": round(keyword_demand, 4),
                    "token_count": int(max(1, len(entry["tokens"]))),
                    "core_overlap": round(core_overlap, 4),
                    "cluster_support": round(cluster_support, 4),
                }
            )

        keywords_detailed.sort(
            key=lambda item: (-float(item.get("score_to_representative", 0.0)), len(str(item.get("keyword", "")))),
        )

        if keywords_detailed:
            max_demand = max(float(item.get("demand", 0.0)) for item in keywords_detailed)
            demand_log_base = math.log1p(max_demand) if max_demand > 0 else 0.0
            representative_rank: List[Dict[str, Any]] = []
            for item in keywords_detailed:
                keyword = str(item.get("keyword", ""))
                demand = float(item.get("demand", 0.0))
                demand_norm = (math.log1p(demand) / demand_log_base) if demand_log_base > 0 else 0.0
                similarity_hint = float(item.get("score_to_representative", 0.0))
                core_overlap = float(item.get("core_overlap", 0.0))
                cluster_support = float(item.get("cluster_support", 0.0))
                token_count = int(item.get("token_count", 1))
                token_shape_bonus = 0.0
                if 2 <= token_count <= 6:
                    token_shape_bonus = 0.07
                elif token_count >= 9:
                    token_shape_bonus = -0.05
                entry_meta = entries_by_normalized.get(str(item.get("normalized_keyword", "")), {})
                rep_score = (
                    (demand_norm * 0.55)
                    + (core_overlap * 0.2)
                    + (cluster_support * 0.1)
                    + (similarity_hint * 0.08)
                    + token_shape_bonus
                    - _representative_penalty(entry_meta)
                )
                representative_rank.append(
                    {
                        "item": item,
                        "rep_score": rep_score,
                        "demand": demand,
                        "token_count": token_count,
                        "keyword_len": len(keyword),
                    }
                )
            representative_rank.sort(
                key=lambda row: (
                    -float(row["rep_score"]),
                    -float(row["demand"]),
                    int(row["token_count"]),
                    int(row["keyword_len"]),
                ),
            )
            representative_clean = str(representative_rank[0]["item"].get("keyword", rep_entry["display"]))
        else:
            representative_clean = rep_entry["display"]

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
        duplicates_in_cluster = sum(int(normalized_occurrence.get(entries[idx]["normalized"], 1)) for idx in member_indexes)
        keywords_in_cluster = [entries[idx]["display"] for idx in member_indexes]
        demand_share_pct = (cluster_demand / unique_demand_total * 100.0) if unique_demand_total > 0 else 0.0
        intent = _detect_intent_from_stems(cluster_stems)
        cluster_priority = (
            (cluster_demand * 0.55)
            + (len(member_indexes) * 0.25)
            + (avg_similarity * 100.0 * 0.2)
        )
        cluster_label = _build_cluster_label(top_tokens, intent)

        cluster_payload = {
            "cluster_id": cluster_pos,
            "size": len(member_indexes),
            "size_with_duplicates": duplicates_in_cluster,
            "cluster_label": cluster_label,
            "representative": representative_clean,
            "top_tokens": top_tokens,
            "edge_count": edge_count,
            "density": round(density, 4),
            "avg_similarity": round(avg_similarity, 4),
            "cohesion": _cluster_quality_label(avg_similarity),
            "intent": intent,
            "demand_total": round(cluster_demand, 4),
            "demand_share_pct": round(demand_share_pct, 2),
            "priority_score": round(cluster_priority, 2),
            "keywords": keywords_in_cluster,
            "keywords_detailed": keywords_detailed,
        }
        clusters.append(cluster_payload)

        for row in keywords_detailed:
            flat_keywords_rows.append(
                {
                    "cluster_id": cluster_pos,
                    "cluster_size": len(member_indexes),
                    "representative": representative_clean,
                    "keyword": row["keyword"],
                    "score_to_representative": row["score_to_representative"],
                    "duplicates_count": row["duplicates_count"],
                    "demand": row["demand"],
                }
            )

        if len(member_indexes) == 1:
            unclustered_keywords.append(keywords_in_cluster[0])

    clusters.sort(
        key=lambda cluster: (
            -float(cluster.get("demand_total", 0.0)),
            -int(cluster.get("size", 0)),
            -float(cluster.get("avg_similarity", 0.0)),
            str(cluster.get("representative", "")),
        )
    )
    cluster_id_map: Dict[int, int] = {}
    for new_idx, cluster in enumerate(clusters, start=1):
        old_idx = int(cluster.get("cluster_id", new_idx))
        cluster_id_map[old_idx] = new_idx
        cluster["cluster_id"] = new_idx
    for row in flat_keywords_rows:
        old_idx = int(row.get("cluster_id", 0) or 0)
        if old_idx in cluster_id_map:
            row["cluster_id"] = cluster_id_map[old_idx]

    primary_clusters = [cluster for cluster in clusters if int(cluster.get("size", 0)) >= min_size]
    singleton_clusters = sum(1 for cluster in clusters if int(cluster.get("size", 0)) == 1)
    multi_keyword_clusters = max(0, len(clusters) - singleton_clusters)
    biggest_cluster_size = max([int(cluster.get("size", 0)) for cluster in clusters], default=0)
    avg_cluster_size = round(unique_keywords_count / float(len(clusters)), 2) if clusters else 0.0
    avg_cluster_cohesion = round(sum(cohesion_scores) / float(len(cohesion_scores)), 4) if cohesion_scores else 0.0
    high_quality_clusters = sum(1 for cluster in clusters if str(cluster.get("cohesion")) == "high")
    singleton_demand_total = sum(float(cluster.get("demand_total", 0.0)) for cluster in clusters if int(cluster.get("size", 0)) == 1)
    primary_clusters_demand_total = sum(float(cluster.get("demand_total", 0.0)) for cluster in primary_clusters)
    singleton_demand_share_pct = (
        round((singleton_demand_total / unique_demand_total) * 100.0, 2)
        if unique_demand_total > 0
        else 0.0
    )
    primary_demand_share_pct = (
        round((primary_clusters_demand_total / unique_demand_total) * 100.0, 2)
        if unique_demand_total > 0
        else 0.0
    )
    top_cluster_demand_share_pct = round(float(clusters[0].get("demand_share_pct", 0.0)), 2) if clusters else 0.0
    intent_distribution: Dict[str, int] = Counter(str(cluster.get("intent") or "mixed") for cluster in clusters)

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
                "input_demand_total": round(total_input_demand, 4),
                "unique_demand_total": round(unique_demand_total, 4),
                "clusters_total": len(clusters),
                "primary_clusters_total": len(primary_clusters),
                "multi_keyword_clusters": multi_keyword_clusters,
                "singleton_clusters": singleton_clusters,
                "biggest_cluster_size": biggest_cluster_size,
                "avg_cluster_size": avg_cluster_size,
                "avg_cluster_cohesion": avg_cluster_cohesion,
                "high_quality_clusters": high_quality_clusters,
                "low_confidence_keywords": low_confidence_keywords,
                "primary_demand_share_pct": primary_demand_share_pct,
                "singleton_demand_share_pct": singleton_demand_share_pct,
                "top_cluster_demand_share_pct": top_cluster_demand_share_pct,
                "comparisons_total": similarity_checks,
                "comparisons_total_potential": comparisons_total_potential,
            },
            "intent_distribution": dict(intent_distribution),
            "clusters": clusters,
            "primary_clusters": primary_clusters,
            "cluster_keywords_flat": flat_keywords_rows,
            "unclustered_keywords": unclustered_keywords,
        },
    }
