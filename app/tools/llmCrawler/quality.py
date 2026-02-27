"""Quality calibration and benchmark utilities for LLM crawler detectors."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


DEFAULT_QUALITY_THRESHOLDS: Dict[str, float] = {
    "page_type_accuracy": 0.80,
    "segmentation_pass_rate": 0.80,
    "retrieval_pass_rate": 0.75,
    "schema_recall_rate": 0.80,
    "citation_pass_rate": 0.75,
    "gate_pass_rate": 0.70,
}


PAGE_TYPE_CALIBRATION_PROFILES: Dict[str, Dict[str, Any]] = {
    "article": {"id": "article-v1", "multiplier": 1.0, "default_floor": 0.50},
    "docs": {"id": "docs-v1", "multiplier": 1.0, "default_floor": 0.50},
    "product": {"id": "product-v1", "multiplier": 1.0, "default_floor": 0.52},
    "service": {"id": "service-v1", "multiplier": 1.0, "default_floor": 0.50},
    "review": {"id": "review-v1", "multiplier": 1.0, "default_floor": 0.48},
    "homepage": {"id": "homepage-v1", "multiplier": 0.95, "default_floor": 0.42},
    "listing": {"id": "listing-v1", "multiplier": 0.92, "default_floor": 0.38},
    "category": {"id": "category-v1", "multiplier": 0.92, "default_floor": 0.40},
    "mixed": {"id": "mixed-v1", "multiplier": 0.90, "default_floor": 0.35},
    "news": {"id": "news-v1", "multiplier": 0.95, "default_floor": 0.42},
    "faq": {"id": "faq-v1", "multiplier": 0.95, "default_floor": 0.44},
    "event": {"id": "event-v1", "multiplier": 0.94, "default_floor": 0.42},
    "unknown": {"id": "unknown-v1", "multiplier": 0.85, "default_floor": 0.30},
}


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(fallback)


def _normalize_conf(value: Any) -> float | None:
    if value is None:
        return None
    raw = _safe_float(value, -1.0)
    if raw < 0:
        return None
    if raw > 1.0:
        raw = raw / 100.0
    return max(0.0, min(1.0, raw))


def _schema_type_count(result: Dict[str, Any]) -> int:
    structured = result.get("structured_data") or {}
    raw_types = list(structured.get("raw_types") or [])
    rendered_types = list(structured.get("rendered_types") or [])
    if raw_types or rendered_types:
        return len(set([str(x).strip() for x in raw_types + rendered_types if str(x).strip()]))
    raw = structured.get("raw") or {}
    rendered = structured.get("rendered") or {}
    raw_t = list(raw.get("types") or [])
    rendered_t = list(rendered.get("types") or [])
    return len(set([str(x).strip() for x in raw_t + rendered_t if str(x).strip()]))


def evaluate_benchmark_cases(cases: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(cases or [])
    if total == 0:
        return {
            "status": "not_evaluated",
            "reason": "no_cases",
            "total_cases": 0,
            "metrics": {},
            "failed_cases": [],
        }

    counters = {
        "page_type_correct": 0,
        "segmentation_pass": 0,
        "retrieval_pass": 0,
        "schema_pass": 0,
        "citation_pass": 0,
        "gate_pass": 0,
    }
    failed_cases: List[Dict[str, Any]] = []

    for case in cases:
        case_id = str(case.get("id") or f"case_{len(failed_cases)+1}")
        expected = case.get("expected") or {}
        result = case.get("result") or {}
        local_failures: List[str] = []

        expected_type = str(expected.get("page_type") or "").strip().lower()
        actual_type = str(
            result.get("page_type")
            or ((result.get("page_classification") or {}).get("type"))
            or "unknown"
        ).strip().lower()
        if expected_type and actual_type == expected_type:
            counters["page_type_correct"] += 1
        elif expected_type:
            local_failures.append(f"page_type expected={expected_type} actual={actual_type}")

        seg_conf = _normalize_conf((result.get("segmentation") or {}).get("segmentation_confidence"))
        seg_min = _safe_float(expected.get("min_seg_conf"), 0.0)
        if seg_conf is not None and seg_conf >= seg_min:
            counters["segmentation_pass"] += 1
        else:
            local_failures.append(f"segmentation_conf expected>={seg_min} actual={seg_conf}")

        retrieval_avg = _normalize_conf((result.get("retrieval") or {}).get("avg_score"))
        retrieval_min = _safe_float(expected.get("min_retrieval"), 0.0)
        if retrieval_avg is not None and retrieval_avg >= retrieval_min:
            counters["retrieval_pass"] += 1
        else:
            local_failures.append(f"retrieval_avg expected>={retrieval_min} actual={retrieval_avg}")

        schema_min = int(expected.get("min_schema_types") or 0)
        schema_count = _schema_type_count(result)
        if schema_count >= schema_min:
            counters["schema_pass"] += 1
        else:
            local_failures.append(f"schema_types expected>={schema_min} actual={schema_count}")

        citation_prob = _normalize_conf((result.get("citation_model") or {}).get("citation_probability"))
        citation_min = _safe_float(expected.get("min_citation"), 0.0)
        if citation_prob is not None and citation_prob >= citation_min:
            counters["citation_pass"] += 1
        else:
            local_failures.append(f"citation_probability expected>={citation_min} actual={citation_prob}")

        gate_status = str((result.get("quality_gates") or {}).get("status") or "")
        if gate_status in {"pass", "warn"}:
            counters["gate_pass"] += 1
        else:
            local_failures.append(f"quality_gates.status invalid={gate_status or 'missing'}")

        if local_failures:
            failed_cases.append({"id": case_id, "failures": local_failures[:8]})

    metrics = {
        "page_type_accuracy": round(counters["page_type_correct"] / total, 4),
        "segmentation_pass_rate": round(counters["segmentation_pass"] / total, 4),
        "retrieval_pass_rate": round(counters["retrieval_pass"] / total, 4),
        "schema_recall_rate": round(counters["schema_pass"] / total, 4),
        "citation_pass_rate": round(counters["citation_pass"] / total, 4),
        "gate_pass_rate": round(counters["gate_pass"] / total, 4),
    }
    return {
        "status": "evaluated",
        "reason": "ok",
        "total_cases": total,
        "metrics": metrics,
        "failed_cases": failed_cases[:20],
        "version": "benchmark-v1",
    }


def evaluate_quality_gate(
    benchmark: Dict[str, Any],
    thresholds: Dict[str, float] | None = None,
) -> Dict[str, Any]:
    thr = dict(DEFAULT_QUALITY_THRESHOLDS)
    thr.update(thresholds or {})
    metrics = benchmark.get("metrics") or {}
    checks: List[Dict[str, Any]] = []
    for key, expected in thr.items():
        value = _safe_float(metrics.get(key), 0.0)
        checks.append(
            {
                "metric": key,
                "value": round(value, 4),
                "threshold": round(float(expected), 4),
                "pass": bool(value >= float(expected)),
            }
        )
    passed = sum(1 for c in checks if c["pass"])
    total = len(checks)
    status = "pass" if passed == total else "warn" if passed >= max(1, total - 2) else "fail"
    return {
        "status": status,
        "passed": passed,
        "total": total,
        "checks": checks,
        "version": "quality-gate-v1",
    }


def calibration_profile_for_page_type(page_type: str) -> Dict[str, Any]:
    key = str(page_type or "unknown").strip().lower()
    return dict(PAGE_TYPE_CALIBRATION_PROFILES.get(key, PAGE_TYPE_CALIBRATION_PROFILES["unknown"]))


def calibrate_detector_layer(
    detectors: Dict[str, Any],
    *,
    page_type: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    calibrated = json.loads(json.dumps(detectors or {}))
    profile = calibration_profile_for_page_type(page_type)
    multiplier = _safe_float(profile.get("multiplier"), 1.0)
    floor = _safe_float(profile.get("default_floor"), 0.3)

    adjusted_count = 0
    downgraded_count = 0
    for key, payload in list(calibrated.items()):
        if key == "summary" or not isinstance(payload, dict):
            continue
        conf = _normalize_conf(payload.get("confidence"))
        if conf is None:
            continue
        adjusted = max(0.0, min(1.0, conf * multiplier))
        payload["confidence_raw"] = round(conf, 4)
        payload["confidence"] = round(adjusted, 4)
        adjusted_count += 1
        status = str(payload.get("status") or "")
        if status == "evaluated" and adjusted < floor:
            payload["status"] = "partial"
            evidence = payload.get("evidence") or []
            evidence.append(f"confidence<{floor} after calibration")
            payload["evidence"] = evidence[:8]
            downgraded_count += 1
        calibrated[key] = payload

    confidence_values = [
        _safe_float(v.get("confidence"), -1.0)
        for k, v in calibrated.items()
        if k != "summary" and isinstance(v, dict) and v.get("confidence") is not None
    ]
    confidence_values = [x for x in confidence_values if x >= 0]
    summary = calibrated.get("summary") if isinstance(calibrated.get("summary"), dict) else {}
    if summary:
        summary["avg_confidence_raw"] = summary.get("avg_confidence")
        summary["avg_confidence"] = (
            round(sum(confidence_values) / len(confidence_values), 4) if confidence_values else None
        )
        calibrated["summary"] = summary

    calibration = {
        "profile_id": str(profile.get("id") or "unknown"),
        "page_type": str(page_type or "unknown"),
        "multiplier": round(multiplier, 4),
        "default_floor": round(floor, 4),
        "adjusted_count": adjusted_count,
        "downgraded_count": downgraded_count,
        "version": "detector-calibration-v1",
    }
    return calibrated, calibration


def load_benchmark_cases(path: str | None = None) -> List[Dict[str, Any]]:
    target = Path(path) if path else (Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "llm_crawler_benchmark_cases.json")
    raw = target.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]


def run_quality_gate_from_file(path: str | None = None, thresholds: Dict[str, float] | None = None) -> Dict[str, Any]:
    cases = load_benchmark_cases(path)
    benchmark = evaluate_benchmark_cases(cases)
    gate = evaluate_quality_gate(benchmark, thresholds=thresholds)
    return {"benchmark": benchmark, "gate": gate}
