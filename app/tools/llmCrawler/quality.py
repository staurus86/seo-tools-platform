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
    "fallback_contract_pass_rate": 0.85,
    "detector_coverage_pass_rate": 0.70,
    "analysis_quality_pass_rate": 0.70,
    "challenge_eval_pass_rate": 0.80,
}


PAGE_TYPE_CALIBRATION_PROFILES: Dict[str, Dict[str, Any]] = {
    "article": {"id": "article-v1", "multiplier": 1.0, "default_floor": 0.50},
    "docs": {"id": "docs-v1", "multiplier": 1.0, "default_floor": 0.50},
    "product": {"id": "product-v1", "multiplier": 1.0, "default_floor": 0.52},
    "service": {"id": "service-v1", "multiplier": 1.0, "default_floor": 0.50},
    "review": {"id": "review-v1", "multiplier": 1.0, "default_floor": 0.48},
    "marketplace": {"id": "marketplace-v1", "multiplier": 0.94, "default_floor": 0.40},
    "homepage": {"id": "homepage-v1", "multiplier": 0.95, "default_floor": 0.42},
    "listing": {"id": "listing-v1", "multiplier": 0.92, "default_floor": 0.38},
    "category": {"id": "category-v1", "multiplier": 0.92, "default_floor": 0.40},
    "mixed": {"id": "mixed-v1", "multiplier": 0.90, "default_floor": 0.35},
    "news": {"id": "news-v1", "multiplier": 0.95, "default_floor": 0.42},
    "faq": {"id": "faq-v1", "multiplier": 0.95, "default_floor": 0.44},
    "event": {"id": "event-v1", "multiplier": 0.94, "default_floor": 0.42},
    "unknown": {"id": "unknown-v1", "multiplier": 0.85, "default_floor": 0.30},
}

DETECTOR_CALIBRATION_BASE: Dict[str, Dict[str, float]] = {
    "page_classification": {"multiplier": 1.0, "floor": 0.42},
    "structured_data": {"multiplier": 1.0, "floor": 0.35},
    "entities": {"multiplier": 1.0, "floor": 0.34},
    "challenge_waf": {"multiplier": 1.0, "floor": 0.34},
    "js_dependency": {"multiplier": 1.0, "floor": 0.30},
}

PAGE_TYPE_DETECTOR_OVERRIDES: Dict[str, Dict[str, Dict[str, float]]] = {
    "listing": {
        "page_classification": {"multiplier": 0.92, "floor": 0.36},
        "js_dependency": {"multiplier": 0.94, "floor": 0.28},
    },
    "marketplace": {
        "page_classification": {"multiplier": 0.93, "floor": 0.38},
        "structured_data": {"multiplier": 0.95, "floor": 0.34},
        "js_dependency": {"multiplier": 0.95, "floor": 0.29},
    },
    "homepage": {
        "page_classification": {"multiplier": 0.94, "floor": 0.36},
    },
    "docs": {
        "entities": {"multiplier": 0.95, "floor": 0.30},
    },
    "unknown": {
        "page_classification": {"multiplier": 0.90, "floor": 0.30},
        "challenge_waf": {"multiplier": 0.95, "floor": 0.30},
    },
}


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(fallback)


def _safe_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(fallback)


def _normalize_conf(value: Any) -> float | None:
    if value is None:
        return None
    raw = _safe_float(value, -1.0)
    if raw < 0:
        return None
    if raw > 1.0:
        raw = raw / 100.0
    return max(0.0, min(1.0, raw))


def _evidence_kv(evidence: List[str] | None) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in (evidence or []):
        raw = str(item or "").strip()
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip().lower()
        value = value.strip()
        if not key:
            continue
        out[key] = value
    return out


def _detector_profile(page_type: str, detector_key: str) -> Dict[str, float]:
    base = dict(DETECTOR_CALIBRATION_BASE.get(detector_key, {"multiplier": 1.0, "floor": 0.0}))
    override = (PAGE_TYPE_DETECTOR_OVERRIDES.get(page_type, {}) or {}).get(detector_key, {})
    if override:
        base.update({k: float(v) for k, v in override.items() if isinstance(v, (int, float))})
    return {
        "multiplier": float(base.get("multiplier", 1.0)),
        "floor": float(base.get("floor", 0.0)),
    }


def _apply_detector_signal_adjustments(
    detector_key: str,
    adjusted: float,
    status: str,
    evidence: Dict[str, str],
) -> tuple[float, List[str]]:
    notes: List[str] = []
    out = float(adjusted)
    if detector_key == "page_classification":
        signals = _safe_int(evidence.get("signals"), -1)
        if signals == 0:
            out *= 0.78
            notes.append("signals=0")
        elif signals == 1:
            out *= 0.90
            notes.append("signals=1")
    elif detector_key == "structured_data":
        raw_count = _safe_int(evidence.get("raw_count"), 0)
        rendered_count = _safe_int(evidence.get("rendered_count"), 0)
        if raw_count == 0 and rendered_count > 0:
            out *= 0.86
            notes.append("rendered_only_schema")
    elif detector_key == "entities":
        entity_total = _safe_int(evidence.get("entity_total"), 0)
        if entity_total == 0:
            out *= 0.72
            notes.append("entity_total=0")
        elif entity_total < 2:
            out *= 0.88
            notes.append("entity_total<2")
    elif detector_key == "js_dependency":
        coverage = _safe_float(evidence.get("coverage_ratio"), -1.0)
        if 0.0 <= coverage <= 1.0:
            out = (out * 0.65) + (coverage * 0.35)
            notes.append("coverage_blend")
        risk = str(evidence.get("risk") or "").lower()
        if risk in {"high", "critical"}:
            out *= 0.90
            notes.append(f"risk={risk}")
    elif detector_key == "challenge_waf":
        risk = str(evidence.get("risk") or "").lower()
        reasons = _safe_int(evidence.get("reasons"), 0)
        if status == "evaluated" and risk == "low" and reasons == 0:
            out = min(out, 0.65)
            notes.append("low_risk_no_evidence_cap")
        if status == "evaluated" and str(evidence.get("status") or "").lower() == "suspected":
            out *= 0.90
            notes.append("suspected_downgrade")
    return max(0.0, min(1.0, out)), notes


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


def _coalesce_str(values: List[Any]) -> str:
    for value in values:
        txt = str(value or "").strip()
        if txt:
            return txt
    return ""


def _validate_status_contract(result: Dict[str, Any]) -> tuple[bool, List[str]]:
    ai_understanding = result.get("ai_understanding") or {}
    llm_sim = result.get("llm_simulation") or {}
    citation_model = result.get("citation_model") or {}
    preview = result.get("ai_answer_preview") or {}
    checks = [
        (
            "topic",
            _coalesce_str([result.get("topic_status"), ai_understanding.get("topic_status")]).lower(),
            _coalesce_str([result.get("topic_reason"), ai_understanding.get("topic_reason")]),
        ),
        (
            "summary",
            _coalesce_str([result.get("summary_status"), llm_sim.get("status")]).lower(),
            _coalesce_str([result.get("summary_reason"), llm_sim.get("reason")]),
        ),
        (
            "citation",
            _coalesce_str([result.get("citation_status"), citation_model.get("status")]).lower(),
            _coalesce_str([result.get("citation_reason"), citation_model.get("reason")]),
        ),
        (
            "preview",
            _coalesce_str([result.get("preview_status"), preview.get("status")]).lower(),
            _coalesce_str([result.get("preview_reason"), preview.get("reason")]),
        ),
        (
            "content_clarity",
            _coalesce_str([result.get("content_clarity_status"), ai_understanding.get("content_clarity_status")]).lower(),
            _coalesce_str([result.get("content_clarity_reason"), ai_understanding.get("content_clarity_reason")]),
        ),
    ]
    issues: List[str] = []
    allowed_status = {"evaluated", "not_evaluated", "partial"}
    for module, status, reason in checks:
        if not status:
            issues.append(f"{module}:missing_status")
            continue
        if status not in allowed_status:
            issues.append(f"{module}:invalid_status={status}")
        if not reason:
            issues.append(f"{module}:missing_reason")
        if status == "not_evaluated" and reason.lower() in {"", "unknown", "not_evaluated"}:
            issues.append(f"{module}:non_explanatory_reason")
    return (len(issues) == 0), issues[:10]


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
        "fallback_contract_pass": 0,
        "detector_coverage_pass": 0,
        "analysis_quality_pass": 0,
        "challenge_eval_pass": 0,
    }
    applicable = {
        "page_type": 0,
        "segmentation": 0,
        "retrieval": 0,
        "schema": 0,
        "citation": 0,
        "gate": 0,
        "fallback_contract": 0,
        "detector_coverage": 0,
        "analysis_quality": 0,
        "challenge_eval": 0,
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
        if expected_type:
            applicable["page_type"] += 1
            if actual_type == expected_type:
                counters["page_type_correct"] += 1
            else:
                local_failures.append(f"page_type expected={expected_type} actual={actual_type}")

        seg_conf = _normalize_conf((result.get("segmentation") or {}).get("segmentation_confidence"))
        if expected.get("min_seg_conf") is not None:
            seg_min = _safe_float(expected.get("min_seg_conf"), 0.0)
            applicable["segmentation"] += 1
            if seg_conf is not None and seg_conf >= seg_min:
                counters["segmentation_pass"] += 1
            else:
                local_failures.append(f"segmentation_conf expected>={seg_min} actual={seg_conf}")

        retrieval_avg = _normalize_conf((result.get("retrieval") or {}).get("avg_score"))
        if expected.get("min_retrieval") is not None:
            retrieval_min = _safe_float(expected.get("min_retrieval"), 0.0)
            applicable["retrieval"] += 1
            if retrieval_avg is not None and retrieval_avg >= retrieval_min:
                counters["retrieval_pass"] += 1
            else:
                local_failures.append(f"retrieval_avg expected>={retrieval_min} actual={retrieval_avg}")

        if expected.get("min_schema_types") is not None:
            schema_min = int(expected.get("min_schema_types") or 0)
            schema_count = _schema_type_count(result)
            applicable["schema"] += 1
            if schema_count >= schema_min:
                counters["schema_pass"] += 1
            else:
                local_failures.append(f"schema_types expected>={schema_min} actual={schema_count}")

        citation_prob = _normalize_conf((result.get("citation_model") or {}).get("citation_probability"))
        if expected.get("min_citation") is not None:
            citation_min = _safe_float(expected.get("min_citation"), 0.0)
            applicable["citation"] += 1
            if citation_prob is not None and citation_prob >= citation_min:
                counters["citation_pass"] += 1
            else:
                local_failures.append(f"citation_probability expected>={citation_min} actual={citation_prob}")

        gate_status = str((result.get("quality_gates") or {}).get("status") or "")
        applicable["gate"] += 1
        if gate_status in {"pass", "warn"}:
            counters["gate_pass"] += 1
        else:
            local_failures.append(f"quality_gates.status invalid={gate_status or 'missing'}")

        if bool(expected.get("require_status_contract", False)):
            applicable["fallback_contract"] += 1
            contract_ok, contract_issues = _validate_status_contract(result)
            if contract_ok:
                counters["fallback_contract_pass"] += 1
            else:
                local_failures.append(f"status_contract {', '.join(contract_issues[:4])}")

        if expected.get("min_detector_coverage") is not None:
            min_cov = _safe_float(expected.get("min_detector_coverage"), 0.0)
            det_cov = _safe_float(((result.get("detectors") or {}).get("summary") or {}).get("coverage_ratio"), -1.0)
            applicable["detector_coverage"] += 1
            if det_cov >= min_cov:
                counters["detector_coverage_pass"] += 1
            else:
                local_failures.append(f"detector_coverage expected>={min_cov} actual={det_cov}")

        if expected.get("min_analysis_quality") is not None:
            min_analysis = _safe_float(expected.get("min_analysis_quality"), 0.0)
            analysis_score = _safe_float((result.get("analysis_quality") or {}).get("analysis_quality_score"), -1.0)
            applicable["analysis_quality"] += 1
            if analysis_score >= min_analysis:
                counters["analysis_quality_pass"] += 1
            else:
                local_failures.append(f"analysis_quality expected>={min_analysis} actual={analysis_score}")

        if bool(expected.get("require_challenge_evaluated", False)):
            challenge_status = str((result.get("challenge") or {}).get("status") or "").strip().lower()
            applicable["challenge_eval"] += 1
            if challenge_status in {"none", "suspected", "detected"}:
                counters["challenge_eval_pass"] += 1
            else:
                local_failures.append(f"challenge_status invalid={challenge_status or 'missing'}")

        if local_failures:
            failed_cases.append({"id": case_id, "failures": local_failures[:8]})

    metrics = {
        "page_type_accuracy": round(counters["page_type_correct"] / max(1, applicable["page_type"]), 4),
        "segmentation_pass_rate": round(counters["segmentation_pass"] / max(1, applicable["segmentation"]), 4),
        "retrieval_pass_rate": round(counters["retrieval_pass"] / max(1, applicable["retrieval"]), 4),
        "schema_recall_rate": round(counters["schema_pass"] / max(1, applicable["schema"]), 4),
        "citation_pass_rate": round(counters["citation_pass"] / max(1, applicable["citation"]), 4),
        "gate_pass_rate": round(counters["gate_pass"] / max(1, applicable["gate"]), 4),
        "fallback_contract_pass_rate": round(counters["fallback_contract_pass"] / max(1, applicable["fallback_contract"]), 4),
        "detector_coverage_pass_rate": round(counters["detector_coverage_pass"] / max(1, applicable["detector_coverage"]), 4),
        "analysis_quality_pass_rate": round(counters["analysis_quality_pass"] / max(1, applicable["analysis_quality"]), 4),
        "challenge_eval_pass_rate": round(counters["challenge_eval_pass"] / max(1, applicable["challenge_eval"]), 4),
    }
    return {
        "status": "evaluated",
        "reason": "ok",
        "total_cases": total,
        "applicable_cases": applicable,
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
    page_type_key = str(page_type or "unknown").strip().lower()
    profile = calibration_profile_for_page_type(page_type_key)
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
        detector_cfg = _detector_profile(page_type_key, key)
        detector_mult = _safe_float(detector_cfg.get("multiplier"), 1.0)
        detector_floor = _safe_float(detector_cfg.get("floor"), floor)
        if detector_floor <= 0.0:
            detector_floor = floor
        evidence_map = _evidence_kv(payload.get("evidence") or [])
        adjusted = max(0.0, min(1.0, conf * multiplier * detector_mult))
        adjusted, signal_notes = _apply_detector_signal_adjustments(
            key,
            adjusted,
            str(payload.get("status") or ""),
            evidence_map,
        )
        payload["confidence_raw"] = round(conf, 4)
        payload["confidence"] = round(adjusted, 4)
        adjusted_count += 1
        status = str(payload.get("status") or "")
        if status == "evaluated" and adjusted < detector_floor:
            payload["status"] = "partial"
            evidence = payload.get("evidence") or []
            evidence.append(f"confidence<{detector_floor} after calibration")
            payload["evidence"] = evidence[:8]
            downgraded_count += 1
        if signal_notes:
            evidence = payload.get("evidence") or []
            evidence.extend([f"calibration_note={note}" for note in signal_notes[:3]])
            payload["evidence"] = evidence[:8]
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
        "page_type": page_type_key,
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


def build_runtime_quality_profile(
    *,
    page_type: str,
    detectors: Dict[str, Any],
    detector_calibration: Dict[str, Any],
    quality_gates: Dict[str, Any],
    retrieval: Dict[str, Any],
    citation_model: Dict[str, Any],
) -> Dict[str, Any]:
    summary = detectors.get("summary") or {}
    coverage = _safe_float(summary.get("coverage_ratio"), 0.0)
    avg_conf = _safe_float(summary.get("avg_confidence"), 0.0)
    downgraded = int(detector_calibration.get("downgraded_count") or 0)
    retrieval_std = _safe_float(retrieval.get("score_stddev"), 0.0)
    retrieval_conf = _safe_float(retrieval.get("retrieval_confidence"), 0.0)
    citation_cal_error = _safe_float(citation_model.get("calibration_error_estimate"), 0.0)
    gates_status = str(quality_gates.get("status") or "unknown")

    drift_flags: List[str] = []
    if coverage < 0.7:
        drift_flags.append("low_detector_coverage")
    if avg_conf < 0.5:
        drift_flags.append("low_average_detector_confidence")
    if downgraded >= 3:
        drift_flags.append("high_calibration_downgrades")
    if retrieval_std > 0.22:
        drift_flags.append("retrieval_high_variance")
    if retrieval_conf < 0.45:
        drift_flags.append("retrieval_low_confidence")
    if citation_cal_error > 0.08:
        drift_flags.append("citation_calibration_instability")
    if gates_status == "fail":
        drift_flags.append("quality_gates_failed")

    status = "stable"
    if drift_flags:
        status = "warning" if len(drift_flags) <= 2 else "unstable"

    return {
        "status": status,
        "page_type": str(page_type or "unknown"),
        "profile_id": str(detector_calibration.get("profile_id") or "unknown"),
        "coverage_ratio": round(coverage, 4),
        "avg_detector_confidence": round(avg_conf, 4),
        "retrieval_confidence": round(retrieval_conf, 4),
        "retrieval_variance": round(retrieval_std, 4),
        "citation_calibration_error": round(citation_cal_error, 4),
        "drift_flags": drift_flags[:8],
        "quality_gates_status": gates_status,
        "version": "runtime-quality-profile-v1",
    }
