import unittest

from app.tools.llmCrawler.quality import (
    build_runtime_quality_profile,
    calibrate_detector_layer,
    evaluate_benchmark_cases,
    evaluate_quality_gate,
    load_benchmark_cases,
    run_quality_gate_from_file,
)


class LlmCrawlerQualityTests(unittest.TestCase):
    def test_benchmark_fixture_gate(self):
        cases = load_benchmark_cases()
        report = evaluate_benchmark_cases(cases)
        self.assertEqual(report.get("status"), "evaluated")
        metrics = report.get("metrics") or {}
        applicable = report.get("applicable_cases") or {}
        self.assertGreaterEqual(float(metrics.get("page_type_accuracy") or 0), 0.80)
        self.assertGreaterEqual(float(metrics.get("segmentation_pass_rate") or 0), 0.80)
        self.assertGreaterEqual(float(metrics.get("fallback_contract_pass_rate") or 0), 0.80)
        self.assertGreaterEqual(float(metrics.get("detector_coverage_pass_rate") or 0), 0.70)
        self.assertGreaterEqual(float(metrics.get("analysis_quality_pass_rate") or 0), 0.70)
        self.assertGreaterEqual(float(metrics.get("challenge_eval_pass_rate") or 0), 0.80)
        self.assertGreaterEqual(int(applicable.get("fallback_contract") or 0), 1)
        self.assertGreaterEqual(int(applicable.get("detector_coverage") or 0), 1)
        self.assertGreaterEqual(int(applicable.get("analysis_quality") or 0), 1)
        self.assertGreaterEqual(int(applicable.get("challenge_eval") or 0), 1)
        gate = evaluate_quality_gate(report)
        self.assertIn(gate.get("status"), {"pass", "warn"})

    def test_calibration_downgrades_low_conf(self):
        detectors = {
            "segmentation": {"status": "evaluated", "confidence": 0.33, "version": "v1", "evidence": []},
            "retrieval": {"status": "evaluated", "confidence": 0.55, "version": "v1", "evidence": []},
            "summary": {"avg_confidence": 0.44},
        }
        calibrated, info = calibrate_detector_layer(detectors, page_type="article")
        self.assertIn("profile_id", info)
        self.assertEqual(str(calibrated["segmentation"]["status"]), "partial")
        self.assertLess(float(calibrated["segmentation"]["confidence"]), 0.5)

    def test_calibration_uses_detector_evidence_adjustments(self):
        detectors = {
            "page_classification": {
                "status": "evaluated",
                "confidence": 0.81,
                "version": "v1",
                "evidence": ["signals=0", "type=unknown"],
            },
            "structured_data": {
                "status": "evaluated",
                "confidence": 0.74,
                "version": "v1",
                "evidence": ["raw_count=0", "rendered_count=2"],
            },
            "challenge_waf": {
                "status": "evaluated",
                "confidence": 0.66,
                "version": "v1",
                "evidence": ["status=suspected", "risk=low", "reasons=0"],
            },
            "summary": {"avg_confidence": 0.74},
        }
        calibrated, _ = calibrate_detector_layer(detectors, page_type="unknown")
        self.assertLess(float(calibrated["page_classification"]["confidence"]), 0.81)
        self.assertLess(float(calibrated["structured_data"]["confidence"]), 0.74)
        self.assertLess(float(calibrated["challenge_waf"]["confidence"]), 0.66)
        self.assertTrue(any("calibration_note=" in str(x) for x in (calibrated["page_classification"].get("evidence") or [])))

    def test_run_quality_gate_from_file(self):
        payload = run_quality_gate_from_file()
        self.assertIn("benchmark", payload)
        self.assertIn("gate", payload)
        self.assertIn(payload["gate"].get("status"), {"pass", "warn", "fail"})

    def test_runtime_quality_profile(self):
        profile = build_runtime_quality_profile(
            page_type="article",
            detectors={"summary": {"coverage_ratio": 0.85, "avg_confidence": 0.76}},
            detector_calibration={"profile_id": "article-v1", "downgraded_count": 0},
            quality_gates={"status": "pass"},
            retrieval={"retrieval_confidence": 0.71, "score_stddev": 0.08},
            citation_model={"calibration_error_estimate": 0.03},
        )
        self.assertIn(profile.get("status"), {"stable", "warning", "unstable"})
        self.assertEqual(profile.get("profile_id"), "article-v1")
        self.assertTrue(isinstance(profile.get("drift_flags"), list))


if __name__ == "__main__":
    unittest.main()
