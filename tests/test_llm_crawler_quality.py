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
        self.assertGreaterEqual(float(metrics.get("page_type_accuracy") or 0), 0.80)
        self.assertGreaterEqual(float(metrics.get("segmentation_pass_rate") or 0), 0.80)
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
