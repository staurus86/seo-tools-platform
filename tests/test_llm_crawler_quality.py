import unittest

from app.tools.llmCrawler.quality import (
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


if __name__ == "__main__":
    unittest.main()
