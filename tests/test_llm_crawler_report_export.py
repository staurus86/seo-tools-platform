import unittest

from app.tools.llmCrawler.report_docx import build_docx_v2


class LlmCrawlerReportExportTests(unittest.TestCase):
    def test_docx_build_with_v3_fields(self):
        job = {
            "result": {
                "final_url": "https://example.com/page",
                "score": {"total": 72, "top_issues": ["Missing schema"]},
                "projected_score_after_fixes": 88,
                "projected_score_waterfall": {
                    "baseline": 72,
                    "steps": [{"label": "Schema coverage", "delta": 12, "value": 84}],
                    "target": 88,
                },
                "citation_probability": 68,
                "eeat_score": {"status": "not_evaluated", "reason": "feature_disabled", "score": None},
                "trust_signal_score": 40,
                "ai_understanding": {
                    "topic": "Industrial vacuum meter calibration",
                    "score": 78,
                    "topic_confidence": 74,
                    "topic_fallback_used": True,
                    "content_clarity_status": "evaluated",
                    "content_clarity": 71,
                    "entities": ["Vacuum meter"],
                },
                "discoverability": {"discoverability_score": 61, "click_depth_estimate": 3},
                "ai_answer_preview": {
                    "question": "What is this page about?",
                    "answer": "The page explains vacuum meter calibration and maintenance.",
                    "confidence": 66,
                },
                "nojs": {
                    "content": {
                        "main_text_length": 1800,
                        "main_text_preview": "Vacuum meter guide",
                        "main_content_ratio": 0.71,
                        "boilerplate_ratio": 0.29,
                        "chunks": [{"idx": 1, "text": "Chunk text 1"}, {"idx": 2, "text": "Chunk text 2"}],
                    },
                    "schema": {"coverage_score": 25, "jsonld_types": []},
                    "resources": {"cookie_wall": False, "paywall": False, "login_wall": False, "csp_strict": False, "mixed_content_count": 0},
                },
                "rendered": {"content": {"main_text_length": 2400}, "render_debug": {"console_errors": [], "failed_requests": []}},
                "bot_matrix": [{"profile": "gptbot", "allowed": True, "reason": "ok"}],
                "metrics_bytes": {"html_bytes": 24000, "text_bytes": 1800, "text_html_ratio": 0.075},
                "quality_profile": {
                    "status": "stable",
                    "profile_id": "article-v1",
                    "coverage_ratio": 0.84,
                    "avg_detector_confidence": 0.76,
                    "retrieval_confidence": 0.71,
                    "retrieval_variance": 0.08,
                    "citation_calibration_error": 0.03,
                    "drift_flags": [],
                },
                "quality_gates": {
                    "status": "pass",
                    "passed": 6,
                    "total": 6,
                    "checks": [
                        {"metric": "page_type_accuracy", "value": 0.9, "threshold": 0.8, "pass": True},
                        {"metric": "citation_pass_rate", "value": 0.82, "threshold": 0.75, "pass": True},
                    ],
                },
                "detector_calibration": {"profile_id": "article-v1", "downgraded_count": 0},
                "recommendations": [
                    {
                        "priority": "P1",
                        "area": "schema",
                        "title": "Add JSON-LD",
                        "expected_lift": "+8..12",
                        "evidence": ["No JSON-LD types found"],
                    }
                ],
                "snippet_library": {"jsonld_organization": "<script>...</script>"},
            }
        }
        payload = build_docx_v2(job, "job-test", wow_enabled=True)
        data = payload.getvalue()
        self.assertTrue(len(data) > 2000)
        self.assertTrue(data.startswith(b"PK"))


if __name__ == "__main__":
    unittest.main()
