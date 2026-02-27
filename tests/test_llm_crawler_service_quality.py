import unittest

from app.tools.llmCrawler.service import (
    _ai_answer_preview,
    _ai_directive_audit,
    _ai_understanding,
    _build_improvement_library,
    _llm_ingestion,
    _snippet_library,
)


class LlmCrawlerServiceQualityTests(unittest.TestCase):
    def _snapshot(self):
        return {
            "meta": {"title": "Industrial Vacuum Meter Guide"},
            "headings": {"h1": 1, "h2": 3, "h3": 1, "h1_texts": ["Vacuum Meters for Industrial Systems"]},
            "content": {
                "main_text_length": 2400,
                "readability_score": 62.0,
                "boilerplate_ratio": 0.32,
                "main_text_preview": (
                    "Vacuum meters are used to monitor pressure in industrial lines. "
                    "The page explains calibration, maintenance, and measurement ranges."
                ),
                "chunks": [
                    {"idx": 1, "text": "Menu Home About Contacts Support Pricing"},
                    {"idx": 2, "text": "Vacuum meter calibration and pressure measurement guide for industrial process lines."},
                    {"idx": 3, "text": "Maintenance schedule, sensor drift checks, and acceptable tolerance for gauge readings."},
                ],
            },
            "signals": {"author_present": False, "date_present": True},
            "schema": {"coverage_score": 50},
            "entity_graph": {"organizations": [], "persons": [], "products": ["Vacuum meter"], "locations": []},
        }

    def test_topic_fallback_is_meaningful(self):
        snapshot = self._snapshot()
        ai = _ai_understanding(snapshot, llm_sim=None)
        self.assertTrue(ai.get("topic"))
        self.assertTrue(ai.get("topic_fallback_used"))
        self.assertGreater(float(ai.get("topic_confidence") or 0), 0)

    def test_preview_uses_ranked_chunks(self):
        snapshot = self._snapshot()
        preview = _ai_answer_preview(snapshot, llm_sim=None)
        self.assertEqual(preview.get("preview_mode"), "extractive")
        self.assertTrue(preview.get("answer"))
        self.assertTrue(preview.get("chunk_ranking_debug"))

    def test_ingestion_not_evaluated_without_chunks(self):
        snapshot = self._snapshot()
        snapshot["content"]["chunks"] = []
        ingestion = _llm_ingestion(snapshot, diff={"textCoverage": 0.7})
        self.assertEqual(ingestion.get("status"), "not_evaluated")
        self.assertEqual(ingestion.get("reason"), "no_chunks_available")

    def test_directive_audit_and_library(self):
        snapshot = self._snapshot()
        snapshot["meta"] = {"meta_robots": "noindex, noai", "x_robots_tag": ""}
        snapshot["schema"] = {"jsonld_types": []}
        snapshot["signals"] = {"author_present": False, "date_present": False}
        snapshot["ai_blocks"] = {"missing_critical": ["Author block", "Contact info"]}
        policies = {
            "robots": {
                "profiles": {
                    "gptbot": {"allowed": False, "reason": "Disallow: /"},
                    "google-extended": {"allowed": True, "reason": "Allowed"},
                }
            }
        }
        audit = _ai_directive_audit(snapshot, policies)
        self.assertIn("profiles", audit)
        self.assertIn("gptbot", audit["profiles"])
        issues = ["LLM simulation not executed"]
        lib = _build_improvement_library(snapshot, snapshot["ai_blocks"], audit, issues, _snippet_library())
        self.assertTrue(lib.get("missing"))


if __name__ == "__main__":
    unittest.main()
