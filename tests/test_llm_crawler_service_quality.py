import unittest

from app.tools.llmCrawler.service import (
    _build_diff,
    _ai_answer_preview,
    _ai_directive_audit,
    _ai_understanding,
    _apply_chunk_dedupe,
    _build_improvement_library,
    _compute_eeat,
    _detect_page_type,
    _js_dependency_score,
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

    def test_preview_warns_for_feed_like_pages(self):
        snapshot = self._snapshot()
        snapshot["segmentation"] = {"main_content_confidence": {"level": "low", "reasons": ["feed layout"]}}
        preview = _ai_answer_preview(snapshot, llm_sim=None, page_type_info={"page_type": "listing"})
        self.assertTrue(preview.get("warning"))
        self.assertEqual(preview.get("answer"), "Page not reliably summarizable")
        self.assertEqual(len(preview.get("fix_steps") or []), 2)

    def test_chunk_dedupe_metrics(self):
        snapshot = self._snapshot()
        snapshot["content"]["chunks"] = [
            {"idx": 1, "text": "Vacuum meter calibration and pressure measurement guide."},
            {"idx": 2, "text": "Vacuum meter calibration and pressure measurement guide."},
            {"idx": 3, "text": "Maintenance schedule and tolerance checks."},
        ]
        stats = _apply_chunk_dedupe(snapshot)
        self.assertEqual(stats.get("chunks_total"), 3)
        self.assertEqual(stats.get("chunks_unique"), 2)
        self.assertGreater(stats.get("dedupe_ratio", 0), 0)

    def test_page_type_detection_listing_feed(self):
        snapshot = self._snapshot()
        snapshot["final_url"] = "https://example.com/news"
        snapshot["links"] = {"count": 120}
        snapshot["segmentation"] = {"noise_breakdown": {"live_pct": 22, "nav_pct": 30}}
        page_type = _detect_page_type(snapshot)
        self.assertEqual(page_type.get("page_type"), "listing")

    def test_h1_only_after_js_flag(self):
        nojs = {"content": {"main_text_length": 1000}, "links": {"all_urls": []}, "headings": {"h1": 0, "h2": 1, "h3": 0}}
        rendered = {"content": {"main_text_length": 1200}, "links": {"all_urls": []}, "headings": {"h1": 1, "h2": 2, "h3": 0}}
        diff = _build_diff(nojs, rendered)
        self.assertTrue(diff.get("h1Consistency", {}).get("h1_appears_only_after_js"))
        self.assertIn("H1 appears only after JS", diff.get("missing", []))

    def test_js_dependency_not_executed_status(self):
        js = _js_dependency_score(None, {"textCoverage": None}, render_status={"status": "not_executed", "reason": "render_disabled_in_options"})
        self.assertEqual(js.get("status"), "not_executed")
        self.assertIsNone(js.get("score"))

    def test_page_type_detection_service_and_review(self):
        service_snapshot = self._snapshot()
        service_snapshot["meta"] = {"title": "SEO consulting services for enterprise websites"}
        service_snapshot["final_url"] = "https://example.com/services/seo-consulting"
        service_snapshot["segmentation"] = {"noise_breakdown": {"main_pct": 65, "ads_pct": 2, "live_pct": 0, "nav_pct": 20}}
        svc = _detect_page_type(service_snapshot)
        self.assertEqual(svc.get("page_type"), "service")

        review_snapshot = self._snapshot()
        review_snapshot["meta"] = {"title": "Vacuum meter review and rating comparison"}
        review_snapshot["final_url"] = "https://example.com/review/vacuum-meter"
        review_snapshot["schema"] = {"jsonld_types": ["Review", "AggregateRating"]}
        review_snapshot["segmentation"] = {"noise_breakdown": {"main_pct": 55, "ads_pct": 5, "live_pct": 0, "nav_pct": 18}}
        rv = _detect_page_type(review_snapshot)
        self.assertEqual(rv.get("page_type"), "review")

    def test_page_type_detection_homepage_and_docs(self):
        homepage = self._snapshot()
        homepage["final_url"] = "https://example.com/"
        homepage["meta"] = {"title": "Acme Company Homepage"}
        homepage["links"] = {"count": 120}
        homepage["signals"] = {"organization_present": True}
        homepage["schema"] = {"jsonld_types": ["WebSite", "Organization"], "microdata_types": [], "rdfa_types": []}
        hp = _detect_page_type(homepage)
        self.assertEqual(hp.get("page_type"), "homepage")

        docs = self._snapshot()
        docs["final_url"] = "https://example.com/docs/api-reference"
        docs["meta"] = {"title": "API documentation and SDK reference"}
        docs["schema"] = {"jsonld_types": ["TechArticle"], "microdata_types": [], "rdfa_types": []}
        d = _detect_page_type(docs)
        self.assertIn(d.get("page_type"), {"docs", "article"})

    def test_ingestion_not_evaluated_without_chunks(self):
        snapshot = self._snapshot()
        snapshot["content"]["chunks"] = []
        ingestion = _llm_ingestion(snapshot, diff={"textCoverage": 0.7})
        self.assertEqual(ingestion.get("status"), "not_evaluated")
        self.assertEqual(ingestion.get("reason"), "no_chunks_available")

    def test_ingestion_fallback_when_llm_disabled(self):
        snapshot = self._snapshot()
        ingestion = _llm_ingestion(snapshot, diff={"textCoverage": 0.8}, llm_enabled=False)
        self.assertEqual(ingestion.get("status"), "evaluated")
        self.assertEqual(ingestion.get("mode"), "heuristic_without_llm")
        self.assertIsNotNone(ingestion.get("score"))

    def test_eeat_heuristic_fallback_has_score(self):
        snapshot = self._snapshot()
        snapshot["signals"].update(
            {
                "has_contact_info": True,
                "has_legal_docs": True,
                "has_reviews": True,
                "trust_badges": False,
                "organization_present": True,
            }
        )
        eeat = _compute_eeat(snapshot, score={"total": 70}, mode="heuristic_fallback")
        self.assertEqual(eeat.get("status"), "evaluated")
        self.assertEqual(eeat.get("mode"), "heuristic_fallback")
        self.assertGreater(float(eeat.get("score") or 0), 0)
        self.assertIn("components", eeat)

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
