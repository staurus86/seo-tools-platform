import unittest

from app.tools.llmCrawler.service import (
    _build_diff,
    _ai_answer_preview,
    _ai_directive_audit,
    _ai_understanding,
    _apply_chunk_dedupe,
    _build_structured_data_split,
    _build_improvement_library,
    _build_detector_layer,
    _quality_gates,
    _recommendation_diagnostics,
    _content_quality_metrics,
    _citation_model_v2,
    _compute_eeat,
    _detect_page_type,
    _page_classification_v2,
    _js_dependency_score,
    _llm_ingestion,
    _retrieval_simulation,
    _snippet_library,
    _extract_entities_v2,
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

    def test_js_dependency_risk_levels(self):
        rendered = {"render_debug": {"failed_requests": []}}
        low = _js_dependency_score(rendered, {"textCoverage": 0.8}, render_status={"status": "executed"})
        self.assertEqual(low.get("risk"), "low")
        self.assertLess(float(low.get("score") or 100), 40)

        medium = _js_dependency_score(rendered, {"textCoverage": 0.54}, render_status={"status": "executed"})
        self.assertEqual(medium.get("risk"), "medium")
        self.assertGreater(float(medium.get("score") or 0), 30)

        high = _js_dependency_score(rendered, {"textCoverage": 0.2}, render_status={"status": "executed"})
        self.assertEqual(high.get("risk"), "high")
        self.assertGreater(float(high.get("score") or 0), 70)

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

    def test_page_type_detection_marketplace(self):
        market = self._snapshot()
        market["meta"] = {"title": "Marketplace: compare prices and add to cart"}
        market["final_url"] = "https://example.com/marketplace/vacuum-meters"
        market["links"] = {"count": 160}
        market["schema"] = {"jsonld_types": ["Product", "Offer", "ItemList"], "microdata_types": [], "rdfa_types": []}
        market["segmentation"] = {"noise_breakdown": {"main_pct": 36, "ads_pct": 4, "live_pct": 0, "nav_pct": 34}}
        cls = _detect_page_type(market)
        self.assertIn(cls.get("page_type"), {"marketplace", "product", "listing"})

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

    def test_page_classification_v2_bitrix_homepage_and_catalog(self):
        nojs = self._snapshot()
        nojs["final_url"] = "https://example.com/"
        nojs["meta"] = {"title": "SIDERUS | Industrial Equipment", "description": "Catalog and services"}
        nojs["links"] = {
            "count": 180,
            "top": [
                {"anchor": "Catalog", "url": "/catalog"},
                {"anchor": "Products", "url": "/products"},
                {"anchor": "Services", "url": "/services"},
                {"anchor": "Collection", "url": "/collection"},
            ],
        }
        nojs["signals"] = {"author_present": False, "date_present": False, "organization_present": True}
        nojs["segmentation"] = {"noise_breakdown": {"nav_pct": 48, "live_pct": 0}, "utility_detection": {"utility_blocks": 4}, "main_ratio": 0.24}
        nojs["schema"] = {"jsonld_types": ["WebSite", "Organization"], "microdata_types": [], "rdfa_types": [], "coverage_score": 66}
        structured = _build_structured_data_split(nojs, None)
        cls = _page_classification_v2(nojs, None, structured)
        self.assertEqual(cls.get("type"), "homepage")
        self.assertGreaterEqual(float(cls.get("confidence") or 0), 0.55)

        catalog = self._snapshot()
        catalog["final_url"] = "https://example.com/catalog/pumps"
        catalog["links"] = {"count": 140, "top": [{"anchor": "Category pumps", "url": "/category/pumps"}]}
        catalog["segmentation"] = {"noise_breakdown": {"nav_pct": 42, "live_pct": 0}, "utility_detection": {"utility_blocks": 1}, "main_ratio": 0.31}
        catalog["schema"] = {"jsonld_types": ["ItemList"], "microdata_types": [], "rdfa_types": [], "coverage_score": 40}
        structured_c = _build_structured_data_split(catalog, None)
        cls_c = _page_classification_v2(catalog, None, structured_c)
        self.assertIn(cls_c.get("type"), {"category", "listing"})

    def test_page_classification_v2_article(self):
        article = self._snapshot()
        article["final_url"] = "https://example.com/blog/vacuum-guide"
        article["signals"] = {"author_present": True, "date_present": True}
        article["content"]["main_text_length"] = 3200
        article["segmentation"] = {"noise_breakdown": {"nav_pct": 12, "live_pct": 0}, "utility_detection": {"utility_blocks": 0}, "main_ratio": 0.62}
        article["schema"] = {"jsonld_types": ["Article", "Organization"], "microdata_types": [], "rdfa_types": [], "coverage_score": 80}
        structured = _build_structured_data_split(article, None)
        cls = _page_classification_v2(article, None, structured)
        self.assertEqual(cls.get("type"), "article")

    def test_page_classification_v2_marketplace(self):
        market = self._snapshot()
        market["final_url"] = "https://example.com/shop/vacuum"
        market["meta"] = {"title": "Seller marketplace for vacuum meters", "description": "Cart, checkout, product availability"}
        market["links"] = {
            "count": 180,
            "top": [
                {"anchor": "Product catalog", "url": "/catalog"},
                {"anchor": "Seller store", "url": "/seller"},
                {"anchor": "Cart", "url": "/cart"},
                {"anchor": "Checkout", "url": "/checkout"},
            ],
        }
        market["segmentation"] = {"noise_breakdown": {"nav_pct": 40, "live_pct": 0}, "utility_detection": {"utility_blocks": 5}, "main_ratio": 0.34}
        market["schema"] = {"jsonld_types": ["Product", "Offer", "ItemList"], "microdata_types": [], "rdfa_types": [], "coverage_score": 70}
        structured = _build_structured_data_split(market, None)
        cls = _page_classification_v2(market, None, structured)
        self.assertIn(cls.get("type"), {"marketplace", "category", "listing", "product"})

    def test_structured_data_raw_vs_rendered_split(self):
        raw = self._snapshot()
        raw["schema"] = {"jsonld_types": ["Organization"], "microdata_types": [], "rdfa_types": [], "coverage_score": 35}
        rendered = {"schema": {"jsonld_types": ["Organization", "FAQPage"], "microdata_types": [], "rdfa_types": [], "coverage_score": 66}}
        split = _build_structured_data_split(raw, rendered)
        self.assertIn("raw", split)
        self.assertIn("rendered", split)
        self.assertIn(split.get("source"), {"raw", "rendered", "raw+rendered"})

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
        self.assertIn("chunks_survive_1024", ingestion)
        self.assertIn("ingestion_score", ingestion)

    def test_content_quality_metrics(self):
        snapshot = self._snapshot()
        quality = _content_quality_metrics(snapshot)
        self.assertEqual(quality.get("status"), "evaluated")
        self.assertIsNotNone(quality.get("text_html_ratio"))
        self.assertGreaterEqual(float(quality.get("avg_paragraph_length") or 0), 0)

    def test_retrieval_and_citation_model_v3(self):
        snapshot = self._snapshot()
        structured = _build_structured_data_split(snapshot, None)
        entities = _extract_entities_v2(snapshot, None, structured)
        retrieval = _retrieval_simulation(snapshot, entities)
        self.assertEqual(retrieval.get("status"), "evaluated")
        self.assertGreaterEqual(float(retrieval.get("avg_score") or 0), 0.0)

        citation = _citation_model_v2(
            structured_data=structured,
            segmentation={"segmentation_confidence": 0.8, "main_content_analysis": {"semantic_density": 0.22}},
            ai_understanding=None,
            ingestion={"status": "evaluated", "ingestion_score": 0.6},
            retrieval=retrieval,
            entities=entities,
        )
        self.assertIn("components", citation)
        self.assertIn("retrieval_score", citation["components"])
        self.assertIn("entity_score", citation["components"])
        self.assertIn(citation.get("version"), {"v2", "v3"})
        self.assertIn("calibration_error_estimate", citation)
        self.assertIn("support_signals", citation)

    def test_detector_layer_contract(self):
        detectors = _build_detector_layer(
            content_extraction={"primary_extractor": "trafilatura", "extraction_confidence": 0.84, "extractor_scores": {"trafilatura": 0.84}},
            segmentation={"segmentation_confidence": 0.78, "main_ratio": 0.42, "boilerplate_ratio": 0.58, "content_segments": [{"id": 1}], "segment_version": "seg-fusion-v3"},
            structured_data={"raw": {"count": 1}, "rendered": {"count": 2}, "source": "raw+rendered", "coverage_score": 0.66, "rendered_only": False},
            entities={"organizations": [{"name": "Acme", "confidence": 0.91}], "persons": [], "products": [], "software": [], "locations": [], "entity_density": 0.003},
            page_classification={"type": "article", "confidence": 0.81, "signals": ["author signal"]},
            js_dependency={"status": "executed", "risk": "medium", "coverage_ratio": 0.54},
            llm_ingestion={"status": "evaluated", "ingestion_score": 0.52, "chunks_total": 12, "chunks_survive_1024": 6},
            retrieval={"status": "evaluated", "avg_score": 0.62, "best_score": 0.78, "retrieval_confidence": 0.71, "queries": ["query"]},
            citation_model={"citation_probability": 0.68, "confidence": 0.81, "version": "v3", "components": {"a": 1}},
            validation={"content_sufficient": True, "warnings": []},
        )
        self.assertIn("summary", detectors)
        self.assertIn("content_extraction", detectors)
        self.assertEqual(detectors["content_extraction"]["status"], "evaluated")
        self.assertEqual(detectors["page_classification"]["status"], "evaluated")

    def test_quality_gates_contract(self):
        detectors = {
            "summary": {"coverage_ratio": 0.82},
        }
        gates = _quality_gates(
            detectors=detectors,
            segmentation={"segmentation_confidence": 0.75},
            retrieval={"avg_score": 0.6},
            citation_model={"confidence": 0.8},
            validation={"content_sufficient": True},
        )
        self.assertIn(gates.get("status"), {"pass", "warn", "fail"})
        self.assertEqual(gates.get("total"), 5)
        self.assertTrue(isinstance(gates.get("checks"), list))

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

    def test_page_classification_v2_docs(self):
        docs = self._snapshot()
        docs["final_url"] = "https://example.com/docs/getting-started"
        docs["meta"] = {"title": "API docs and SDK reference"}
        docs["signals"] = {"author_present": False, "date_present": False, "organization_present": True}
        docs["schema"] = {"jsonld_types": ["TechArticle"], "microdata_types": [], "rdfa_types": [], "coverage_score": 52}
        docs["segmentation"] = {"noise_breakdown": {"nav_pct": 18, "live_pct": 0}, "utility_detection": {"utility_blocks": 1}, "main_ratio": 0.5}
        structured = _build_structured_data_split(docs, None)
        cls = _page_classification_v2(docs, None, structured)
        self.assertIn(cls.get("type"), {"docs", "article"})

    def test_entity_extraction_source_boost_and_density(self):
        snapshot = self._snapshot()
        snapshot["meta"]["site_name"] = "Acme LLC"
        snapshot["meta"]["title"] = "Acme LLC - Vacuum Meters"
        snapshot["schema"] = {"jsonld_types": ["Organization", "Product"], "microdata_types": [], "rdfa_types": [], "coverage_score": 75}
        structured = _build_structured_data_split(snapshot, None)
        entities = _extract_entities_v2(snapshot, None, structured)
        self.assertTrue(entities.get("organizations"))
        org = entities["organizations"][0]
        self.assertGreaterEqual(float(org.get("confidence") or 0), 0.8)
        self.assertIn("source_count", org)
        self.assertGreaterEqual(float(entities.get("entity_density") or 0), 0.0)

    def test_entity_extraction_uses_schema_entity_values(self):
        snapshot = self._snapshot()
        snapshot["schema"] = {
            "jsonld_types": ["Organization", "Product", "Person"],
            "microdata_types": [],
            "rdfa_types": [],
            "coverage_score": 82,
            "entities": {
                "organizations": ["SIDERUS"],
                "persons": ["Alex Roe"],
                "products": ["Vacuum Pro 2000"],
                "locations": ["Berlin"],
            },
        }
        structured = _build_structured_data_split(snapshot, None)
        entities = _extract_entities_v2(snapshot, None, structured)
        org_names = [str(x.get("name")) for x in (entities.get("organizations") or [])]
        product_names = [str(x.get("name")) for x in (entities.get("products") or [])]
        person_names = [str(x.get("name")) for x in (entities.get("persons") or [])]
        self.assertIn("SIDERUS", org_names)
        self.assertIn("Vacuum Pro 2000", product_names)
        self.assertIn("Alex Roe", person_names)

    def test_recommendation_diagnostics_contract(self):
        nojs = self._snapshot()
        nojs["schema"] = {"jsonld_types": ["Organization"], "microdata_types": [], "rdfa_types": []}
        nojs["signals"] = {"author_present": True, "date_present": True}
        nojs["links"] = {"js_only_count": 0}
        recs = [
            {"title": "Добавьте JSON-LD (Organization/Article/Product) для доверия и извлечения"},
            {"title": "Укажите автора/дату публикации — повышает понятность и доверие"},
            {"title": "Избегайте JS-only ссылок — используйте href для навигации ботов"},
        ]
        diag = _recommendation_diagnostics(recs, nojs, None, {"status": "not_executed"})
        self.assertIn(diag.get("status"), {"ok", "warning"})
        self.assertTrue(isinstance(diag.get("issues"), list))
        self.assertEqual(diag.get("checked_rules"), 5)


if __name__ == "__main__":
    unittest.main()
