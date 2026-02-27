import unittest

from app.tools.llmCrawler.extraction import build_snapshot, detect_challenge


class LlmCrawlerExtractionSchemaTests(unittest.TestCase):
    def test_detects_jsonld_microdata_rdfa_types(self):
        html = """
        <html><head>
          <script type="application/ld+json">
          {
            "@context":"https://schema.org",
            "@graph":[
              {"@type":"https://schema.org/Organization","name":"Acme"},
              {"@type":"ItemList"},
              {"@type":"FAQPage"},
              {"@type":"Review"}
            ]
          }
          </script>
        </head>
        <body>
          <div itemscope itemtype="https://schema.org/Organization"></div>
          <section itemscope itemtype="https://schema.org/ItemList"></section>
          <article typeof="schema:FAQPage"></article>
          <div typeof="Review"></div>
          <main><h1>Title</h1><p>Main content for extraction pipeline.</p></main>
        </body></html>
        """
        snap = build_snapshot(
            html=html,
            final_url="https://example.com",
            status_code=200,
            headers={},
            timing_ms=10,
            redirect_chain=[],
            show_headers=False,
            content_type="text/html",
            size_bytes=len(html),
            truncated=False,
        )
        schema = snap.get("schema") or {}
        all_types = set(schema.get("jsonld_types") or []) | set(schema.get("microdata_types") or []) | set(schema.get("rdfa_types") or [])
        seg = snap.get("segmentation") or {}
        self.assertIn("Organization", all_types)
        self.assertIn("ItemList", all_types)
        self.assertIn("FAQPage", all_types)
        self.assertIn("Review", all_types)
        self.assertGreaterEqual(int(schema.get("count") or 0), 4)
        self.assertGreaterEqual(int(schema.get("microdata_count") or 0), 1)
        self.assertIn("content_extraction", seg)

    def test_detects_loose_jsonld_type_when_json_invalid(self):
        html = """
        <html><head>
          <script type="application/ld+json">
            {"@context":"https://schema.org","@type":"Organization","name":"Acme",}
          </script>
        </head><body><main><p>Content</p></main></body></html>
        """
        snap = build_snapshot(
            html=html,
            final_url="https://example.com/page",
            status_code=200,
            headers={},
            timing_ms=12,
            redirect_chain=[],
            show_headers=False,
            content_type="text/html",
            size_bytes=len(html),
            truncated=False,
        )
        schema = snap.get("schema") or {}
        self.assertIn("Organization", set(schema.get("jsonld_types") or []))

    def test_detects_jsonld_without_type_attribute(self):
        html = """
        <html><head>
          <script>
            {"@context":"https://schema.org","@type":"FAQPage","mainEntity":[{"@type":"Question","name":"Q?"}]}
          </script>
        </head><body><main><p>Content</p></main></body></html>
        """
        snap = build_snapshot(
            html=html,
            final_url="https://example.com/faq",
            status_code=200,
            headers={},
            timing_ms=12,
            redirect_chain=[],
            show_headers=False,
            content_type="text/html",
            size_bytes=len(html),
            truncated=False,
        )
        schema = snap.get("schema") or {}
        jsonld_types = set(schema.get("jsonld_types") or [])
        self.assertIn("FAQPage", jsonld_types)
        self.assertIn("Question", jsonld_types)

    def test_extracts_schema_entities_names(self):
        html = """
        <html><head>
          <script type="application/ld+json">
          {
            "@context":"https://schema.org",
            "@graph":[
              {"@type":"Organization","name":"ACME Industrial"},
              {"@type":"Person","name":"John Smith"},
              {"@type":"Product","name":"Vacuum X1000"},
              {"@type":"Place","name":"Berlin"}
            ]
          }
          </script>
        </head>
        <body>
          <div itemscope itemtype="https://schema.org/Organization"><span itemprop="name">ACME Industrial LLC</span></div>
          <main><h1>Vacuum product page</h1><p>Industrial vacuum meter content.</p></main>
        </body></html>
        """
        snap = build_snapshot(
            html=html,
            final_url="https://example.com/product",
            status_code=200,
            headers={},
            timing_ms=9,
            redirect_chain=[],
            show_headers=False,
            content_type="text/html",
            size_bytes=len(html),
            truncated=False,
        )
        entities = ((snap.get("schema") or {}).get("entities") or {})
        self.assertTrue(entities.get("organizations"))
        self.assertTrue(entities.get("persons"))
        self.assertTrue(entities.get("products"))
        self.assertTrue(entities.get("locations"))

    def test_challenge_detection_avoids_false_positive_on_legal_text(self):
        html = """
        <html><body>
          <main>
            <h1>Реклама 18+</h1>
            <p>Рекламодатель ООО Тест, ИНН 1234567890, адрес и контакты компании.</p>
          </main>
        </body></html>
        """
        result = detect_challenge(200, {"server": "nginx"}, html)
        self.assertFalse(bool(result.get("is_challenge")))
        self.assertIn(result.get("status"), {"none", "suspected"})

    def test_challenge_detection_detects_cloudflare_markers(self):
        html = """
        <html><body>
          <script src="/cdn-cgi/challenge-platform/h/b/orchestrate/chl_page/v1"></script>
          <div>Attention Required! Verify you are human</div>
        </body></html>
        """
        result = detect_challenge(403, {"cf-ray": "12345", "server": "cloudflare"}, html)
        self.assertTrue(bool(result.get("is_challenge")))
        self.assertGreaterEqual(float(result.get("confidence") or 0), float(result.get("threshold") or 0))
        self.assertIn("challenge_body", result.get("reasons") or [])


if __name__ == "__main__":
    unittest.main()
