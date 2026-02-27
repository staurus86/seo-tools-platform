import unittest

from app.tools.llmCrawler.extraction import build_snapshot


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


if __name__ == "__main__":
    unittest.main()
