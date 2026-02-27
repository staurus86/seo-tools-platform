import unittest

from bs4 import BeautifulSoup

from app.tools.llmCrawler.patterns import detect_ai_blocks


class LlmCrawlerPatternTests(unittest.TestCase):
    def test_detect_ai_blocks_finds_author_contact_schema(self):
        html = """
        <html><head>
          <meta name="author" content="John Doe" />
          <script type="application/ld+json">{"@context":"https://schema.org","@type":"Organization","name":"Acme"}</script>
        </head>
        <body>
          <article><h1>Vacuum Meter Guide</h1><p>Written by John Doe.</p></article>
          <section class="contact"><a href="mailto:test@example.com">Email</a></section>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = detect_ai_blocks(
            soup=soup,
            main_text="Vacuum meter guide written by John Doe with contact information.",
            full_text="Vacuum meter guide written by John Doe with contact information.",
            schema_types=["Organization"],
        )
        labels = [x.get("label") for x in result.get("detected") or []]
        self.assertIn("Author block", labels)
        self.assertIn("Contact info", labels)
        self.assertGreater(float(result.get("coverage_percent") or 0), 0)


if __name__ == "__main__":
    unittest.main()

