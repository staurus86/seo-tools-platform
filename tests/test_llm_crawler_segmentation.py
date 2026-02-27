import unittest

from bs4 import BeautifulSoup

from app.tools.llmCrawler.segmentation import segment_content


class LlmCrawlerSegmentationTests(unittest.TestCase):
    def test_segmentation_noise_breakdown(self):
        html = """
        <html><body>
          <nav><a href="/a">Home</a> <a href="/b">News</a> <a href="/c">Live</a></nav>
          <div class="promo">Реклама 18+ Рекламодатель ООО Test ИНН 12345 erid XXX</div>
          <section class="live">Live match 2:1 Continues PGL Qualifier 1:0</section>
          <article>
            <h1>Vacuum meters for industrial systems</h1>
            <p>Vacuum meters help measure pressure and calibrate industrial lines safely.</p>
            <p>This guide explains maintenance, tolerance, and sensor diagnostics.</p>
          </article>
          <footer>copyright terms policy</footer>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        out = segment_content(
            soup=soup,
            rendered_text=soup.get_text(" ", strip=True),
            extracted_text=soup.get_text(" ", strip=True),
            links={"count": 3},
            headings={"h1": 1},
        )
        self.assertIn("noise_breakdown", out)
        self.assertIn("content_segments", out)
        self.assertTrue(out.get("main_text"))
        breakdown = out.get("noise_breakdown") or {}
        self.assertGreaterEqual(float(breakdown.get("main_pct") or 0), 20.0)
        self.assertGreaterEqual(float(breakdown.get("ads_pct") or 0), 1.0)


if __name__ == "__main__":
    unittest.main()

