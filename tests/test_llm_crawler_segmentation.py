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
        self.assertIn("navigation_detection", out)
        self.assertIn("ads_detection", out)
        self.assertIn("utility_detection", out)
        self.assertFalse(bool((out.get("ads_detection") or {}).get("ads_detected")))

    def test_strict_ads_detection_with_adtech_markers(self):
        html = """
        <html><body>
          <script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js"></script>
          <iframe src="https://doubleclick.net/ad/some-slot"></iframe>
          <main><p>Main article content about industrial sensors and calibration workflow.</p></main>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        out = segment_content(
            soup=soup,
            rendered_text=soup.get_text(" ", strip=True),
            extracted_text=soup.get_text(" ", strip=True),
            links={"count": 1},
            headings={"h1": 0},
        )
        self.assertTrue(bool((out.get("ads_detection") or {}).get("ads_detected")))

    def test_bitrix_megamenu_navigation_detection(self):
        html = """
        <html><body>
          <div id="bx_menu" class="catalog megamenu section-list">
            <a href="/catalog/a">Catalog pumps</a><a href="/catalog/b">Catalog valves</a><a href="/catalog/c">Catalog meters</a>
            <a href="/catalog/d">Catalog sensors</a><a href="/catalog/e">Catalog control</a><a href="/catalog/f">Catalog fittings</a>
          </div>
          <div class="content"><p>Core article text with useful details and explanations for users.</p></div>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        out = segment_content(
            soup=soup,
            rendered_text=soup.get_text(" ", strip=True),
            extracted_text=soup.get_text(" ", strip=True),
            links={"count": 6},
            headings={"h1": 0},
        )
        nav = out.get("navigation_detection") or {}
        self.assertTrue(bool(nav.get("mega_menu_detected")))


if __name__ == "__main__":
    unittest.main()
