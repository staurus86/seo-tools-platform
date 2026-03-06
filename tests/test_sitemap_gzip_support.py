import gzip
import unittest
from unittest.mock import patch

from app.api.routers import robots


SITEMAP_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/</loc></url>
  <url><loc>https://example.com/about</loc></url>
</urlset>
"""

SITEMAP_GZ = gzip.compress(SITEMAP_XML)

HTML_PAGE = """
<html>
<head><link rel="canonical" href="https://example.com/"></head>
<body>ok</body>
</html>
"""

HTML_ABOUT = """
<html>
<head><link rel="canonical" href="https://example.com/about"></head>
<body>about</body>
</html>
"""


class _MockResponse:
    def __init__(self, url: str, status_code: int = 200, *, content: bytes = b"", text: str = "", headers=None):
        self.url = url
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content if content else text.encode("utf-8")
        if text:
            self.text = text
        else:
            try:
                self.text = self.content.decode("utf-8")
            except UnicodeDecodeError:
                self.text = ""


class _MockSession:
    def __init__(self, by_url):
        self.by_url = by_url
        self.headers = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, timeout=0, allow_redirects=True, headers=None):
        if url not in self.by_url:
            raise AssertionError(f"Unexpected URL requested: {url}")
        return self.by_url[url]


class SitemapGzipSupportTests(unittest.TestCase):
    def test_discover_sitemap_urls_accepts_gzip_sitemap_from_robots(self):
        by_url = {
            "https://example.com/robots.txt": _MockResponse(
                "https://example.com/robots.txt",
                text="Sitemap: https://example.com/sitemap.xml.gz\n",
                headers={"Content-Type": "text/plain; charset=utf-8"},
            ),
            "https://example.com/sitemap.xml.gz": _MockResponse(
                "https://example.com/sitemap.xml.gz",
                content=SITEMAP_GZ,
                headers={"Content-Type": "application/x-gzip"},
            ),
        }

        with patch("app.api.routers.robots.requests.Session", side_effect=lambda: _MockSession(by_url)):
            sitemap_urls, source = robots._discover_sitemap_urls("https://example.com")

        self.assertEqual(sitemap_urls, ["https://example.com/sitemap.xml.gz"])
        self.assertEqual(source, "robots.txt")

    def test_check_sitemap_full_parses_gzip_sitemap(self):
        by_url = {
            "https://example.com/sitemap.xml.gz": _MockResponse(
                "https://example.com/sitemap.xml.gz",
                content=SITEMAP_GZ,
                headers={"Content-Type": "application/x-gzip"},
            ),
            "https://example.com/": _MockResponse(
                "https://example.com/",
                text=HTML_PAGE,
                headers={"Content-Type": "text/html; charset=utf-8"},
            ),
            "https://example.com/about": _MockResponse(
                "https://example.com/about",
                text=HTML_ABOUT,
                headers={"Content-Type": "text/html; charset=utf-8"},
            ),
        }

        with patch("app.api.routers.robots.requests.Session", side_effect=lambda: _MockSession(by_url)):
            result = robots.check_sitemap_full("https://example.com/sitemap.xml.gz")

        payload = result["results"]
        self.assertTrue(payload["valid"])
        self.assertEqual(payload["urls_count"], 2)
        self.assertEqual(payload["unique_urls_count"], 2)
        self.assertEqual(payload["sitemaps_valid"], 1)
        self.assertEqual(payload["sitemap_files"][0]["compression"], "gzip")
        self.assertEqual(payload["sitemap_files"][0]["type"], "urlset")

    def test_validate_sitemaps_accepts_gzip_xml(self):
        response = _MockResponse(
            "https://example.com/sitemap.xml.gz",
            content=SITEMAP_GZ,
            headers={"Content-Type": "application/x-gzip"},
        )

        with patch("app.api.routers.robots.requests.get", return_value=response):
            checks = robots.validate_sitemaps(["https://example.com/sitemap.xml.gz"])

        self.assertEqual(len(checks), 1)
        self.assertTrue(checks[0]["ok"])
        self.assertEqual(checks[0]["status_code"], 200)


if __name__ == "__main__":
    unittest.main()
