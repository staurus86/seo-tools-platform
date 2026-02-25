import unittest
from unittest.mock import patch

from app.tools.llmCrawler.security import assert_safe_url, normalize_http_url


class LlmCrawlerSecurityTests(unittest.TestCase):
    def test_normalize_http_url_accepts_domain(self):
        self.assertEqual(normalize_http_url("example.com"), "https://example.com/")

    def test_assert_safe_url_blocks_localhost(self):
        with self.assertRaises(ValueError):
            assert_safe_url("http://localhost/")

    @patch("app.tools.llmCrawler.security.resolve_hostname_ips", return_value=["10.10.0.1"])
    def test_assert_safe_url_blocks_private_ip(self, _mock_resolve):
        with self.assertRaises(ValueError):
            assert_safe_url("https://example.com/")

    @patch("app.tools.llmCrawler.security.resolve_hostname_ips", return_value=["93.184.216.34"])
    def test_assert_safe_url_allows_public_ip(self, _mock_resolve):
        assert_safe_url("https://example.com/")


if __name__ == "__main__":
    unittest.main()

