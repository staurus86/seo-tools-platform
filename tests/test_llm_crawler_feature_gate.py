import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.tools.llmCrawler.feature_gate import is_llm_crawler_enabled_for_request


class _FakeRequest:
    def __init__(self, headers=None, client_host="127.0.0.1"):
        self.headers = headers or {}
        self.client = SimpleNamespace(host=client_host)


class LlmCrawlerFeatureGateTests(unittest.TestCase):
    def test_enabled_by_feature_flag(self):
        req = _FakeRequest()
        with patch("app.tools.llmCrawler.feature_gate.settings.FEATURE_LLM_CRAWLER", True):
            self.assertTrue(is_llm_crawler_enabled_for_request(req))

    def test_enabled_by_admin_role(self):
        req = _FakeRequest(headers={"x-role": "admin"})
        with patch("app.tools.llmCrawler.feature_gate.settings.FEATURE_LLM_CRAWLER", False), patch(
            "app.tools.llmCrawler.feature_gate.settings.LLM_CRAWLER_ALLOW_ADMIN", True
        ):
            self.assertTrue(is_llm_crawler_enabled_for_request(req))

    def test_enabled_by_allowlist_wildcard(self):
        req = _FakeRequest(headers={"x-user-id": ""})
        with patch("app.tools.llmCrawler.feature_gate.settings.FEATURE_LLM_CRAWLER", False), patch(
            "app.tools.llmCrawler.feature_gate.settings.LLM_CRAWLER_ALLOW_ADMIN", False
        ), patch("app.tools.llmCrawler.feature_gate.settings.LLM_CRAWLER_ALLOWLIST", "\"*\""):
            self.assertTrue(is_llm_crawler_enabled_for_request(req))

    def test_enabled_by_allowlist_ip(self):
        req = _FakeRequest(client_host="10.10.10.10")
        with patch("app.tools.llmCrawler.feature_gate.settings.FEATURE_LLM_CRAWLER", False), patch(
            "app.tools.llmCrawler.feature_gate.settings.LLM_CRAWLER_ALLOW_ADMIN", False
        ), patch("app.tools.llmCrawler.feature_gate.settings.LLM_CRAWLER_ALLOWLIST", "10.10.10.10"):
            self.assertTrue(is_llm_crawler_enabled_for_request(req))


if __name__ == "__main__":
    unittest.main()
