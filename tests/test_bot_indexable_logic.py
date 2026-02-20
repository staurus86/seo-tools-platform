import unittest

from app.tools.bots.service_v2 import BotAccessibilityServiceV2, BotDefinition


class _MockResponse:
    def __init__(self):
        self.status_code = 200
        self.headers = {"content-type": "text/html; charset=utf-8"}
        self.text = "<html><head><title>ok</title></head><body>content</body></html>"
        self.content = self.text.encode("utf-8")
        self.url = "https://example.com/"


class _MockSession:
    def get(self, *args, **kwargs):
        return _MockResponse()


class BotIndexableLogicTests(unittest.TestCase):
    def test_waf_signal_does_not_force_indexable_false(self):
        svc = BotAccessibilityServiceV2()
        svc.session = _MockSession()
        svc._detect_waf_cdn = lambda response, html_head, error: {  # type: ignore[method-assign]
            "detected": True,
            "provider": "mock",
            "reason": "challenge",
            "confidence": 0.95,
        }
        bot = BotDefinition("Googlebot Desktop", "Mozilla/5.0 (compatible; Googlebot/2.1)", "Google")
        row = svc._check_one("https://example.com/", bot, robots_text=None)
        self.assertTrue(row.get("accessible"))
        self.assertFalse(row.get("renderable"))
        self.assertTrue(row.get("indexable"))

    def test_low_confidence_waf_signal_does_not_force_renderable_false(self):
        svc = BotAccessibilityServiceV2()
        svc.session = _MockSession()
        svc._detect_waf_cdn = lambda response, html_head, error: {  # type: ignore[method-assign]
            "detected": True,
            "provider": "mock",
            "reason": "weak_hint",
            "confidence": 0.72,
        }
        bot = BotDefinition("Googlebot Desktop", "Mozilla/5.0 (compatible; Googlebot/2.1)", "Google")
        row = svc._check_one("https://example.com/", bot, robots_text=None)
        self.assertTrue(row.get("accessible"))
        self.assertTrue(row.get("renderable"))
        self.assertTrue(row.get("indexable"))


if __name__ == "__main__":
    unittest.main()
