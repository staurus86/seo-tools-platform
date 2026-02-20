import unittest

from app.tools.bots.service_v2 import BotAccessibilityServiceV2


class _Resp:
    def __init__(self, status_code=200, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


class BotWafDetectionTests(unittest.TestCase):
    def test_no_false_positive_on_normal_200_html(self):
        svc = BotAccessibilityServiceV2()
        resp = _Resp(status_code=200, headers={"content-type": "text/html; charset=utf-8"})
        html = (
            "<html><head><title>Site</title></head><body>"
            "Промышленное оборудование. Полезный контент для пользователей."
            "</body></html>"
        )
        signal = svc._detect_waf_cdn(resp, html, None)
        self.assertFalse(signal.get("detected"))

    def test_detects_strong_challenge_signature(self):
        svc = BotAccessibilityServiceV2()
        resp = _Resp(status_code=403, headers={"server": "cloudflare"})
        html = "<html><body>Attention Required! Verify you are human. Cloudflare Ray ID</body></html>"
        signal = svc._detect_waf_cdn(resp, html, None)
        self.assertTrue(signal.get("detected"))
        self.assertGreaterEqual(float(signal.get("confidence", 0.0) or 0.0), 0.7)

    def test_provider_header_only_on_200_is_not_detected(self):
        svc = BotAccessibilityServiceV2()
        resp = _Resp(status_code=200, headers={"server": "cloudflare", "cf-ray": "abc"})
        html = "<html><body>Normal business content page</body></html>"
        signal = svc._detect_waf_cdn(resp, html, None)
        self.assertFalse(signal.get("detected"))

    def test_script_marker_only_on_200_is_not_detected(self):
        svc = BotAccessibilityServiceV2()
        resp = _Resp(status_code=200, headers={"content-type": "text/html"})
        html = (
            "<html><head><script>var t='verify you are human';</script></head>"
            "<body>Каталог оборудования и услуги компании</body></html>"
        )
        signal = svc._detect_waf_cdn(resp, html, None)
        self.assertFalse(signal.get("detected"))


if __name__ == "__main__":
    unittest.main()
