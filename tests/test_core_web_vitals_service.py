import unittest
from unittest.mock import patch

from app.tools.core_web_vitals.service_v1 import run_core_web_vitals


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


class CoreWebVitalsServiceTests(unittest.TestCase):
    @patch("app.tools.core_web_vitals.service_v1.requests.get")
    def test_runs_and_parses_metrics(self, mock_get):
        payload = {
            "lighthouseResult": {
                "categories": {"performance": {"score": 0.82}},
                "audits": {
                    "largest-contentful-paint": {"numericValue": 2900},
                    "interaction-to-next-paint": {"numericValue": 180},
                    "cumulative-layout-shift": {"numericValue": 0.12},
                    "first-contentful-paint": {"numericValue": 1700},
                    "server-response-time": {"numericValue": 620},
                    "unused-css-rules": {
                        "score": 0.2,
                        "scoreDisplayMode": "numeric",
                        "title": "Reduce unused CSS",
                        "displayValue": "120 KiB",
                        "description": "Unused styles were found",
                    },
                },
            },
            "loadingExperience": {
                "metrics": {
                    "LARGEST_CONTENTFUL_PAINT_MS": {"percentile": 2700},
                    "INTERACTION_TO_NEXT_PAINT": {"percentile": 220},
                    "CUMULATIVE_LAYOUT_SHIFT_SCORE": {"percentile": 18},
                }
            },
        }
        mock_get.return_value = _FakeResponse(200, payload)

        result = run_core_web_vitals(url="example.com", strategy="mobile", api_key="token")
        self.assertEqual(result.get("task_type"), "core_web_vitals")
        self.assertEqual(result.get("url"), "https://example.com/")
        r = result.get("results", {})
        self.assertEqual(r.get("strategy"), "mobile")
        self.assertEqual((r.get("summary") or {}).get("performance_score"), 82)
        self.assertEqual(((r.get("metrics") or {}).get("lcp") or {}).get("status"), "needs_improvement")
        self.assertEqual(((r.get("metrics") or {}).get("inp") or {}).get("status"), "needs_improvement")
        self.assertEqual(((r.get("metrics") or {}).get("cls") or {}).get("status"), "needs_improvement")
        self.assertTrue(len(r.get("opportunities") or []) >= 1)
        self.assertTrue(len(r.get("recommendations") or []) >= 1)
        self.assertTrue((r.get("api") or {}).get("has_key"))

    @patch("app.tools.core_web_vitals.service_v1.requests.get")
    def test_raises_on_api_error(self, mock_get):
        mock_get.return_value = _FakeResponse(429, {"error": {"message": "quota exceeded"}})
        with self.assertRaises(RuntimeError):
            run_core_web_vitals(url="https://example.com", strategy="desktop")


if __name__ == "__main__":
    unittest.main()
