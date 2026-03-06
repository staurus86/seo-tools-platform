import unittest
from unittest.mock import patch

from app.tools.redirect_checker.service_v1 import run_redirect_checker


def _mk_trace(start_url, codes, final_url=None, canonical_url=""):
    final_url = final_url or start_url
    chain = []
    for idx, code in enumerate(codes):
        chain.append(
            {
                "url": start_url if idx == 0 else final_url,
                "status_code": int(code),
                "location": final_url if idx < len(codes) - 1 else "",
            }
        )
    return {
        "start_url": start_url,
        "final_url": final_url,
        "final_status_code": int(codes[-1]) if codes else None,
        "hops": max(0, len(codes) - 1),
        "chain": chain,
        "error": "",
        "loop_detected": False,
        "duration_ms": 10,
        "content_type": "text/html; charset=utf-8",
        "canonical_url": canonical_url,
        "canonical_source": "html" if canonical_url else "",
    }


class RedirectCheckerServiceTests(unittest.TestCase):
    @patch("app.tools.redirect_checker.service_v1._trace_url")
    def test_returns_all_11_scenarios(self, mock_trace):
        def side_effect(url, user_agent, timeout=12, max_hops=10):  # noqa: ARG001
            if url.startswith("http://example.com"):
                return _mk_trace(url, [301, 200], "https://example.com/")
            if "www.example.com" in url:
                return _mk_trace(url, [301, 200], "https://example.com/")
            if "/redirect-checker//probe//" in url:
                return _mk_trace(url, [301, 200], "https://example.com/redirect-checker/probe/")
            if url.endswith("/CART"):
                return _mk_trace(url, [301, 200], "https://example.com/cart")
            if url.endswith("/index.html"):
                return _mk_trace(url, [301, 200], "https://example.com/")
            if url.endswith("/page"):
                return _mk_trace(url, [301, 200], "https://example.com/page/")
            if url.endswith("/page/"):
                return _mk_trace(url, [200], "https://example.com/page/")
            if url.endswith("/legacy-page.html"):
                return _mk_trace(url, [301, 200], "https://example.com/legacy-page")
            if url.endswith("/legacy-page.php"):
                return _mk_trace(url, [404], url)
            if "/redirect-checker-404-" in url:
                return _mk_trace(url, [404], url)
            return _mk_trace(url, [200], "https://example.com/", canonical_url="https://example.com/")

        mock_trace.side_effect = side_effect

        result = run_redirect_checker(url="example.com", user_agent_key="googlebot_desktop")
        payload = result.get("results", {})
        scenarios = payload.get("scenarios", [])
        summary = payload.get("summary", {})
        keyed = {item.get("key"): item for item in scenarios}

        self.assertEqual(len(scenarios), 17)
        self.assertEqual(summary.get("total_scenarios"), 17)
        self.assertEqual(keyed.get("http_to_https", {}).get("status"), "passed")
        self.assertEqual(keyed.get("canonical_tag", {}).get("status"), "passed")
        self.assertEqual(keyed.get("missing_404", {}).get("status"), "passed")
        self.assertEqual(keyed.get("query_params_canonicalization", {}).get("status"), "passed")
        self.assertEqual(keyed.get("soft_404_detection", {}).get("status"), "passed")
        self.assertEqual(keyed.get("http_to_https", {}).get("duration_ms"), 10)
        self.assertEqual(keyed.get("trailing_slash", {}).get("duration_ms"), 20)
        self.assertEqual(summary.get("errors"), 0)
        self.assertIn("applied_policy", payload)

    @patch("app.tools.redirect_checker.service_v1._trace_url")
    def test_emits_progress_callbacks_for_scenarios(self, mock_trace):
        def side_effect(url, user_agent, timeout=12, max_hops=10):  # noqa: ARG001
            if url.startswith("http://example.com"):
                return _mk_trace(url, [301, 200], "https://example.com/")
            if "www.example.com" in url:
                return _mk_trace(url, [301, 200], "https://example.com/")
            if "/redirect-checker//probe//" in url:
                return _mk_trace(url, [301, 200], "https://example.com/redirect-checker/probe/")
            if url.endswith("/CART"):
                return _mk_trace(url, [301, 200], "https://example.com/cart")
            if url.endswith("/index.html"):
                return _mk_trace(url, [301, 200], "https://example.com/")
            if url.endswith("/page"):
                return _mk_trace(url, [301, 200], "https://example.com/page/")
            if url.endswith("/page/"):
                return _mk_trace(url, [200], "https://example.com/page/")
            if url.endswith("/legacy-page.html"):
                return _mk_trace(url, [301, 200], "https://example.com/legacy-page")
            if url.endswith("/legacy-page.php"):
                return _mk_trace(url, [404], url)
            if "/redirect-checker-404-" in url:
                return _mk_trace(url, [404], url)
            return _mk_trace(url, [200], "https://example.com/", canonical_url="https://example.com/")

        mock_trace.side_effect = side_effect
        progress_events = []

        run_redirect_checker(
            url="example.com",
            user_agent_key="googlebot_desktop",
            progress_callback=progress_events.append,
        )

        self.assertEqual(len(progress_events), 17)
        self.assertEqual(progress_events[0].get("current_scenario_key"), "http_to_https")
        self.assertEqual(progress_events[-1].get("current_scenario_key"), "soft_404_detection")
        self.assertEqual(progress_events[-1].get("current_scenario_index"), 17)
        self.assertEqual(progress_events[-1].get("scenario_count"), 17)

    @patch("app.tools.redirect_checker.service_v1._trace_url")
    def test_policy_controls_affect_statuses(self, mock_trace):
        def side_effect(url, user_agent, timeout=12, max_hops=10):  # noqa: ARG001
            if url.startswith("http://example.com"):
                return _mk_trace(url, [301, 200], "https://example.com/")
            if "www.example.com" in url:
                return _mk_trace(url, [301, 200], "https://example.com/")
            if "/redirect-checker//probe//" in url:
                return _mk_trace(url, [301, 200], "https://example.com/redirect-checker/probe/")
            if url.endswith("/CART"):
                return _mk_trace(url, [200], "https://example.com/CART")
            if url.endswith("/index.html"):
                return _mk_trace(url, [301, 200], "https://example.com/")
            if url.endswith("/page"):
                return _mk_trace(url, [200], "https://example.com/page")
            if url.endswith("/page/"):
                return _mk_trace(url, [200], "https://example.com/page/")
            if url.endswith("/legacy-page.html"):
                return _mk_trace(url, [301, 200], "https://example.com/legacy-page")
            if url.endswith("/legacy-page.php"):
                return _mk_trace(url, [404], url)
            if "/redirect-checker-required-probe" in url:
                return _mk_trace(url, [200], "https://example.com/redirect-checker-required-probe")
            if "/redirect-checker-404-" in url:
                return _mk_trace(url, [200], "https://example.com/")
            return _mk_trace(url, [200], "https://example.com/", canonical_url="https://example.com/")

        mock_trace.side_effect = side_effect

        result = run_redirect_checker(
            url="example.com",
            user_agent_key="googlebot_desktop",
            canonical_host_policy="www",
            trailing_slash_policy="no-slash",
            enforce_lowercase=False,
            required_query_params=["page"],
        )
        payload = result.get("results", {})
        keyed = {item.get("key"): item for item in (payload.get("scenarios", []) or [])}
        policy = payload.get("applied_policy", {})

        self.assertEqual(policy.get("canonical_host_policy"), "www")
        self.assertEqual(policy.get("trailing_slash_policy"), "no-slash")
        self.assertEqual(policy.get("enforce_lowercase"), False)
        self.assertEqual(keyed.get("www_consistency", {}).get("status"), "error")
        self.assertEqual(keyed.get("url_case", {}).get("status"), "passed")
        self.assertEqual(keyed.get("trailing_slash", {}).get("status"), "warning")
        self.assertEqual(keyed.get("required_query_params", {}).get("status"), "error")


if __name__ == "__main__":
    unittest.main()
