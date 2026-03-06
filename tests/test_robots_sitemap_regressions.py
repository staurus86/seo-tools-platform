import gzip
import asyncio
import unittest
from unittest.mock import patch

from app.api.routers import robots


class _Resp:
    def __init__(self, url: str, status_code: int = 200, text: str = "", content: bytes = b"", headers=None):
        self.url = url
        self.status_code = status_code
        self.text = text
        self.content = content if content else text.encode("utf-8")
        self.headers = headers or {}


class _Session:
    def __init__(self, mapping):
        self.mapping = mapping
        self.headers = {}
        self.calls = []

    def get(self, url, timeout=0, allow_redirects=True, headers=None):
        self.calls.append(url)
        if url not in self.mapping:
            raise AssertionError(f"Unexpected URL requested: {url}")
        return self.mapping[url]


class RobotsAndSitemapRegressionTests(unittest.TestCase):
    def test_parse_robots_keeps_consecutive_user_agents_in_same_group(self):
        parsed = robots.parse_robots(
            "User-agent: Googlebot\n"
            "User-agent: Bingbot\n"
            "Disallow: /private\n"
        )

        self.assertEqual(len(parsed.groups), 1)
        self.assertEqual(parsed.groups[0].user_agents, ["Googlebot", "Bingbot"])
        self.assertEqual(sorted(rule.user_agent for rule in parsed.all_disallow), ["Bingbot", "Googlebot"])

    def test_fetch_robots_always_uses_site_root(self):
        seen = {}

        def fake_get(url, timeout=0, headers=None):
            seen["url"] = url
            return _Resp(url, 200, text="User-agent: *\n")

        with patch("app.api.routers.robots._get_public_target_error", return_value=""), patch(
            "app.api.routers.robots.requests.get", side_effect=fake_get
        ):
            text, status_code, error = robots.fetch_robots("https://example.com/path/page")

        self.assertEqual(text, "User-agent: *\n")
        self.assertEqual(status_code, 200)
        self.assertIsNone(error)
        self.assertEqual(seen["url"], "https://example.com/robots.txt")

    def test_request_models_accept_bare_domain_input(self):
        robots_req = robots.RobotsCheckRequest(url="example.com")
        sitemap_req = robots.SitemapValidateRequest(url="example.com")

        self.assertEqual(robots_req.url, "https://example.com")
        self.assertEqual(sitemap_req.url, "https://example.com")

    def test_validate_sitemaps_rejects_non_xml_plain_text(self):
        response = _Resp(
            "https://example.com/sitemap.xml",
            200,
            text="not xml at all",
            headers={"Content-Type": "text/plain"},
        )

        with patch("app.api.routers.robots._get_public_target_error", return_value=""), patch(
            "app.api.routers.robots.requests.get", return_value=response
        ):
            checks = robots.validate_sitemaps(["https://example.com/sitemap.xml"])

        self.assertFalse(checks[0]["ok"])

    def test_check_sitemap_full_skips_unsafe_child_sitemaps(self):
        xml = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
            "<sitemap><loc>http://127.0.0.1/private.xml</loc></sitemap>"
            "</sitemapindex>"
        )
        session = _Session(
            {
                "https://example.com/sitemap.xml": _Resp(
                    "https://example.com/sitemap.xml",
                    200,
                    text=xml,
                    headers={"Content-Type": "application/xml"},
                )
            }
        )

        def fake_target_error(url):
            return "private blocked" if "127.0.0.1" in url else ""

        with patch("app.api.routers.robots.requests.Session", return_value=session), patch(
            "app.api.routers.robots._get_public_target_error", side_effect=fake_target_error
        ):
            result = robots.check_sitemap_full("https://example.com/sitemap.xml")

        sitemap_files = result["results"]["sitemap_files"]
        self.assertEqual(session.calls, ["https://example.com/sitemap.xml"])
        self.assertEqual(len(sitemap_files), 1)
        self.assertTrue(any("Небезопасный URL дочернего sitemap" in item for item in sitemap_files[0]["warnings"]))

    def test_decode_sitemap_payload_limits_gzip_size(self):
        payload = gzip.compress(b"a" * 256)

        with self.assertRaises(ValueError):
            robots._decode_sitemap_payload(
                payload,
                "https://example.com/sitemap.xml.gz",
                {"Content-Type": "application/x-gzip"},
                max_decoded_bytes=64,
            )

    def test_create_sitemap_validate_writes_metadata_into_results(self):
        captured = {}

        def fake_create_task_result(task_id, task_type, url, result):
            captured["task_id"] = task_id
            captured["task_type"] = task_type
            captured["url"] = url
            captured["result"] = result

        with patch("app.api.routers.robots._get_public_target_error", return_value=""), patch(
            "app.api.routers.robots._discover_sitemap_urls",
            return_value=(["https://example.com/sitemap.xml"], "robots_txt"),
        ), patch(
            "app.api.routers.robots.check_sitemap_full",
            return_value={"results": {"valid": True}},
        ), patch(
            "app.api.routers.robots.create_task_result",
            side_effect=fake_create_task_result,
        ):
            response = asyncio.run(robots.create_sitemap_validate(robots.SitemapValidateRequest(url="example.com")))

        self.assertEqual(response["status"], "SUCCESS")
        self.assertEqual(captured["task_type"], "sitemap_validate")
        self.assertEqual(captured["result"]["results"]["input_url"], "https://example.com")
        self.assertEqual(captured["result"]["results"]["resolved_sitemap_url"], "https://example.com/sitemap.xml")
        self.assertEqual(captured["result"]["results"]["resolved_sitemap_urls"], ["https://example.com/sitemap.xml"])
        self.assertEqual(captured["result"]["results"]["sitemap_discovery_source"], "robots_txt")

    def test_non_self_canonical_does_not_mark_sample_as_non_indexable(self):
        sitemap_xml = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
            "<url><loc>https://example.com/page</loc></url>"
            "</urlset>"
        )
        page_html = (
            "<html><head>"
            "<link rel=\"canonical\" href=\"https://example.com/target\" />"
            "</head><body>ok</body></html>"
        )
        session = _Session(
            {
                "https://example.com/sitemap.xml": _Resp(
                    "https://example.com/sitemap.xml",
                    200,
                    text=sitemap_xml,
                    headers={"Content-Type": "application/xml"},
                ),
                "https://example.com/page": _Resp(
                    "https://example.com/page",
                    200,
                    text=page_html,
                    headers={"Content-Type": "text/html; charset=utf-8"},
                ),
            }
        )

        with patch("app.api.routers.robots.requests.Session", return_value=session), patch(
            "app.api.routers.robots._get_public_target_error", return_value=""
        ):
            result = robots.check_sitemap_full("https://example.com/sitemap.xml")

        live_item = result["results"]["live_indexability_checks"][0]
        canonical_sample = result["results"]["canonical_sample"]
        self.assertTrue(live_item["indexable"])
        self.assertEqual(live_item["canonical_status"], "other")
        self.assertEqual(result["results"]["live_non_indexable_count"], 0)
        self.assertEqual(canonical_sample["non_self_count"], 1)


if __name__ == "__main__":
    unittest.main()
