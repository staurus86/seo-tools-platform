import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from app.tools.llmCrawler.router import get_llm_crawler_job, run_llm_crawler
from app.tools.llmCrawler.schemas import LlmCrawlerRunRequest


class _FakeRequest:
    def __init__(self, headers=None, client_host="127.0.0.1"):
        self.headers = headers or {}
        self.client = SimpleNamespace(host=client_host)


class LlmCrawlerRouteTests(unittest.IsolatedAsyncioTestCase):
    def _healthy_heartbeat(self):
        return {"updatedAt": datetime.now(timezone.utc).isoformat()}

    async def test_feature_flag_off_returns_404(self):
        payload = LlmCrawlerRunRequest(url="example.com")
        req = _FakeRequest()
        with patch("app.tools.llmCrawler.router.settings.FEATURE_LLM_CRAWLER", False), patch(
            "app.tools.llmCrawler.router.settings.LLM_CRAWLER_ALLOW_ADMIN", False
        ):
            with self.assertRaises(HTTPException) as ctx:
                await run_llm_crawler(payload, req)
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_run_queues_job_when_enabled(self):
        payload = LlmCrawlerRunRequest(url="example.com")
        req = _FakeRequest(headers={"x-role": "admin"})
        with patch("app.tools.llmCrawler.router.settings.FEATURE_LLM_CRAWLER", True), patch(
            "app.tools.llmCrawler.router.get_worker_heartbeat", return_value=self._healthy_heartbeat()
        ), patch(
            "app.tools.llmCrawler.router.enqueue_job", return_value="llmcrawler-test-1"
        ):
            response = await run_llm_crawler(payload, req)
        self.assertTrue(str(response.get("jobId", "")).startswith("llmcrawler-"))

    async def test_run_returns_503_when_worker_unavailable(self):
        payload = LlmCrawlerRunRequest(url="example.com")
        req = _FakeRequest(headers={"x-role": "admin"})
        with patch("app.tools.llmCrawler.router.settings.FEATURE_LLM_CRAWLER", True), patch(
            "app.tools.llmCrawler.router.settings.LLM_CRAWLER_REQUIRE_HEALTHY_WORKER", True
        ), patch("app.tools.llmCrawler.router.get_worker_heartbeat", return_value=None):
            with self.assertRaises(HTTPException) as ctx:
                await run_llm_crawler(payload, req)
        self.assertEqual(ctx.exception.status_code, 503)

    async def test_get_job_status(self):
        req = _FakeRequest(headers={"x-role": "admin"})
        fake_job = {
            "jobId": "llmcrawler-test-1",
            "status": "done",
            "progress": 100,
            "result": {"result_version": "1.0"},
            "error": None,
            "requestId": "req-1",
        }
        with patch("app.tools.llmCrawler.router.settings.FEATURE_LLM_CRAWLER", True), patch(
            "app.tools.llmCrawler.router.get_job_record", return_value=fake_job
        ):
            response = await get_llm_crawler_job("llmcrawler-test-1", req)
        self.assertEqual(response.get("status"), "done")
        self.assertEqual(response.get("progress"), 100)


if __name__ == "__main__":
    unittest.main()
