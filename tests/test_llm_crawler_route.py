import unittest
from datetime import datetime, timezone
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from app.tools.llmCrawler.router import (
    get_llm_crawler_job,
    llm_crawler_quality_gate,
    llm_crawler_report,
    llm_crawler_report_docx,
    run_llm_crawler,
)
from app.tools.llmCrawler.schemas import LlmCrawlerRunRequest


class _FakeRequest:
    def __init__(self, headers=None, client_host="127.0.0.1"):
        self.headers = headers or {}
        self.client = SimpleNamespace(host=client_host)


class LlmCrawlerRouteTests(unittest.IsolatedAsyncioTestCase):
    def _healthy_heartbeat(self):
        return {"updatedAt": datetime.now(timezone.utc).isoformat()}

    async def test_feature_flag_off_returns_403(self):
        payload = LlmCrawlerRunRequest(url="example.com")
        req = _FakeRequest()
        with patch("app.tools.llmCrawler.router.settings.FEATURE_LLM_CRAWLER", False), patch(
            "app.tools.llmCrawler.router.settings.LLM_CRAWLER_ALLOW_ADMIN", False
        ):
            with self.assertRaises(HTTPException) as ctx:
                await run_llm_crawler(payload, req)
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_run_queues_job_when_enabled(self):
        payload = LlmCrawlerRunRequest(url="example.com")
        req = _FakeRequest(headers={"x-role": "admin"})
        _rate_ok = {"allowed": True, "remaining": 5, "reset_in": 60}
        with patch("app.tools.llmCrawler.router.settings.FEATURE_LLM_CRAWLER", True), patch(
            "app.tools.llmCrawler.router.get_worker_heartbeat", return_value=self._healthy_heartbeat()
        ), patch(
            "app.tools.llmCrawler.router.check_rate_limit", return_value=_rate_ok
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

    async def test_quality_gate_admin(self):
        req = _FakeRequest(headers={"x-role": "admin"})
        with patch("app.tools.llmCrawler.router.run_quality_gate_from_file", return_value={"gate": {"status": "pass"}}):
            response = await llm_crawler_quality_gate(req)
        self.assertIn("gate", response)

    async def test_quality_gate_forbidden_non_admin(self):
        req = _FakeRequest(headers={"x-role": "user"})
        with patch("app.tools.llmCrawler.router.settings.FEATURE_LLM_CRAWLER", True):
            with self.assertRaises(HTTPException) as ctx:
                await llm_crawler_quality_gate(req)
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_quality_gate_returns_500_on_runner_error(self):
        req = _FakeRequest(headers={"x-role": "admin"})
        with patch("app.tools.llmCrawler.router.settings.FEATURE_LLM_CRAWLER", True), patch(
            "app.tools.llmCrawler.router.run_quality_gate_from_file", side_effect=RuntimeError("broken benchmark")
        ):
            with self.assertRaises(HTTPException) as ctx:
                await llm_crawler_quality_gate(req)
        self.assertEqual(ctx.exception.status_code, 500)

    async def test_docx_report_respects_v3_flag(self):
        req = _FakeRequest(headers={"x-role": "admin"})
        fake_job = {"jobId": "llmcrawler-test-1", "result": {"score": {"total": 70}}}
        with patch("app.tools.llmCrawler.router.settings.FEATURE_LLM_CRAWLER", True), patch(
            "app.tools.llmCrawler.router.settings.LLM_REPORT_V3_ENABLED", False
        ), patch(
            "app.tools.llmCrawler.router.get_job_record", return_value=fake_job
        ), patch(
            "app.tools.llmCrawler.report_docx.build_docx_v2", return_value=BytesIO(b"docx-bytes")
        ) as mock_build:
            response = await llm_crawler_report_docx("llmcrawler-test-1", req)
        self.assertEqual(response.status_code, 200)
        mock_build.assert_called_once_with(fake_job, "llmcrawler-test-1", wow_enabled=False)

    async def test_html_report_shows_v3_flag_message_when_disabled(self):
        req = _FakeRequest(headers={"x-role": "admin"})
        fake_job = {"jobId": "llmcrawler-test-1", "result": {"score": {"total": 70}, "ai_understanding": {}, "ai_answer_preview": {}}}
        with patch("app.tools.llmCrawler.router.settings.FEATURE_LLM_CRAWLER", True), patch(
            "app.tools.llmCrawler.router.settings.LLM_REPORT_V3_ENABLED", False
        ), patch(
            "app.tools.llmCrawler.router.get_job_record", return_value=fake_job
        ):
            response = await llm_crawler_report("llmcrawler-test-1", req)
        body = response.body.decode("utf-8", errors="ignore")
        self.assertIn("LLM_REPORT_V3_ENABLED=false", body)


if __name__ == "__main__":
    unittest.main()
