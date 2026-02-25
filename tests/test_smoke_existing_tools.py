import importlib.util
import unittest
from unittest.mock import patch

from fastapi import BackgroundTasks


class ExistingToolsSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def test_healthcheck_endpoint(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.main import health_check

        payload = await health_check()
        self.assertEqual(payload.get("status"), "healthy")

    async def test_robots_endpoint_smoke(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import RobotsCheckRequest, create_robots_check

        fake_result = {"task_type": "robots_check", "url": "https://example.com", "results": {"ok": True}}
        with patch("app.api.routes.check_robots_full", return_value=fake_result):
            response = await create_robots_check(RobotsCheckRequest(url="example.com"))
        self.assertEqual(response.get("status"), "SUCCESS")

    async def test_render_endpoint_smoke(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import RenderAuditRequest, create_render_audit

        with patch("app.api.routes.check_render_full", return_value={"task_type": "render_audit", "results": {}}):
            response = await create_render_audit(RenderAuditRequest(url="https://example.com"), BackgroundTasks())
        self.assertEqual(response.get("status"), "PENDING")

    async def test_redirect_checker_endpoint_smoke(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import RedirectCheckerRequest, create_redirect_checker

        fake_result = {"task_type": "redirect_checker", "url": "https://example.com/", "results": {"summary": {}}}
        with patch("app.api.routes.check_redirect_checker_full", return_value=fake_result):
            response = await create_redirect_checker(RedirectCheckerRequest(url="example.com"))
        self.assertEqual(response.get("status"), "SUCCESS")


if __name__ == "__main__":
    unittest.main()
