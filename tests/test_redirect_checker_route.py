import unittest
import importlib.util
import shutil
from pathlib import Path
from unittest.mock import patch


class RedirectCheckerRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_route_creates_success_task(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import RedirectCheckerRequest, create_redirect_checker, get_task_result

        fake_result = {
            "task_type": "redirect_checker",
            "url": "https://example.com/",
            "results": {
                "summary": {
                    "total_scenarios": 11,
                    "passed": 11,
                    "warnings": 0,
                    "errors": 0,
                    "quality_score": 100,
                    "quality_grade": "A",
                },
                "scenarios": [],
            },
        }

        with patch("app.api.routes.check_redirect_checker_full", return_value=fake_result):
            payload = RedirectCheckerRequest(url="example.com", user_agent="googlebot_desktop")
            response = await create_redirect_checker(payload)

        self.assertEqual(response.get("status"), "SUCCESS")
        task_id = str(response.get("task_id", ""))
        self.assertTrue(task_id.startswith("redirect-"))
        stored = get_task_result(task_id)
        self.assertIsNotNone(stored)
        self.assertEqual((stored or {}).get("task_type"), "redirect_checker")

    async def test_route_passes_policy_parameters(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import RedirectCheckerRequest, create_redirect_checker

        fake_result = {
            "task_type": "redirect_checker",
            "url": "https://example.com/",
            "results": {
                "summary": {"total_scenarios": 17, "passed": 17, "warnings": 0, "errors": 0},
                "scenarios": [],
            },
        }

        with patch("app.api.routes.check_redirect_checker_full", return_value=fake_result) as mock_check:
            payload = RedirectCheckerRequest(
                url="example.com",
                user_agent="googlebot_desktop",
                canonical_host_policy="non-www",
                trailing_slash_policy="no-slash",
                enforce_lowercase=False,
                allowed_query_params=["page", "sort"],
                required_query_params=["page"],
                ignore_query_params=["utm_source", "gclid"],
            )
            response = await create_redirect_checker(payload)

        self.assertEqual(response.get("status"), "SUCCESS")
        self.assertEqual(mock_check.call_count, 1)
        _, kwargs = mock_check.call_args
        self.assertEqual(kwargs.get("user_agent"), "googlebot_desktop")
        self.assertEqual(kwargs.get("canonical_host_policy"), "non-www")
        self.assertEqual(kwargs.get("trailing_slash_policy"), "no-slash")
        self.assertEqual(kwargs.get("enforce_lowercase"), False)
        self.assertEqual(kwargs.get("allowed_query_params"), ["page", "sort"])
        self.assertEqual(kwargs.get("required_query_params"), ["page"])
        self.assertEqual(kwargs.get("ignore_query_params"), ["utm_source", "gclid"])

    async def test_export_redirect_checker_docx(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")

        from fastapi.responses import Response
        from app.api.routes import (
            ExportRequest,
            create_task_result,
            export_redirect_checker_docx,
        )
        from app.reports.docx_generator import docx_generator

        temp_dir = Path("tests") / ".tmp_redirect_docx_route"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)

        original_reports_dir = docx_generator.reports_dir
        try:
            docx_generator.reports_dir = str(temp_dir)
            task_id = "redirect-docx-route-test"
            fake_result = {
                "task_type": "redirect_checker",
                "url": "https://example.com/",
                "results": {
                    "checked_url": "https://example.com/",
                    "selected_user_agent": {"key": "googlebot_desktop", "label": "Googlebot Desktop"},
                    "summary": {
                        "total_scenarios": 11,
                        "passed": 9,
                        "warnings": 1,
                        "errors": 1,
                        "quality_score": 78,
                        "quality_grade": "B",
                        "duration_ms": 820,
                    },
                    "scenarios": [
                        {
                            "id": 1,
                            "key": "http_to_https",
                            "title": "HTTP -> HTTPS",
                            "what_checked": "Редирект с http:// на https://",
                            "status": "error",
                            "expected": "Постоянный редирект 301/308 на HTTPS",
                            "actual": "200 | Final: http://example.com/",
                            "recommendation": "Настройте принудительный HTTPS.",
                            "test_url": "http://example.com/",
                            "response_codes": [200],
                            "final_url": "http://example.com/",
                            "hops": 0,
                            "chain": [{"url": "http://example.com/", "status_code": 200, "location": ""}],
                            "error": "",
                        },
                        {
                            "id": 8,
                            "key": "canonical_tag",
                            "title": "Canonical тег",
                            "what_checked": "Наличие canonical",
                            "status": "warning",
                            "expected": "Canonical присутствует",
                            "actual": "canonical не найден",
                            "recommendation": "Добавьте canonical в <head>.",
                            "test_url": "https://example.com/",
                            "response_codes": [200],
                            "final_url": "https://example.com/",
                            "hops": 0,
                            "chain": [{"url": "https://example.com/", "status_code": 200, "location": ""}],
                            "error": "",
                        },
                    ],
                    "recommendations": [
                        "Настройте принудительный HTTPS.",
                        "Добавьте canonical в <head>.",
                    ],
                    "checked_at": "2026-02-25T10:00:00Z",
                },
            }
            create_task_result(task_id, "redirect_checker", "https://example.com/", fake_result)

            response = await export_redirect_checker_docx(ExportRequest(task_id=task_id))
            self.assertIsInstance(response, Response)
            self.assertEqual(
                response.media_type,
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
            self.assertIn("redirect_checker_report_example.com_", response.headers.get("content-disposition", ""))
            self.assertGreater(len(response.body), 0)
        finally:
            docx_generator.reports_dir = original_reports_dir
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
