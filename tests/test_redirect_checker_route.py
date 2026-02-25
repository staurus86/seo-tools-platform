import unittest
import importlib.util
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


if __name__ == "__main__":
    unittest.main()
