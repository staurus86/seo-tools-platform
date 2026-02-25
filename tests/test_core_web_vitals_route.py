import importlib.util
import unittest
from unittest.mock import patch


class CoreWebVitalsRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_route_creates_success_task(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import CoreWebVitalsRequest, create_core_web_vitals, get_task_result

        fake_result = {
            "task_type": "core_web_vitals",
            "url": "https://example.com/",
            "results": {
                "strategy": "desktop",
                "summary": {
                    "performance_score": 90,
                    "core_web_vitals_status": "good",
                },
                "metrics": {},
                "recommendations": [],
            },
        }

        with patch("app.api.routes.check_core_web_vitals", return_value=fake_result):
            payload = CoreWebVitalsRequest(url="example.com", strategy="desktop")
            response = await create_core_web_vitals(payload)

        self.assertEqual(response.get("status"), "SUCCESS")
        task_id = str(response.get("task_id", ""))
        self.assertTrue(task_id.startswith("cwv-"))
        stored = get_task_result(task_id)
        self.assertIsNotNone(stored)
        self.assertEqual((stored or {}).get("task_type"), "core_web_vitals")


if __name__ == "__main__":
    unittest.main()
