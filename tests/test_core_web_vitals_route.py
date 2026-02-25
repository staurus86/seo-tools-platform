import importlib.util
import unittest
from unittest.mock import patch
from fastapi import BackgroundTasks


class CoreWebVitalsRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_route_queues_single_task(self):
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
            response = await create_core_web_vitals(payload, BackgroundTasks())

        self.assertEqual(response.get("status"), "PENDING")
        task_id = str(response.get("task_id", ""))
        self.assertTrue(task_id.startswith("cwv-"))
        stored = get_task_result(task_id)
        self.assertIsNotNone(stored)
        self.assertEqual((stored or {}).get("task_type"), "core_web_vitals")
        self.assertEqual((stored or {}).get("status"), "PENDING")

    async def test_route_queues_batch_task(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import CoreWebVitalsRequest, create_core_web_vitals, get_task_result

        payload = CoreWebVitalsRequest(
            scan_mode="batch",
            strategy="desktop",
            batch_urls=["example.com", "https://example.org/"],
        )
        response = await create_core_web_vitals(payload, BackgroundTasks())

        self.assertEqual(response.get("status"), "PENDING")
        task_id = str(response.get("task_id", ""))
        self.assertTrue(task_id.startswith("cwv-"))
        stored = get_task_result(task_id)
        self.assertIsNotNone(stored)
        self.assertEqual((stored or {}).get("task_type"), "core_web_vitals")
        self.assertEqual((stored or {}).get("status"), "PENDING")

    async def test_route_rejects_batch_over_limit(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import CoreWebVitalsRequest, create_core_web_vitals
        from fastapi import HTTPException

        payload = CoreWebVitalsRequest(
            scan_mode="batch",
            strategy="desktop",
            batch_urls=[f"https://example{i}.com/" for i in range(11)],
        )
        with self.assertRaises(HTTPException) as ctx:
            await create_core_web_vitals(payload, BackgroundTasks())
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("максимум 10", str(ctx.exception.detail))


if __name__ == "__main__":
    unittest.main()
