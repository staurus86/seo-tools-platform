import unittest
import importlib.util

from fastapi import BackgroundTasks, HTTPException

from app.config import settings


class ClusterizerRouteLimitsTests(unittest.IsolatedAsyncioTestCase):
    def _get_route_items(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import ClusterizerRequest, create_clusterizer_task

        return ClusterizerRequest, create_clusterizer_task

    def setUp(self):
        self._had_attr = hasattr(settings, "CLUSTERIZER_MAX_KEYWORDS")
        self._orig_value = getattr(settings, "CLUSTERIZER_MAX_KEYWORDS", None)

    def tearDown(self):
        if self._had_attr:
            setattr(settings, "CLUSTERIZER_MAX_KEYWORDS", self._orig_value)
        elif hasattr(settings, "CLUSTERIZER_MAX_KEYWORDS"):
            delattr(settings, "CLUSTERIZER_MAX_KEYWORDS")

    async def test_rejects_more_than_limit(self):
        ClusterizerRequest, create_clusterizer_task = self._get_route_items()

        settings.CLUSTERIZER_MAX_KEYWORDS = 3
        payload = ClusterizerRequest(keywords_text="a\nb\nc\nd")

        with self.assertRaises(HTTPException) as ctx:
            await create_clusterizer_task(payload, BackgroundTasks())

        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("Слишком много ключей", str(ctx.exception.detail))

    async def test_accepts_valid_payload(self):
        ClusterizerRequest, create_clusterizer_task = self._get_route_items()

        settings.CLUSTERIZER_MAX_KEYWORDS = 10
        payload = ClusterizerRequest(
            keywords_text="iphone 16\niphone 16 цена\nsamsung galaxy",
            similarity_threshold_pct=35,
            min_cluster_size=2,
        )
        response = await create_clusterizer_task(payload, BackgroundTasks())

        self.assertEqual(response.get("status"), "PENDING")
        self.assertTrue(str(response.get("task_id", "")).startswith("clusterizer-"))


if __name__ == "__main__":
    unittest.main()
