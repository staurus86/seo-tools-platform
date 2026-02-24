import unittest
from io import BytesIO
import importlib.util

from fastapi import BackgroundTasks, HTTPException
from starlette.datastructures import UploadFile

from app.config import settings


class LinkProfileRouteLimitsTests(unittest.IsolatedAsyncioTestCase):
    def _get_route(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import create_link_profile_audit

        return create_link_profile_audit

    def setUp(self):
        self._attr_names = [
            "LINK_PROFILE_MAX_BACKLINK_FILES",
            "LINK_PROFILE_MAX_FILE_SIZE_BYTES",
            "LINK_PROFILE_MAX_BATCH_FILE_SIZE_BYTES",
            "LINK_PROFILE_MAX_TOTAL_UPLOAD_BYTES",
        ]
        self._orig_values = {name: getattr(settings, name, None) for name in self._attr_names}
        self._had_attr = {name: hasattr(settings, name) for name in self._attr_names}

    def tearDown(self):
        for name in self._attr_names:
            if self._had_attr.get(name):
                setattr(settings, name, self._orig_values.get(name))
            elif hasattr(settings, name):
                delattr(settings, name)

    async def test_rejects_too_many_backlink_files(self):
        settings.LINK_PROFILE_MAX_BACKLINK_FILES = 1
        settings.LINK_PROFILE_MAX_FILE_SIZE_BYTES = 1024
        settings.LINK_PROFILE_MAX_TOTAL_UPLOAD_BYTES = 4096

        file_one = UploadFile(filename="one.csv", file=BytesIO(b"a,b\n1,2\n"))
        file_two = UploadFile(filename="two.csv", file=BytesIO(b"a,b\n3,4\n"))
        route = self._get_route()
        with self.assertRaises(HTTPException) as ctx:
            await route(
                background_tasks=BackgroundTasks(),
                our_domain="our.com",
                backlink_files=[file_one, file_two],
            )
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("Слишком много файлов бэклинков", str(ctx.exception.detail))

    async def test_rejects_oversized_backlink_file(self):
        settings.LINK_PROFILE_MAX_BACKLINK_FILES = 5
        settings.LINK_PROFILE_MAX_FILE_SIZE_BYTES = 10
        settings.LINK_PROFILE_MAX_TOTAL_UPLOAD_BYTES = 1024

        large_file = UploadFile(filename="big.csv", file=BytesIO(b"x" * 11))
        route = self._get_route()
        with self.assertRaises(HTTPException) as ctx:
            await route(
                background_tasks=BackgroundTasks(),
                our_domain="our.com",
                backlink_files=[large_file],
            )
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("превышает лимит", str(ctx.exception.detail))

    async def test_rejects_oversized_batch_file(self):
        settings.LINK_PROFILE_MAX_BACKLINK_FILES = 5
        settings.LINK_PROFILE_MAX_FILE_SIZE_BYTES = 1024
        settings.LINK_PROFILE_MAX_BATCH_FILE_SIZE_BYTES = 5
        settings.LINK_PROFILE_MAX_TOTAL_UPLOAD_BYTES = 2048

        backlinks = UploadFile(filename="ok.csv", file=BytesIO(b"a,b\n1,2\n"))
        batch = UploadFile(filename="batch.csv", file=BytesIO(b"x" * 6))
        route = self._get_route()
        with self.assertRaises(HTTPException) as ctx:
            await route(
                background_tasks=BackgroundTasks(),
                our_domain="our.com",
                backlink_files=[backlinks],
                batch_file=batch,
            )
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("Batch файл превышает лимит", str(ctx.exception.detail))


if __name__ == "__main__":
    unittest.main()
