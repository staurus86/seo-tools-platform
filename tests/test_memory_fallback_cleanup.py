import time
import unittest
from unittest.mock import patch

from app.api.routers import _task_store
from app.config import settings
from app.core.progress import ProgressTracker


class TaskStoreMemoryCleanupTests(unittest.TestCase):
    def setUp(self):
        _task_store.task_results_memory.clear()
        _task_store._task_updated_at.clear()
        _task_store._task_last_access_at.clear()
        _task_store._task_payload_size_bytes.clear()

    def tearDown(self):
        _task_store.task_results_memory.clear()
        _task_store._task_updated_at.clear()
        _task_store._task_last_access_at.clear()
        _task_store._task_payload_size_bytes.clear()

    @patch("app.api.routers._task_store.get_redis_client", return_value=None)
    def test_aggressive_idle_cleanup_removes_terminal_tasks(self, _redis_mock):
        with patch.object(settings, "TASK_STORE_MEMORY_MAX_ITEMS", 100, create=True), \
             patch.object(settings, "TASK_STORE_MEMORY_TTL_SEC", 7200, create=True), \
             patch.object(settings, "TASK_STORE_IDLE_KEEP_SEC", 60, create=True):
            _task_store.create_task_pending("task-running", "clusterizer", "keywords:1")
            _task_store.create_task_pending("task-done", "clusterizer", "keywords:1")
            _task_store.update_task_state("task-done", status="SUCCESS", result={"ok": True})

            # Simulate stale terminal task in idle period.
            _task_store._task_last_access_at["task-done"] = time.time() - 3600
            summary = _task_store.cleanup_task_results_memory(idle_seconds=3600, aggressive=True)

            self.assertIsNotNone(_task_store.get_task_result("task-running"))
            self.assertIsNone(_task_store.get_task_result("task-done"))
            self.assertGreaterEqual(summary["removed_idle"], 1)


class ProgressMemoryCleanupTests(unittest.TestCase):
    def test_progress_ttl_cleanup_removes_expired_entries(self):
        tracker = ProgressTracker()
        tracker._redis_next_retry_ts = time.time() + 3600

        with patch.object(settings, "PROGRESS_MEMORY_TTL_SEC", 30, create=True), \
             patch.object(settings, "PROGRESS_MEMORY_MAX_ITEMS", 100, create=True), \
             patch.object(settings, "PROGRESS_IDLE_KEEP_SEC", 60, create=True):
            tracker.update_progress("progress-1", current=1, total=10, message="start")
            tracker._memory_updated_at["progress-1"] = time.time() - 600

            summary = tracker.cleanup_memory(idle_seconds=0.0, aggressive=False)

            self.assertIsNone(tracker.get_progress("progress-1"))
            self.assertGreaterEqual(summary["removed_expired"], 1)


if __name__ == "__main__":
    unittest.main()
