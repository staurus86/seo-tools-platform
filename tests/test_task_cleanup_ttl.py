import os
import shutil
import time
import unittest
from pathlib import Path
from unittest.mock import patch
import sys
import types

# Keep test independent from runtime-only settings deps.
if "app.config" not in sys.modules:
    fake_config = types.ModuleType("app.config")
    fake_config.settings = types.SimpleNamespace(REPORTS_DIR="reports_output", MAX_REPORT_AGE_DAYS=7)
    sys.modules["app.config"] = fake_config

from app.core.task_cleanup import prune_stale_report_artifacts


class TaskCleanupTtlTests(unittest.TestCase):
    def test_prune_stale_report_artifacts_removes_old_files(self):
        root = Path("tests") / ".tmp_reports_cleanup"
        if root.exists():
            shutil.rmtree(root, ignore_errors=True)
        (root / "site_pro" / "task-a").mkdir(parents=True, exist_ok=True)
        (root / "site_pro" / "task-b").mkdir(parents=True, exist_ok=True)

        old_file = root / "site_pro" / "task-a" / "issues_part-001.jsonl"
        new_file = root / "site_pro" / "task-b" / "issues_part-001.jsonl"
        old_file.write_text('{"a":1}\n', encoding="utf-8")
        new_file.write_text('{"b":2}\n', encoding="utf-8")

        now = time.time()
        old_mtime = now - (10 * 24 * 3600)
        new_mtime = now - (1 * 24 * 3600)
        os.utime(old_file, (old_mtime, old_mtime))
        os.utime(new_file, (new_mtime, new_mtime))

        with patch("app.core.task_cleanup.settings.REPORTS_DIR", str(root)):
            summary = prune_stale_report_artifacts(max_age_days=7)

        self.assertTrue(summary["deleted_files"] >= 1)
        self.assertFalse(old_file.exists(), "Old artifact should be deleted")
        self.assertTrue(new_file.exists(), "Recent artifact should remain")
        # Empty directory of task-a should be removed by cleanup.
        self.assertFalse((root / "site_pro" / "task-a").exists())

        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
