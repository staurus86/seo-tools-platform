import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

from app.tools.site_pro.service import SiteAuditProService


class SiteProArtifactsTests(unittest.TestCase):
    def test_full_mode_emits_chunk_manifest_and_files(self):
        service = SiteAuditProService()
        task_id = "sitepro-artifacts-test"
        chunk_root = Path("reports_output") / "site_pro" / task_id
        legacy_chunk_root = Path("site_pro") / task_id
        if chunk_root.exists():
            shutil.rmtree(chunk_root, ignore_errors=True)
        if legacy_chunk_root.exists():
            shutil.rmtree(legacy_chunk_root, ignore_errors=True)

        public_payload = {
            "summary": {"total_pages": 2, "issues_total": 2},
            "pages": [
                {"url": "https://site.test", "status_code": 200, "health_score": 90, "topic_label": "home", "recommendation": "Keep"},
                {"url": "https://site.test/a", "status_code": 200, "health_score": 88, "topic_label": "a", "recommendation": "Fix alt"},
            ],
            "issues": [
                {"url": "https://site.test", "severity": "warning", "code": "x", "title": "X", "details": ""},
                {"url": "https://site.test/a", "severity": "info", "code": "y", "title": "Y", "details": ""},
            ],
            "pipeline": {
                "semantic_linking_map": [
                    {"source_url": "https://site.test", "target_url": "https://site.test/a", "topic": "home", "reason": "related"}
                ]
            },
            "artifacts": {},
        }

        with patch.object(service.adapter, "run", return_value=object()), patch.object(
            service.adapter, "to_public_results", return_value=public_payload
        ):
            result = service.run(url="https://site.test", task_id=task_id, mode="full", max_pages=5)

        chunk_manifest = (((result or {}).get("results") or {}).get("artifacts") or {}).get("chunk_manifest", {})
        self.assertEqual(chunk_manifest.get("task_id"), task_id)
        chunks = chunk_manifest.get("chunks", [])
        self.assertTrue(chunks)

        all_files = [f for chunk in chunks for f in (chunk.get("files") or [])]
        self.assertTrue(all_files)
        for meta in all_files:
            path = Path(meta.get("path", ""))
            self.assertTrue(path.exists(), msg=f"Chunk file does not exist: {path}")
            self.assertTrue(str(meta.get("download_url", "")).startswith("/api/site-pro-artifacts/"))

        shutil.rmtree(chunk_root, ignore_errors=True)
        shutil.rmtree(legacy_chunk_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
