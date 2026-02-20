import shutil
import sys
import types
import unittest
from pathlib import Path

from docx import Document

# Make DOCX generator import independent from runtime-only settings dependency.
if "app.config" not in sys.modules:
    fake_config = types.ModuleType("app.config")
    fake_config.settings = types.SimpleNamespace(REPORTS_DIR=".")
    sys.modules["app.config"] = fake_config

from app.reports.docx_generator import DOCXGenerator


class BotDocxReportTests(unittest.TestCase):
    def test_bot_docx_contains_trend_section(self):
        data = {
            "url": "https://example.com",
            "results": {
                "retry_profile": "standard",
                "criticality_profile": "balanced",
                "sla_profile": "standard",
                "bots_checked": ["Googlebot Desktop", "GPTBot"],
                "summary": {
                    "total": 2,
                    "accessible": 1,
                    "crawlable": 1,
                    "renderable": 1,
                    "indexable": 1,
                    "non_indexable": 1,
                    "waf_cdn_detected": 1,
                    "avg_response_time_ms": 280,
                },
                "category_stats": [],
                "priority_blockers": [],
                "playbooks": [],
                "host_consistency": {"consistent": True, "notes": []},
                "baseline_diff": {"has_baseline": False, "message": "No baseline found."},
                "trend": {
                    "history_count": 2,
                    "latest": {
                        "timestamp": "2026-02-20T12:00:00",
                        "indexable": 1,
                        "crawlable": 1,
                        "renderable": 1,
                        "avg_response_time_ms": 280,
                        "critical_issues": 1,
                        "warning_issues": 0,
                    },
                    "previous": {
                        "timestamp": "2026-02-19T12:00:00",
                        "indexable": 0,
                        "crawlable": 0,
                        "renderable": 0,
                        "avg_response_time_ms": 360,
                        "critical_issues": 2,
                        "warning_issues": 1,
                    },
                    "delta_vs_previous": {
                        "indexable": 1,
                        "critical_issues": -1,
                        "avg_response_time_ms": -80,
                    },
                    "history": [
                        {
                            "timestamp": "2026-02-20T12:00:00",
                            "indexable": 1,
                            "crawlable": 1,
                            "renderable": 1,
                            "avg_response_time_ms": 280,
                            "critical_issues": 1,
                            "warning_issues": 0,
                        },
                        {
                            "timestamp": "2026-02-19T12:00:00",
                            "indexable": 0,
                            "crawlable": 0,
                            "renderable": 0,
                            "avg_response_time_ms": 360,
                            "critical_issues": 2,
                            "warning_issues": 1,
                        },
                    ],
                },
                "recommendations": [],
            },
        }

        temp_dir = Path("tests") / ".tmp_bot_docx"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            generator = DOCXGenerator()
            generator.reports_dir = str(temp_dir)
            report_path = generator.generate_bot_report("bot-docx-test", data)

            doc = Document(report_path)
            text = "\n".join(p.text for p in doc.paragraphs)
            self.assertIn("7. Trend History", text)
            self.assertIn("Delta vs previous:", text)
            self.assertIn("indexable 1", text)
            self.assertIn("avg response ms -80", text)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
