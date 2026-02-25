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


class RedirectCheckerDocxReportTests(unittest.TestCase):
    def test_redirect_checker_docx_contains_plan_and_tz_sections(self):
        data = {
            "url": "https://example.com/",
            "results": {
                "checked_url": "https://example.com/",
                "selected_user_agent": {
                    "key": "googlebot_desktop",
                    "label": "Googlebot Desktop",
                },
                "summary": {
                    "total_scenarios": 11,
                    "passed": 8,
                    "warnings": 2,
                    "errors": 1,
                    "quality_score": 71,
                    "quality_grade": "C",
                    "duration_ms": 1240,
                },
                "scenarios": [
                    {
                        "id": 1,
                        "key": "http_to_https",
                        "title": "HTTP -> HTTPS",
                        "what_checked": "Редирект с http:// на https://",
                        "status": "error",
                        "expected": "301/308 на HTTPS",
                        "actual": "200 на http://example.com/",
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
                        "what_checked": "Проверка canonical",
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

        temp_dir = Path("tests") / ".tmp_redirect_docx"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            generator = DOCXGenerator()
            generator.reports_dir = str(temp_dir)
            report_path = generator.generate_redirect_checker_report("redirect-docx-test", data)

            doc = Document(report_path)
            text = "\n".join(p.text for p in doc.paragraphs)
            self.assertIn("Отчет Redirect Checker", text)
            self.assertIn("3. Нарушения и разбор", text)
            self.assertIn("4. План действий при нарушениях", text)
            self.assertIn("6. Краткие ТЗ на исправление", text)
            self.assertIn("HTTP -> HTTPS", text)
            self.assertIn("Canonical тег", text)
            self.assertIn("Критерий приемки:", text)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
