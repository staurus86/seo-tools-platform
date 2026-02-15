import unittest
from pathlib import Path
import sys
import types
import shutil

from openpyxl import load_workbook

# Make XLSX generator import independent from runtime-only settings dependency.
if "app.config" not in sys.modules:
    fake_config = types.ModuleType("app.config")
    fake_config.settings = types.SimpleNamespace(REPORTS_DIR=".")
    sys.modules["app.config"] = fake_config

from app.reports.xlsx_generator import XLSXGenerator


class SiteProXlsxLayoutTests(unittest.TestCase):
    def test_quick_report_has_8_sheets_and_unique_parameter_headers(self):
        data = {
            "url": "https://site.test",
            "results": {
                "mode": "quick",
                "summary": {
                    "total_pages": 1,
                    "internal_pages": 1,
                    "issues_total": 1,
                    "critical_issues": 0,
                    "warning_issues": 1,
                    "info_issues": 0,
                    "score": 91.0,
                },
                "pages": [
                    {
                        "url": "https://site.test",
                        "final_url": "https://site.test",
                        "status_code": 200,
                        "response_time_ms": 220,
                        "html_size_bytes": 15320,
                        "dom_nodes_count": 340,
                        "redirect_count": 0,
                        "is_https": True,
                        "compression_enabled": True,
                        "cache_enabled": True,
                        "indexable": True,
                        "health_score": 91.0,
                        "title": "Home",
                        "meta_description": "Main page",
                        "canonical": "https://site.test",
                        "meta_robots": "index,follow",
                        "schema_count": 1,
                        "hreflang_count": 0,
                        "mobile_friendly_hint": True,
                        "word_count": 330,
                        "unique_word_count": 180,
                        "lexical_diversity": 0.545,
                        "readability_score": 84.2,
                        "toxicity_score": 0.0,
                        "filler_ratio": 0.03,
                        "h1_count": 1,
                        "images_count": 8,
                        "images_without_alt": 1,
                        "external_nofollow_links": 1,
                        "external_follow_links": 2,
                        "outgoing_internal_links": 6,
                        "incoming_internal_links": 3,
                        "outgoing_external_links": 3,
                        "orphan_page": False,
                        "topic_hub": True,
                        "pagerank": 100.0,
                        "topic_label": "home",
                        "top_terms": ["home", "services", "seo"],
                        "duplicate_title_count": 1,
                        "duplicate_description_count": 1,
                        "weak_anchor_ratio": 0.08,
                        "link_quality_score": 96.5,
                        "ai_markers_count": 0,
                        "recommendation": "Maintain page quality and monitor regressions.",
                        "issues": [{"severity": "warning", "code": "thin_content", "title": "Thin", "details": ""}],
                    }
                ],
                "issues": [
                    {"severity": "warning", "url": "https://site.test", "code": "thin_content", "title": "Thin", "details": ""}
                ],
                "pipeline": {
                    "tf_idf": [{"url": "https://site.test", "top_terms": ["home", "services", "seo"]}]
                },
            },
        }

        temp_dir = Path("tests") / ".tmp_site_pro_xlsx"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            generator = XLSXGenerator()
            generator.reports_dir = str(temp_dir)
            report_path = generator.generate_site_audit_pro_report("site-pro-layout-test", data)

            wb = load_workbook(report_path)
            expected_sheets = [
                "1_Executive",
                "2_OnPage+Structured",
                "3_Technical",
                "4_Content+AI",
                "5_LinkGraph",
                "6_Images+External",
                "7_HierarchyErrors",
                "8_Keywords",
            ]
            self.assertEqual(wb.sheetnames[:8], expected_sheets)

            allow_repeats = {"URL", "Severity"}
            used_headers = {}
            for sheet_name in expected_sheets[1:]:
                ws = wb[sheet_name]
                headers = [cell.value for cell in ws[1] if cell.value]
                for header in headers:
                    if header in allow_repeats:
                        continue
                    self.assertNotIn(
                        header,
                        used_headers,
                        msg=f"Header '{header}' is duplicated in '{used_headers.get(header)}' and '{sheet_name}'",
                    )
                    used_headers[header] = sheet_name

            # Smoke-check that key metric families are represented exactly once.
            required_headers = {
                "Status",
                "Response ms",
                "Schema count",
                "Word count",
                "PageRank",
                "External nofollow",
                "Code",
                "Top terms (TF-IDF)",
            }
            self.assertTrue(required_headers.issubset(set(used_headers.keys())))

            self.assertTrue(Path(report_path).exists())
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_full_report_adds_deep_sheets(self):
        data = {
            "url": "https://site.test",
            "results": {
                "mode": "full",
                "summary": {"total_pages": 1, "issues_total": 0, "critical_issues": 0, "warning_issues": 0, "info_issues": 0},
                "pages": [{"url": "https://site.test", "topic_label": "home", "top_terms": ["home"], "issues": []}],
                "issues": [],
                "pipeline": {
                    "tf_idf": [{"url": "https://site.test", "top_terms": ["home"]}],
                    "semantic_linking_map": [
                        {"source_url": "https://site.test", "target_url": "https://site.test/a", "topic": "home", "reason": "test"}
                    ],
                    "duplicates": {"title_groups": [], "description_groups": []},
                },
            },
        }

        temp_dir = Path("tests") / ".tmp_site_pro_xlsx_full"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            generator = XLSXGenerator()
            generator.reports_dir = str(temp_dir)
            report_path = generator.generate_site_audit_pro_report("site-pro-layout-full", data)
            wb = load_workbook(report_path)
            self.assertIn("9_Indexability", wb.sheetnames)
            self.assertIn("10_StructuredData", wb.sheetnames)
            self.assertIn("11_Trust_EEAT", wb.sheetnames)
            self.assertIn("12_Topics_Semantics", wb.sheetnames)
            self.assertIn("13_AI_Markers", wb.sheetnames)
            self.assertIn("CrawlBudget", wb.sheetnames)
            self.assertIn("14_Issues_Raw", wb.sheetnames)
            self.assertIn("15_ActionPlan", wb.sheetnames)

            self.assertNotIn("13_MainReport_Compat", wb.sheetnames)
            self.assertNotIn("29_AIMarkers_Compat", wb.sheetnames)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
