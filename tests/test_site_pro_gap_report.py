import importlib.util
import shutil
import textwrap
import unittest
from pathlib import Path


def _load_gap_report_module():
    root = Path(__file__).resolve().parents[1]
    module_path = root / "scripts" / "site_pro_gap_report.py"
    spec = importlib.util.spec_from_file_location("site_pro_gap_report", str(module_path))
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class SiteProGapReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = Path("tests") / ".tmp_gap_report"
        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_extract_analyze_page_keys_from_fixture(self):
        module = _load_gap_report_module()
        sample = textwrap.dedent(
            """
            def helper():
                return {}

            def analyze_page(url):
                x = 1
                return {
                    'status': 200,
                    'title': 'Home',
                    'word_count': 123
                }

            def another():
                return None
            """
        ).strip()
        legacy_path = self.tmp_dir / "seopro.py"
        legacy_path.write_text(sample, encoding="utf-8")
        keys = module.extract_analyze_page_keys(legacy_path)
        self.assertEqual(keys, ["status", "title", "word_count"])

    def test_resolve_legacy_seopro_path_finds_root_seopro_old(self):
        module = _load_gap_report_module()
        root = self.tmp_dir / "root1"
        root.mkdir(parents=True, exist_ok=True)
        legacy = root / "seopro-Old.py"
        legacy.write_text("def analyze_page(url):\n    return {}\n", encoding="utf-8")
        resolved = module.resolve_legacy_seopro_path(root)
        self.assertEqual(resolved.name, "seopro-Old.py")

    def test_resolve_legacy_seopro_path_uses_explicit_path(self):
        module = _load_gap_report_module()
        root = self.tmp_dir / "root2"
        root.mkdir(parents=True, exist_ok=True)
        explicit = root / "legacy.py"
        explicit.write_text("def analyze_page(url):\n    return {}\n", encoding="utf-8")
        resolved = module.resolve_legacy_seopro_path(root, explicit_legacy_path=str(explicit))
        self.assertEqual(resolved, explicit)


if __name__ == "__main__":
    unittest.main()
