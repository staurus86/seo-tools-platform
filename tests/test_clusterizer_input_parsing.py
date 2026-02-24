import importlib.util
import unittest


class ClusterizerInputParsingTests(unittest.TestCase):
    def _load_parser(self):
        if importlib.util.find_spec("multipart") is None:
            self.skipTest("python-multipart is not installed in this environment")
        from app.api.routes import _collect_clusterizer_keyword_rows

        return _collect_clusterizer_keyword_rows

    def test_parses_quoted_multiline_keyword_frequency_blocks(self):
        parser = self._load_parser()
        raw = """
подготовка к триатлону онлайн
подготовка к триатлону за месяц
"подготовка к триатлону прикол
0"
"подготовка к триатлону питание
0"
подготовка к триатлону олимпийка
"подготовка к триатлону айронмен
0"
""".strip()

        rows = parser([], raw)
        keywords = [str(row.get("keyword") or "").strip().lower() for row in rows]

        self.assertIn("подготовка к триатлону онлайн", keywords)
        self.assertIn("подготовка к триатлону прикол", keywords)
        self.assertIn("подготовка к триатлону питание", keywords)
        self.assertIn("подготовка к триатлону айронмен", keywords)
        self.assertNotIn("0", keywords)
        self.assertNotIn('0"', keywords)


if __name__ == "__main__":
    unittest.main()

