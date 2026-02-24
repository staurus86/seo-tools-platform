import unittest

from app.tools.clusterizer.input_parser import collect_clusterizer_keyword_rows


class ClusterizerInputParsingTests(unittest.TestCase):
    def test_parses_quoted_multiline_keyword_frequency_blocks(self):
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

        rows = collect_clusterizer_keyword_rows([], raw)
        keywords = [str(row.get("keyword") or "").strip().lower() for row in rows]

        self.assertIn("подготовка к триатлону онлайн", keywords)
        self.assertIn("подготовка к триатлону прикол", keywords)
        self.assertIn("подготовка к триатлону питание", keywords)
        self.assertIn("подготовка к триатлону айронмен", keywords)
        self.assertNotIn("0", keywords)
        self.assertNotIn('0"', keywords)

        by_keyword = {str(row.get("keyword") or "").strip().lower(): float(row.get("frequency") or 0.0) for row in rows}
        self.assertEqual(by_keyword.get("подготовка к триатлону прикол"), 0.0)
        self.assertEqual(by_keyword.get("подготовка к триатлону питание"), 0.0)

    def test_parses_quoted_tab_frequency(self):
        rows = collect_clusterizer_keyword_rows([], "\"триатлон теория и практика\\t0\"")
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0].get("keyword") or ""), "триатлон теория и практика")
        self.assertEqual(float(rows[0].get("frequency")), 0.0)


if __name__ == "__main__":
    unittest.main()
