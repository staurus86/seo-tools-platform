import unittest

from app.tools.llmCrawler.scoring import compute_score


class LlmCrawlerScoringTests(unittest.TestCase):
    def test_score_contains_breakdown(self):
        nojs = {
            "status_code": 200,
            "challenge": {"is_challenge": False},
            "meta": {"meta_robots": "", "x_robots_tag": ""},
            "content": {"main_text_length": 800},
            "headings": {"h1": 1, "h2": 3, "h3": 2},
            "structure": {"lists_count": 1, "tables_count": 1},
            "schema": {"jsonld_types": ["Organization"]},
            "signals": {"author_present": True, "date_present": True},
        }
        rendered = {
            "content": {"main_text_length": 1200},
            "headings": {"h1": 1, "h2": 4, "h3": 2},
            "structure": {"lists_count": 1, "tables_count": 0},
            "schema": {"jsonld_types": ["Organization", "WebSite"]},
            "signals": {"author_present": True, "date_present": True},
        }
        diff = {"textCoverage": 0.8}
        policies = {"robots": {"profiles": {"ai-bot": {"allowed": True}}}}

        score = compute_score(nojs=nojs, rendered=rendered, diff=diff, policies=policies)
        self.assertIsInstance(score.get("total"), int)
        self.assertIn("breakdown", score)
        self.assertIn("access", score["breakdown"])
        self.assertIn("content", score["breakdown"])
        self.assertIn("structure", score["breakdown"])
        self.assertIn("signals", score["breakdown"])

    def test_noindex_reduces_access_score(self):
        baseline_nojs = {
            "status_code": 200,
            "challenge": {"is_challenge": False},
            "meta": {"meta_robots": "", "x_robots_tag": ""},
            "content": {"main_text_length": 1500},
            "headings": {"h1": 1, "h2": 2, "h3": 1},
            "structure": {"lists_count": 1, "tables_count": 1},
            "schema": {"jsonld_types": []},
            "signals": {"author_present": False, "date_present": False},
        }
        noindex_nojs = dict(baseline_nojs)
        noindex_nojs["meta"] = {"meta_robots": "noindex", "x_robots_tag": ""}
        empty = {"robots": {"profiles": {"ai-bot": {"allowed": True}}}}

        base_score = compute_score(
            nojs=baseline_nojs,
            rendered=None,
            diff={"textCoverage": None},
            policies=empty,
        )
        noindex_score = compute_score(
            nojs=noindex_nojs,
            rendered=None,
            diff={"textCoverage": None},
            policies=empty,
        )
        self.assertLess(noindex_score["breakdown"]["access"], base_score["breakdown"]["access"])


if __name__ == "__main__":
    unittest.main()

