import unittest

from app.tools.llmCrawler.policies import evaluate_profile_access, parse_robots_rules


ROBOTS_SAMPLE = """
User-agent: *
Disallow: /private
Allow: /private/public

User-agent: gptbot
Disallow: /
"""


class LlmCrawlerPolicyTests(unittest.TestCase):
    def test_parse_robots_rules(self):
        rules = parse_robots_rules(ROBOTS_SAMPLE)
        self.assertGreaterEqual(len(rules), 3)

    def test_ai_profile_disallowed(self):
        rules = parse_robots_rules(ROBOTS_SAMPLE)
        result = evaluate_profile_access(
            rules=rules,
            profile="ai-bot",
            url="https://example.com/",
        )
        self.assertFalse(result.get("allowed"))
        self.assertIn("Matched", str(result.get("reason")))

    def test_generic_profile_allowed_for_public_path(self):
        rules = parse_robots_rules(ROBOTS_SAMPLE)
        result = evaluate_profile_access(
            rules=rules,
            profile="generic-bot",
            url="https://example.com/private/public",
        )
        self.assertTrue(result.get("allowed"))


if __name__ == "__main__":
    unittest.main()

