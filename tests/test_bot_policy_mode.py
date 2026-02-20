import unittest

from app.tools.bots.service_v2 import BotAccessibilityServiceV2


class BotPolicyModeTests(unittest.TestCase):
    def test_ai_expected_blocks_are_excluded_from_priority_blockers(self):
        rows = [
            {
                "bot_name": "GPTBot",
                "category": "AI",
                "accessible": False,
                "crawlable": False,
                "has_content": False,
                "robots_allowed": False,
                "x_robots_forbidden": False,
                "meta_forbidden": False,
                "waf_cdn_signal": {"detected": True},
            },
            {
                "bot_name": "Googlebot Desktop",
                "category": "Google",
                "accessible": False,
                "crawlable": False,
                "has_content": False,
                "robots_allowed": False,
                "x_robots_forbidden": False,
                "meta_forbidden": False,
                "waf_cdn_signal": {"detected": False},
            },
        ]

        strict_service = BotAccessibilityServiceV2(ai_block_expected=False)
        strict_blockers = strict_service._build_priority_blockers(rows)
        strict_affected = sum(int(x.get("affected_bots", 0) or 0) for x in strict_blockers)
        self.assertGreaterEqual(strict_affected, 2)

        policy_service = BotAccessibilityServiceV2(ai_block_expected=True)
        policy_blockers = policy_service._build_priority_blockers(rows)
        policy_affected = sum(int(x.get("affected_bots", 0) or 0) for x in policy_blockers)
        self.assertGreaterEqual(policy_affected, 1)
        self.assertLess(policy_affected, strict_affected)

        policy_issues = policy_service._build_issues(rows)
        ai_issue = next((x for x in policy_issues if x.get("bot") == "GPTBot"), {})
        self.assertEqual(ai_issue.get("severity"), "info")
        self.assertIn("Expected", str(ai_issue.get("title", "")))


if __name__ == "__main__":
    unittest.main()
