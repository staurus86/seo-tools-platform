"""robots.txt parsing and policy evaluation for LLM crawler."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse


PROFILE_USER_AGENTS: Dict[str, List[str]] = {
    "generic-bot": ["generic-bot", "*"],
    "search-bot": ["googlebot", "bingbot", "yandexbot", "*"],
    "ai-bot": ["gptbot", "chatgpt-user", "claudebot", "anthropic-ai", "perplexitybot", "google-extended", "ccbot", "*"],
    "gptbot": ["gptbot", "chatgpt-user", "gptbot-fetch"],
    "chatgpt-user": ["chatgpt-user"],
    "claudebot": ["claudebot", "anthropic-ai"],
    "perplexitybot": ["perplexitybot", "spbot"],
    "google-extended": ["google-extended"],
    "ccbot": ["ccbot"],
}


@dataclass
class RobotsRule:
    user_agent: str
    directive: str
    path: str
    line: int


def parse_robots_rules(content: str) -> List[RobotsRule]:
    rules: List[RobotsRule] = []
    current_agents: List[str] = []
    for idx, raw in enumerate((content or "").splitlines(), start=1):
        line = str(raw or "").strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        directive = key.strip().lower()
        payload = value.strip()
        if directive == "user-agent":
            ua = payload.lower()
            current_agents = [ua] if ua else []
            continue
        if directive not in {"allow", "disallow"}:
            continue
        if not current_agents:
            current_agents = ["*"]
        for agent in current_agents:
            rules.append(RobotsRule(user_agent=agent, directive=directive, path=payload or "", line=idx))
    return rules


def _rule_matches_path(rule_path: str, test_path: str) -> bool:
    rp = str(rule_path or "").strip()
    if rp == "":
        return True
    # Basic robots wildcard support.
    escaped = re.escape(rp).replace(r"\*", ".*")
    if rp.endswith("$"):
        escaped = escaped[:-2] + "$"
    pattern = "^" + escaped
    return bool(re.match(pattern, test_path))


def _pick_best_match(rules: List[RobotsRule], test_path: str) -> Tuple[RobotsRule | None, List[RobotsRule]]:
    matched = [rule for rule in rules if _rule_matches_path(rule.path, test_path)]
    if not matched:
        return None, []
    matched.sort(key=lambda r: (len(r.path or ""), 1 if r.directive == "allow" else 0), reverse=True)
    return matched[0], matched


def evaluate_profile_access(
    *,
    rules: List[RobotsRule],
    profile: str,
    url: str,
) -> Dict[str, Any]:
    parsed = urlparse(url)
    test_path = parsed.path or "/"
    profile_agents = PROFILE_USER_AGENTS.get(profile, ["*"])
    candidates = [
        rule
        for rule in rules
        if str(rule.user_agent or "").lower() in profile_agents
    ]
    best, matched = _pick_best_match(candidates, test_path)
    if not best:
        return {
            "profile": profile,
            "allowed": True,
            "reason": "No matching robots rule, default allow",
            "matched_rule": None,
            "matched_candidates": [],
        }
    allowed = best.directive == "allow" or (best.directive == "disallow" and best.path == "")
    return {
        "profile": profile,
        "allowed": bool(allowed),
        "reason": (
            f"Matched {best.directive.upper()} '{best.path}' for user-agent '{best.user_agent}' (line {best.line})"
        ),
        "matched_rule": {
            "user_agent": best.user_agent,
            "directive": best.directive,
            "path": best.path,
            "line": best.line,
        },
        "matched_candidates": [
            {"directive": r.directive, "path": r.path, "line": r.line, "user_agent": r.user_agent}
            for r in matched[:10]
        ],
    }
