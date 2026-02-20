"""
Bot accessibility checker v2.

Designed for API usage (no CLI/interactive behavior).
"""
from __future__ import annotations

import re
import time
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class BotDefinition:
    name: str
    user_agent: str
    category: str


BOT_DEFINITIONS: List[BotDefinition] = [
    BotDefinition("Googlebot Desktop", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", "Google"),
    BotDefinition("Googlebot Smartphone", "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", "Google"),
    BotDefinition("Googlebot News", "Googlebot-News", "Google"),
    BotDefinition("Googlebot Image", "Googlebot-Image/1.0", "Google"),
    BotDefinition("Googlebot Video", "Googlebot-Video/1.0", "Google"),
    BotDefinition("AdsBot Google", "AdsBot-Google (+http://www.google.com/adsbot.html)", "Google"),
    BotDefinition("YandexBot", "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)", "Yandex"),
    BotDefinition("YandexMobileBot", "Mozilla/5.0 (iPhone; CPU iPhone OS 8_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12B411 Safari/600.1.4 (compatible; YandexMobileBot/3.0; +http://yandex.com/bots)", "Yandex"),
    BotDefinition("YandexImages", "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)", "Yandex"),
    BotDefinition("YandexVideo", "Mozilla/5.0 (compatible; YandexVideo/3.0; +http://yandex.com/bots)", "Yandex"),
    BotDefinition("YandexNews", "Mozilla/5.0 (compatible; YandexNews/4.0; +http://yandex.com/bots)", "Yandex"),
    BotDefinition("YandexMetrika", "Mozilla/5.0 (Windows NT 6.1; rv:18.0) Gecko/20100101 Firefox/18.0 (compatible; YandexMetrika/3.0; +http://yandex.com/bots)", "Yandex"),
    BotDefinition("Bingbot", "Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)", "Bing"),
    BotDefinition("Bingbot Mobile", "Mozilla/5.0 (iPhone; CPU iPhone OS 7_0 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11A465 Safari/9537.53 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)", "Bing"),
    BotDefinition("BingPreview", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534+ (KHTML, like Gecko) BingPreview/1.0b", "Bing"),
    BotDefinition("DuckDuckBot", "Mozilla/5.0 (compatible; DuckDuckBot-Https/1.1; https://duckduckgo.com/duckduckbot)", "Search"),
    BotDefinition("Baiduspider", "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)", "Search"),
    BotDefinition("Yahoo Slurp", "Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)", "Search"),
    BotDefinition("Applebot", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Applebot/0.1", "Search"),
    BotDefinition("AhrefsBot", "Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)", "SEO Crawler"),
    BotDefinition("SemrushBot", "Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)", "SEO Crawler"),
    BotDefinition("MJ12bot", "Mozilla/5.0 (compatible; MJ12bot/v1.4.8; http://mj12bot.com/)", "SEO Crawler"),
    BotDefinition("DotBot", "Mozilla/5.0 (compatible; DotBot/1.1; http://www.opensiteexplorer.org/dotbot, help@moz.com)", "SEO Crawler"),
    BotDefinition("BLEXBot", "Mozilla/5.0 (compatible; BLEXBot/1.0; +http://webmeup-crawler.com/)", "SEO Crawler"),
    BotDefinition("Facebook External Hit", "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)", "Social"),
    BotDefinition("Twitterbot", "Twitterbot/1.0", "Social"),
    BotDefinition("LinkedInBot", "LinkedInBot/1.0 (compatible; Mozilla/5.0; Jakarta Commons-HttpClient/3.1 +http://www.linkedin.com)", "Social"),
    BotDefinition("Slackbot", "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)", "Social"),
    BotDefinition("TelegramBot", "TelegramBot (like TwitterBot)", "Social"),
    BotDefinition("Discordbot", "Mozilla/5.0 (compatible; Discordbot/2.0; +https://discordapp.com)", "Social"),
    BotDefinition("WhatsApp", "WhatsApp/2.0", "Social"),
    BotDefinition("Pinterest", "Pinterest/0.2 (+http://www.pinterest.com/bot.html)", "Social"),
    BotDefinition("ChatGPT-User", "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; ChatGPT-User/1.0; +https://openai.com/bot", "AI"),
    BotDefinition("GPTBot", "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; GPTBot/1.0; +https://openai.com/gptbot", "AI"),
    BotDefinition("OpenAI Bot", "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; OpenAIbot/1.0; +https://openai.com/bot", "AI"),
    BotDefinition("Google-Extended", "Mozilla/5.0 (compatible; Google-Extended/1.0; +http://www.google.com/bot.html)", "AI"),
    BotDefinition("Gemini", "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Gemini-AI/1.0; +https://developers.google.com/search/docs/crawling-indexing/google-common-crawlers)", "AI"),
    BotDefinition("Google-CloudVertexBot", "Mozilla/5.0 (compatible; Google-CloudVertexBot/1.0; +https://cloud.google.com/vertex-ai)", "AI"),
    BotDefinition("DeepSeekBot", "Mozilla/5.0 (compatible; DeepseekBot/1.0; +https://www.deepseek.com/bot)", "AI"),
    BotDefinition("ClaudeBot", "Mozilla/5.0 (compatible; ClaudeBot/1.0; +https://claude.ai/bot)", "AI"),
    BotDefinition("Claude-Web", "Mozilla/5.0 (compatible; Claude-Web/1.0; +https://claude.ai)", "AI"),
    BotDefinition("Anthropic AI", "Mozilla/5.0 (compatible; Anthropic-AI/1.0; +https://anthropic.com/bot)", "AI"),
    BotDefinition("Meta-ExternalAgent", "Mozilla/5.0 (compatible; Meta-ExternalAgent/1.0; +https://meta.com/bot)", "AI"),
    BotDefinition("LlamaBot", "Mozilla/5.0 (compatible; LlamaBot/1.0; +https://meta.com/llama)", "AI"),
    BotDefinition("Amazonbot", "Mozilla/5.0 (compatible; Amazonbot/1.0; +https://developer.amazon.com/support/amazonbot)", "AI"),
    BotDefinition("Applebot-Extended", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Applebot-Extended/1.0", "AI"),
    BotDefinition("PerplexityBot", "Mozilla/5.0 (compatible; PerplexityBot/1.0; +https://perplexity.ai/bot)", "AI"),
    BotDefinition("Perplexity-User", "Mozilla/5.0 (compatible; Perplexity-User/1.0; +https://perplexity.ai)", "AI"),
    BotDefinition("YouBot", "Mozilla/5.0 (compatible; YouBot/1.0; +https://you.com/bot)", "AI"),
    BotDefinition("CohereBot", "Mozilla/5.0 (compatible; CohereBot/1.0; +https://cohere.com/bot)", "AI"),
    BotDefinition("ConsensusBot", "Mozilla/5.0 (compatible; ConsensusBot/1.0; +https://consensus.app/bot)", "AI"),
    BotDefinition("ElicitBot", "Mozilla/5.0 (compatible; ElicitBot/1.0; +https://elicit.org/bot)", "AI"),
    BotDefinition("CCBot", "Mozilla/5.0 (compatible; CCBot/2.0; +https://commoncrawl.org/faq/)", "AI"),
    BotDefinition("Diffbot", "Mozilla/5.0 (compatible; Diffbot/1.0; +https://www.diffbot.com/bot)", "AI"),
    BotDefinition("HuggingFace", "Mozilla/5.0 (compatible; HuggingFace/1.0; +https://huggingface.co/bot)", "AI"),
    BotDefinition("Replicate", "Mozilla/5.0 (compatible; Replicate/1.0; +https://replicate.com/bot)", "AI"),
    BotDefinition("Midjourney Bot", "Mozilla/5.0 (compatible; MidjourneyBot/1.0; +https://midjourney.com/bot)", "AI"),
    BotDefinition("Stable Diffusion", "Mozilla/5.0 (compatible; StableDiffusion/1.0; +https://stability.ai/bot)", "AI"),
    BotDefinition("Codeium", "Mozilla/5.0 (compatible; Codeium/1.0; +https://codeium.com/bot)", "AI"),
    BotDefinition("Cursor.sh", "Mozilla/5.0 (compatible; Cursor/1.0; +https://cursor.sh/bot)", "AI"),
    BotDefinition("GitHub Copilot", "Mozilla/5.0 (compatible; GitHub-Copilot/1.0; +https://github.com/features/copilot)", "AI"),
]

DEFAULT_BOT_NAMES: List[str] = [
    "YandexBot",
    "Googlebot Desktop",
    "Bingbot",
    "ChatGPT-User",
    "ClaudeBot",
    "Gemini",
    "DeepSeekBot",
    "SemrushBot",
    "AhrefsBot",
]

BOT_GROUPS: Dict[str, List[str]] = {
    "search": ["Google", "Yandex", "Bing", "Search"],
    "ai": ["AI"],
    "crawlers": ["SEO Crawler"],
}

UA_DESKTOP_FALLBACK = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

BOT_CATEGORY_CRITICALITY: Dict[str, float] = {
    "Google": 1.0,
    "Yandex": 1.0,
    "Bing": 0.9,
    "Search": 0.8,
    "AI": 0.7,
    "SEO Crawler": 0.5,
    "Social": 0.3,
}

CRITICALITY_PROFILES: Dict[str, Dict[str, float]] = {
    "balanced": BOT_CATEGORY_CRITICALITY,
    "search_first": {
        "Google": 1.0,
        "Yandex": 1.0,
        "Bing": 1.0,
        "Search": 0.9,
        "AI": 0.5,
        "SEO Crawler": 0.4,
        "Social": 0.2,
    },
    "ai_first": {
        "Google": 0.9,
        "Yandex": 0.8,
        "Bing": 0.8,
        "Search": 0.7,
        "AI": 1.0,
        "SEO Crawler": 0.4,
        "Social": 0.2,
    },
}

SLA_PROFILES: Dict[str, Dict[str, float]] = {
    "standard": {
        "Google": 98.0,
        "Yandex": 98.0,
        "Bing": 95.0,
        "Search": 92.0,
        "AI": 90.0,
        "SEO Crawler": 85.0,
        "Social": 80.0,
    },
    "strict": {
        "Google": 99.0,
        "Yandex": 99.0,
        "Bing": 98.0,
        "Search": 95.0,
        "AI": 93.0,
        "SEO Crawler": 88.0,
        "Social": 82.0,
    },
}

RETRY_PROFILES: Dict[str, Dict[str, Any]] = {
    "strict": {"retries": 1, "backoff": 0.2, "timeout": 10},
    "standard": {"retries": 2, "backoff": 0.4, "timeout": 15},
    "aggressive": {"retries": 4, "backoff": 0.6, "timeout": 22},
}


def normalize_url(raw_url: str) -> str:
    cleaned = (raw_url or "").strip()
    if not cleaned:
        return cleaned
    if cleaned.startswith(("http://", "https://")):
        return cleaned
    return f"https://{cleaned}"


def _is_forbidden_directive(content: str) -> bool:
    if not content:
        return False
    val = content.lower()
    tokens = [t for t in re.split(r"[,\s]+", val) if t]
    forbidden = {"noindex", "nofollow", "none", "noarchive", "nosnippet", "notranslate", "noimageindex"}
    return bool(forbidden.intersection(tokens))


def _extract_meta_robots(html: str) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", attrs={"name": re.compile(r"^robots$", re.I)})
    if not meta:
        return None
    return (meta.get("content") or "").strip() or None


def _extract_response_sample(html_head: str, limit: int = 380) -> str:
    if not html_head:
        return ""
    try:
        soup = BeautifulSoup(html_head, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = " ".join((soup.get_text(" ", strip=True) or "").split())
    except Exception:
        text = " ".join(str(html_head).split())
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _extract_visible_text(html_head: str) -> str:
    if not html_head:
        return ""
    try:
        soup = BeautifulSoup(html_head, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        return " ".join((soup.get_text(" ", strip=True) or "").split()).lower()
    except Exception:
        return " ".join(str(html_head).split()).lower()


def _normalize_batch_urls(raw_urls: Optional[List[str]], limit: int = 100) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in (raw_urls or []):
        u = normalize_url(str(item or "").strip())
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
        if len(out) >= limit:
            break
    return out


def _parse_robots_groups(robots_text: str) -> List[Dict[str, Any]]:
    groups: List[Dict[str, Any]] = []
    current_group: Optional[Dict[str, Any]] = None
    for raw in (robots_text or "").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = [x.strip() for x in line.split(":", 1)]
        key_l = key.lower()
        if key_l == "user-agent":
            ua = value.lower()
            if current_group is None or current_group.get("rules"):
                current_group = {"user_agents": [], "rules": []}
                groups.append(current_group)
            current_group["user_agents"].append(ua)
            continue
        if key_l in ("allow", "disallow"):
            if current_group is None:
                continue
            current_group["rules"].append({"directive": key_l, "pattern": value})
    return groups


def _bot_tokens(bot_name: str, user_agent: str) -> List[str]:
    tokens: List[str] = []
    raw = [bot_name or "", user_agent or ""]
    for value in raw:
        val = value.lower()
        parts = re.split(r"[^a-z0-9]+", val)
        for part in parts:
            if len(part) >= 3 and part not in tokens:
                tokens.append(part)
        agent = val.split("/", 1)[0].strip()
        if agent and agent not in tokens:
            tokens.append(agent)
    return tokens


def _ua_group_match_score(group_ua: str, tokens: List[str]) -> int:
    ua = (group_ua or "").lower().strip()
    if not ua:
        return -1
    if ua == "*":
        return 1
    if ua in tokens:
        return 100 + len(ua)
    for token in tokens:
        if token and ua in token:
            return 60 + len(ua)
        if token and token in ua:
            return 40 + len(token)
    return -1


def _robots_pattern_to_regex(pattern: str) -> str:
    p = (pattern or "").strip()
    if not p:
        return r"^"
    escaped = re.escape(p)
    escaped = escaped.replace(r"\*", ".*")
    if escaped.endswith(r"\$"):
        escaped = escaped[:-2] + "$"
    else:
        escaped = escaped + ".*"
    return r"^" + escaped


def _robots_rule_matches(path: str, pattern: str) -> bool:
    p = (pattern or "").strip()
    if p == "":
        return True
    try:
        return re.match(_robots_pattern_to_regex(p), path or "/") is not None
    except Exception:
        return False


def _evaluate_robots_for_path(bot_name: str, user_agent: str, robots_text: Optional[str], path: str) -> Dict[str, Any]:
    if not robots_text:
        return {
            "allowed": None,
            "matched_user_agent": None,
            "matched_rule": None,
            "matched_pattern": None,
            "explain": "robots.txt not found",
        }

    groups = _parse_robots_groups(robots_text)
    tokens = _bot_tokens(bot_name, user_agent)
    scored_groups: List[Tuple[int, Dict[str, Any]]] = []
    for group in groups:
        uas = group.get("user_agents", []) or []
        score = max((_ua_group_match_score(ua, tokens) for ua in uas), default=-1)
        if score >= 0:
            scored_groups.append((score, group))
    if not scored_groups:
        return {
            "allowed": None,
            "matched_user_agent": None,
            "matched_rule": None,
            "matched_pattern": None,
            "explain": "no matching user-agent group in robots.txt",
        }

    scored_groups.sort(key=lambda x: x[0], reverse=True)
    selected_group = scored_groups[0][1]
    selected_uas = selected_group.get("user_agents", []) or []
    selected_ua = selected_uas[0] if selected_uas else "*"

    matched_rule: Optional[Dict[str, Any]] = None
    matched_len = -1
    for rule in (selected_group.get("rules", []) or []):
        directive = str(rule.get("directive") or "").lower()
        pattern = str(rule.get("pattern") or "")
        if directive not in ("allow", "disallow"):
            continue
        if not _robots_rule_matches(path, pattern):
            continue
        plen = len(pattern.replace("*", ""))
        if plen > matched_len:
            matched_len = plen
            matched_rule = {"directive": directive, "pattern": pattern}
        elif plen == matched_len and matched_rule is not None:
            # Tie-break: Allow wins over Disallow.
            if matched_rule.get("directive") == "disallow" and directive == "allow":
                matched_rule = {"directive": directive, "pattern": pattern}

    if matched_rule is None:
        return {
            "allowed": True,
            "matched_user_agent": selected_ua,
            "matched_rule": "none",
            "matched_pattern": "",
            "explain": f"default allow (no matching allow/disallow rule for path '{path}')",
        }

    directive = matched_rule.get("directive")
    allowed = directive != "disallow"
    return {
        "allowed": allowed,
        "matched_user_agent": selected_ua,
        "matched_rule": directive,
        "matched_pattern": matched_rule.get("pattern", ""),
        "explain": (
            f"matched {directive} rule '{matched_rule.get('pattern', '')}' "
            f"for UA group '{selected_ua}' and path '{path}'"
        ),
    }


def _robots_allows_bot(bot_name: str, user_agent: str, robots_text: Optional[str], path: str = "/") -> Optional[bool]:
    return _evaluate_robots_for_path(bot_name, user_agent, robots_text, path).get("allowed")


class BotAccessibilityServiceV2:
    def __init__(
        self,
        timeout: int = 15,
        max_workers: int = 10,
        retry_profile: str = "standard",
        criticality_profile: str = "balanced",
        sla_profile: str = "standard",
        baseline_enabled: bool = True,
        ai_block_expected: bool = False,
    ):
        profile = RETRY_PROFILES.get(str(retry_profile or "standard").lower(), RETRY_PROFILES["standard"])
        self.retry_profile = str(retry_profile or "standard").lower()
        self.timeout = max(3, int(timeout or profile["timeout"] or 15))
        self.max_workers = max(1, int(max_workers or 10))
        self.retries = max(0, int(profile["retries"]))
        self.backoff = float(profile["backoff"])
        self.criticality_profile_name = str(criticality_profile or "balanced").lower()
        self.sla_profile_name = str(sla_profile or "standard").lower()
        self.category_weights = dict(CRITICALITY_PROFILES.get(self.criticality_profile_name, CRITICALITY_PROFILES["balanced"]))
        self.category_sla = dict(SLA_PROFILES.get(self.sla_profile_name, SLA_PROFILES["standard"]))
        self.baseline_enabled = bool(baseline_enabled)
        self.ai_block_expected = bool(ai_block_expected)
        self.trend_history_limit = 50
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.retries,
            backoff_factor=self.backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=100)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _baseline_file_path(self, domain: str) -> str:
        safe = re.sub(r"[^a-z0-9._-]+", "_", (domain or "site").lower())
        base_dir = os.path.join("reports_output", "bot_baselines")
        os.makedirs(base_dir, exist_ok=True)
        return os.path.join(base_dir, f"{safe}.json")

    def _load_baseline(self, domain: str) -> Optional[Dict[str, Any]]:
        path = self._baseline_file_path(domain)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return payload if isinstance(payload, dict) else None
        except Exception:
            return None

    def _save_baseline(self, domain: str, summary: Dict[str, Any]) -> None:
        if not self.baseline_enabled:
            return
        path = self._baseline_file_path(domain)
        payload = {
            "updated_at": datetime.utcnow().isoformat(),
            "summary": summary or {},
        }
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _compute_baseline_diff(self, baseline: Optional[Dict[str, Any]], current_summary: Dict[str, Any]) -> Dict[str, Any]:
        if not baseline:
            return {"has_baseline": False, "message": "No baseline found for this domain.", "metrics": []}
        base_summary = (baseline.get("summary") or {}) if isinstance(baseline, dict) else {}
        metrics_to_compare = [
            "accessible",
            "indexable",
            "non_indexable",
            "robots_disallowed",
            "x_robots_forbidden",
            "meta_forbidden",
            "avg_response_time_ms",
        ]
        rows: List[Dict[str, Any]] = []
        for key in metrics_to_compare:
            cur = current_summary.get(key, 0)
            prev = base_summary.get(key, 0)
            try:
                delta = float(cur) - float(prev)
            except Exception:
                delta = 0.0
            rows.append({"metric": key, "current": cur, "baseline": prev, "delta": round(delta, 2)})
        return {
            "has_baseline": True,
            "baseline_updated_at": baseline.get("updated_at"),
            "metrics": rows,
        }

    def _trend_file_path(self, domain: str) -> str:
        safe = re.sub(r"[^a-z0-9._-]+", "_", (domain or "site").lower())
        base_dir = os.path.join("reports_output", "bot_trends")
        os.makedirs(base_dir, exist_ok=True)
        return os.path.join(base_dir, f"{safe}.json")

    def _load_trend_history(self, domain: str) -> List[Dict[str, Any]]:
        path = self._trend_file_path(domain)
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                rows = payload.get("history", [])
            else:
                rows = payload
            return rows if isinstance(rows, list) else []
        except Exception:
            return []

    def _save_trend_history(self, domain: str, rows: List[Dict[str, Any]]) -> None:
        path = self._trend_file_path(domain)
        payload = {
            "updated_at": datetime.utcnow().isoformat(),
            "history": rows[: self.trend_history_limit],
        }
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _append_trend_snapshot(
        self,
        domain: str,
        url: str,
        summary: Dict[str, Any],
        completed_at: str,
    ) -> Dict[str, Any]:
        history = self._load_trend_history(domain)
        snapshot = {
            "timestamp": completed_at,
            "url": url,
            "total": int(summary.get("total", 0) or 0),
            "crawlable": int(summary.get("crawlable", 0) or 0),
            "renderable": int(summary.get("renderable", 0) or 0),
            "accessible": int(summary.get("accessible", 0) or 0),
            "indexable": int(summary.get("indexable", 0) or 0),
            "non_indexable": int(summary.get("non_indexable", 0) or 0),
            "avg_response_time_ms": float(summary.get("avg_response_time_ms", 0) or 0),
            "critical_issues": int(summary.get("critical_issues", 0) or 0),
            "warning_issues": int(summary.get("warning_issues", 0) or 0),
            "info_issues": int(summary.get("info_issues", 0) or 0),
            "waf_cdn_detected": int(summary.get("waf_cdn_detected", 0) or 0),
            "retry_profile": self.retry_profile,
            "criticality_profile": self.criticality_profile_name,
            "sla_profile": self.sla_profile_name,
        }
        history = [x for x in history if str(x.get("timestamp", "")) != str(snapshot["timestamp"])]
        history.insert(0, snapshot)
        history = history[: self.trend_history_limit]
        self._save_trend_history(domain, history)

        previous = history[1] if len(history) > 1 else None
        if previous:
            delta = {
                "indexable": int(snapshot.get("indexable", 0)) - int(previous.get("indexable", 0) or 0),
                "critical_issues": int(snapshot.get("critical_issues", 0)) - int(previous.get("critical_issues", 0) or 0),
                "avg_response_time_ms": round(
                    float(snapshot.get("avg_response_time_ms", 0)) - float(previous.get("avg_response_time_ms", 0) or 0),
                    2,
                ),
            }
        else:
            delta = None
        return {
            "history_count": len(history),
            "latest": snapshot,
            "previous": previous,
            "delta_vs_previous": delta,
            "history": history[:10],
        }

    def _detect_waf_cdn(self, response: Optional[requests.Response], html_head: str, error: Optional[str]) -> Dict[str, Any]:
        headers = {k.lower(): v for k, v in ((response.headers if response is not None else {}) or {}).items()}
        body = _extract_visible_text(html_head)
        err = (error or "").lower()
        status_code = int(getattr(response, "status_code", 0) or 0) if response is not None else 0

        provider = ""
        reason = ""
        confidence = 0.0

        if "cf-ray" in headers or "cloudflare" in str(headers.get("server", "")).lower():
            provider = "Cloudflare"
            confidence = max(confidence, 0.25)
        if any(k in headers for k in ("x-akamai-transformed", "akamai-cache-status")) or "akamai" in str(headers.get("server", "")).lower():
            provider = provider or "Akamai"
            confidence = max(confidence, 0.25)
        if "x-sucuri-id" in headers or "sucuri" in str(headers.get("server", "")).lower():
            provider = provider or "Sucuri"
            confidence = max(confidence, 0.25)
        if "x-ddos-protection" in headers:
            provider = provider or "DDoS protection"
            confidence = max(confidence, 0.3)

        strong_body_markers = [
            "attention required",
            "verify you are human",
            "captcha",
            "cf-chl",
            "cloudflare ray id",
            "ddos protection by",
            "access denied | sucuri website firewall",
        ]
        weak_body_markers = [
            "access denied",
            "request blocked",
            "security check",
            "automated queries",
            "forbidden",
        ]
        has_strong_marker = any(m in body for m in strong_body_markers)
        weak_hits = sum(1 for m in weak_body_markers if m in body)
        if has_strong_marker:
            reason = "strong challenge signature in response body"
            confidence = max(confidence, 0.9)
        elif weak_hits > 0:
            # Weak markers alone are not enough on successful pages.
            if status_code in (401, 403, 429, 503):
                reason = "challenge/block hints in response body with restrictive status"
                confidence = max(confidence, 0.75)
            elif provider:
                reason = "weak challenge hints in response body"
                confidence = max(confidence, 0.45)
        if "ssl" in err and "handshake" in err:
            reason = reason or "tls handshake blocked"
            confidence = max(confidence, 0.65)
        if "429" in err or (response is not None and response.status_code == 429):
            reason = reason or "rate limiting"
            confidence = max(confidence, 0.7)
        if response is not None and response.status_code in (401, 403):
            reason = reason or f"http {response.status_code} access restriction"
            confidence = max(confidence, 0.8)

        detected = confidence >= 0.7
        return {
            "detected": detected,
            "provider": provider or "unknown",
            "reason": reason or "",
            "confidence": round(confidence, 2),
        }

    def _host_consistency_check(self, url: str) -> Dict[str, Any]:
        parsed = urlparse(url)
        host = parsed.netloc
        if not host:
            return {"variants": [], "consistent": True, "notes": ["invalid host"]}

        hostname = parsed.hostname or host
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        host_variants = {hostname}
        if hostname.startswith("www."):
            host_variants.add(hostname[4:])
        else:
            host_variants.add(f"www.{hostname}")
        schemes = {"http", "https"}

        variants: List[Dict[str, Any]] = []
        for scheme in schemes:
            for hv in host_variants:
                target = f"{scheme}://{hv}{path}"
                try:
                    resp = self.session.get(target, timeout=max(3, min(self.timeout, 8)), allow_redirects=False, headers={"User-Agent": UA_DESKTOP_FALLBACK})
                    variants.append(
                        {
                            "url": target,
                            "status": resp.status_code,
                            "location": resp.headers.get("Location"),
                            "reachable": 200 <= resp.status_code < 500,
                        }
                    )
                except Exception as exc:
                    variants.append({"url": target, "status": None, "location": None, "reachable": False, "error": str(exc)})

        status_set = {v.get("status") for v in variants if v.get("status") is not None}
        redirect_hosts = set()
        for v in variants:
            loc = v.get("location")
            if not loc:
                continue
            try:
                lp = urlparse(loc)
                if lp.netloc:
                    redirect_hosts.add(lp.netloc.lower())
            except Exception:
                pass
        consistent = len(status_set) <= 2 and len(redirect_hosts) <= 2
        notes: List[str] = []
        if not consistent:
            notes.append("host variants return inconsistent status/redirect behavior")
        if any((v.get("status") in (401, 403)) for v in variants):
            notes.append("some host variants are protected or denied")
        return {"variants": variants, "consistent": consistent, "notes": notes}

    def _run_waf_bypass_probe(self, url: str) -> Dict[str, Any]:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        try:
            resp = self.session.get(url, headers=headers, timeout=max(3, min(self.timeout, 10)), allow_redirects=True)
            content_type = resp.headers.get("content-type", "")
            is_html = "text/html" in content_type.lower()
            html_head = (resp.text or "")[:120000] if is_html else ""
            return {
                "ok": True,
                "status": resp.status_code,
                "final_url": resp.url,
                "content_type": content_type,
                "sample": _extract_response_sample(html_head),
                "waf_cdn_signal": self._detect_waf_cdn(resp, html_head, None),
            }
        except Exception as exc:
            return {
                "ok": False,
                "status": None,
                "final_url": None,
                "content_type": None,
                "sample": "",
                "waf_cdn_signal": self._detect_waf_cdn(None, "", str(exc)),
                "error": str(exc),
            }

    def _load_robots(self, url: str) -> Tuple[Optional[str], Optional[int]]:
        robots_url = url.rstrip("/") + "/robots.txt"
        try:
            resp = self.session.get(robots_url, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.text, resp.status_code
            return None, resp.status_code
        except Exception:
            return None, None

    def _resolve_bots(self, selected_bots: Optional[List[str]], bot_groups: Optional[List[str]]) -> List[BotDefinition]:
        selected_names = {
            (name or "").strip().lower()
            for name in (selected_bots or DEFAULT_BOT_NAMES)
            if (name or "").strip()
        }
        selected_categories = set()
        for group in (bot_groups or []):
            selected_categories.update(BOT_GROUPS.get((group or "").strip().lower(), []))

        resolved: List[BotDefinition] = []
        for bot in BOT_DEFINITIONS:
            if bot.name.lower() in selected_names or bot.category in selected_categories:
                resolved.append(bot)

        if not resolved:
            fallback_names = {name.lower() for name in DEFAULT_BOT_NAMES}
            resolved = [bot for bot in BOT_DEFINITIONS if bot.name.lower() in fallback_names]
        return resolved

    def _check_one(self, url: str, bot: BotDefinition, robots_text: Optional[str]) -> Dict[str, Any]:
        headers = {
            "User-Agent": bot.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        started = time.perf_counter()
        try:
            response = self.session.get(url, headers=headers, timeout=self.timeout, allow_redirects=True)
            elapsed_ms = int((time.perf_counter() - started) * 1000)

            content_type = response.headers.get("content-type", "")
            is_html = "text/html" in content_type.lower()
            html_head = (response.text or "")[:120000] if is_html else ""
            meta_robots = _extract_meta_robots(html_head) if is_html else None
            x_robots = response.headers.get("X-Robots-Tag") or response.headers.get("X-Robots")
            final_parsed = urlparse(response.url or url)
            path = final_parsed.path or "/"
            if final_parsed.query:
                path = f"{path}?{final_parsed.query}"
            robots_eval = _evaluate_robots_for_path(bot.name, bot.user_agent, robots_text, path)
            robots_allowed = robots_eval.get("allowed")
            has_content = len(response.content or b"") > 0
            accessible = 200 <= response.status_code < 400
            crawlable = bool(accessible and robots_allowed is not False)
            waf_cdn = self._detect_waf_cdn(response, html_head, None)
            response_sample = _extract_response_sample(html_head)
            waf_blocks_render = bool((waf_cdn.get("detected")) and float(waf_cdn.get("confidence", 0.0) or 0.0) >= 0.85)
            renderable = bool(crawlable and has_content and not waf_blocks_render)
            indexable = bool(
                crawlable
                and has_content
                and robots_allowed is not False
                and not _is_forbidden_directive(x_robots or "")
                and not _is_forbidden_directive(meta_robots or "")
            )
            indexability_reasons: List[str] = []
            if not accessible:
                indexability_reasons.append(f"http_{response.status_code}")
            if robots_allowed is False:
                indexability_reasons.append("robots_disallow")
            if _is_forbidden_directive(x_robots or ""):
                indexability_reasons.append("x_robots_forbidden")
            if _is_forbidden_directive(meta_robots or ""):
                indexability_reasons.append("meta_robots_forbidden")
            if not has_content:
                indexability_reasons.append("empty_content")
            if (waf_cdn.get("detected") and float(waf_cdn.get("confidence", 0.0) or 0.0) >= 0.85):
                indexability_reasons.append("high_confidence_waf_challenge")
            if not indexability_reasons:
                indexability_reasons.append("indexable")
            blocked_reasons: List[str] = []
            if not accessible:
                blocked_reasons.append("http_error_or_denied")
            if robots_allowed is False:
                blocked_reasons.append("robots_disallow")
            if waf_cdn.get("detected"):
                blocked_reasons.append("waf_or_cdn_challenge")
            if _is_forbidden_directive(x_robots or "") or _is_forbidden_directive(meta_robots or ""):
                blocked_reasons.append("indexing_directive")
            category_weight = float(self.category_weights.get(bot.category, 0.5))
            sla_target_pct = float(self.category_sla.get(bot.category, 85.0))

            return {
                "bot_name": bot.name,
                "category": bot.category,
                "user_agent": bot.user_agent,
                "status": response.status_code,
                "accessible": accessible,
                "response_time_ms": elapsed_ms,
                "has_content": has_content,
                "x_robots_tag": x_robots,
                "x_robots_forbidden": _is_forbidden_directive(x_robots or ""),
                "meta_robots": meta_robots,
                "meta_forbidden": _is_forbidden_directive(meta_robots or ""),
                "robots_allowed": robots_allowed,
                "robots_evaluation": robots_eval,
                "content_type": content_type,
                "final_url": response.url,
                "crawlable": crawlable,
                "renderable": renderable,
                "indexable": indexable,
                "waf_cdn_signal": waf_cdn,
                "response_sample": response_sample,
                "indexability_reasons": indexability_reasons,
                "indexability_reason": " | ".join(indexability_reasons),
                "blocked_reasons": blocked_reasons,
                "bot_priority_weight": category_weight,
                "sla_target_pct": sla_target_pct,
                "error": None,
            }
        except Exception as exc:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            robots_eval = _evaluate_robots_for_path(bot.name, bot.user_agent, robots_text, "/")
            waf_cdn = self._detect_waf_cdn(None, "", str(exc))
            return {
                "bot_name": bot.name,
                "category": bot.category,
                "user_agent": bot.user_agent,
                "status": None,
                "accessible": False,
                "response_time_ms": elapsed_ms,
                "has_content": False,
                "x_robots_tag": None,
                "x_robots_forbidden": False,
                "meta_robots": None,
                "meta_forbidden": False,
                "robots_allowed": robots_eval.get("allowed"),
                "robots_evaluation": robots_eval,
                "content_type": None,
                "final_url": None,
                "crawlable": False,
                "renderable": False,
                "indexable": False,
                "waf_cdn_signal": waf_cdn,
                "response_sample": "",
                "indexability_reasons": ["request_failed"],
                "indexability_reason": "request_failed",
                "blocked_reasons": ["request_failed"],
                "bot_priority_weight": float(self.category_weights.get(bot.category, 0.5)),
                "sla_target_pct": float(self.category_sla.get(bot.category, 85.0)),
                "error": str(exc),
            }

    def _build_issues(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []
        def is_expected_ai_policy(row: Dict[str, Any]) -> bool:
            if not self.ai_block_expected:
                return False
            if str(row.get("category") or "") != "AI":
                return False
            return (not row.get("accessible")) or (row.get("robots_allowed") is False) or bool((row.get("waf_cdn_signal", {}) or {}).get("detected"))

        for row in rows:
            if row.get("accessible"):
                continue
            expected_policy = is_expected_ai_policy(row)
            issues.append({
                "severity": "info" if expected_policy else "critical",
                "bot": row["bot_name"],
                "category": row["category"],
                "title": "Expected policy block" if expected_policy else "Bot cannot access URL",
                "details": ("Expected AI policy block: " + (row.get("error") or f"HTTP {row.get('status')}")) if expected_policy else (row.get("error") or f"HTTP {row.get('status')}"),
            })
        for row in rows:
            if row.get("accessible") and row.get("robots_allowed") is False:
                expected_policy = is_expected_ai_policy(row)
                issues.append({
                    "severity": "info" if expected_policy else "warning",
                    "bot": row["bot_name"],
                    "category": row["category"],
                    "title": "Blocked by robots.txt",
                    "details": row.get("indexability_reason") or "robots disallow matched for this bot/path",
                })
        for row in rows:
            if row.get("accessible") and not row.get("has_content"):
                issues.append({
                    "severity": "warning",
                    "bot": row["bot_name"],
                    "category": row["category"],
                    "title": "Empty response body",
                    "details": "HTTP is reachable but response body appears empty.",
                })
        for row in rows:
            waf = row.get("waf_cdn_signal", {}) or {}
            if waf.get("detected"):
                expected_policy = is_expected_ai_policy(row)
                issues.append(
                    {
                        "severity": "info" if expected_policy else "critical",
                        "bot": row["bot_name"],
                        "category": row["category"],
                        "title": "Expected WAF/CDN policy block" if expected_policy else "WAF/CDN challenge detected",
                        "details": (
                            f"Expected AI policy block via {waf.get('provider', 'unknown')}: {waf.get('reason', 'access challenge')}"
                            if expected_policy
                            else f"{waf.get('provider', 'unknown')}: {waf.get('reason', 'access challenge')}"
                        ),
                    }
                )
        for row in rows:
            if row.get("x_robots_forbidden") or row.get("meta_forbidden"):
                issues.append({
                    "severity": "info",
                    "bot": row["bot_name"],
                    "category": row["category"],
                    "title": "Indexing restrictive directive found",
                    "details": f"X-Robots={row.get('x_robots_tag') or '-'}; Meta={row.get('meta_robots') or '-'}",
                })
        return issues

    def _build_recommendations(self, rows: List[Dict[str, Any]], issues: Optional[List[Dict[str, Any]]] = None) -> List[str]:
        recs: List[str] = []
        total = len(rows)
        unavailable = sum(1 for r in rows if not r.get("accessible"))
        empty = sum(1 for r in rows if r.get("accessible") and not r.get("has_content"))
        robots_disallow = sum(1 for r in rows if r.get("robots_allowed") is False)
        restrictive = sum(1 for r in rows if r.get("x_robots_forbidden") or r.get("meta_forbidden"))
        waf_detected = sum(1 for r in rows if (r.get("waf_cdn_signal", {}) or {}).get("detected"))
        if unavailable:
            recs.append(f"{unavailable}/{total} bots cannot access the URL. Review WAF, CDN, and rate-limit rules for bot traffic.")
        if empty:
            recs.append(f"{empty}/{total} bots receive empty content. Verify SSR/edge rendering and anti-bot challenge behavior.")
        if robots_disallow:
            recs.append(f"{robots_disallow}/{total} bots look blocked by robots.txt rules. Validate User-agent groups and Disallow paths.")
        if restrictive:
            recs.append(f"{restrictive}/{total} bots receive restrictive indexing directives. Confirm this is intentional for the audited URL.")
        if waf_detected:
            recs.append(f"{waf_detected}/{total} bots hit WAF/CDN challenge signatures. Add verified bot allowlists and bypass rules.")
        if not recs:
            issues = issues or []
            non_info_issues = sum(1 for item in issues if str(item.get("severity", "")).lower() in {"critical", "warning"})
            if non_info_issues > 0:
                recs.append(f"{non_info_issues} warning/critical findings detected. Review Top Issues and Priority Blockers.")
            elif issues:
                recs.append("Only informational policy findings detected. Review Top Issues for context.")
            else:
                recs.append("No accessibility findings detected for checked bot set.")
        return recs

    def _build_category_stats(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            groups.setdefault(row["category"], []).append(row)
        stats: List[Dict[str, Any]] = []
        for category, items in sorted(groups.items()):
            total = len(items)
            accessible = sum(1 for x in items if x.get("accessible"))
            with_content = sum(1 for x in items if x.get("has_content"))
            crawlable = sum(1 for x in items if x.get("crawlable"))
            renderable = sum(1 for x in items if x.get("renderable"))
            indexable = sum(
                1
                for x in items
                if x.get("indexable")
            )
            restrictive = sum(1 for x in items if x.get("x_robots_forbidden") or x.get("meta_forbidden"))
            criticality_weight = float(self.category_weights.get(category, 0.5))
            sla_target_pct = float(self.category_sla.get(category, 85.0))
            non_indexable = total - indexable
            indexable_pct = round((indexable / max(1, total)) * 100.0, 1)
            priority_risk_score = round((non_indexable / max(1, total)) * 100.0 * criticality_weight, 1)
            stats.append({
                "category": category,
                "total": total,
                "accessible": accessible,
                "with_content": with_content,
                "crawlable": crawlable,
                "renderable": renderable,
                "indexable": indexable,
                "non_indexable": non_indexable,
                "indexable_pct": indexable_pct,
                "criticality_weight": criticality_weight,
                "sla_target_pct": sla_target_pct,
                "sla_met": indexable_pct >= sla_target_pct,
                "priority_risk_score": priority_risk_score,
                "restrictive_directives": restrictive,
            })
        return stats

    def _build_priority_blockers(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        buckets: Dict[str, Dict[str, Any]] = {
            "unreachable": {
                "code": "unreachable",
                "title": "Bots cannot reach URL",
                "details": "HTTP/network failures or denied access.",
                "count": 0,
                "weighted": 0.0,
                "bots": [],
            },
            "empty_content": {
                "code": "empty_content",
                "title": "Accessible but empty content",
                "details": "Response body is empty or unusable for indexing.",
                "count": 0,
                "weighted": 0.0,
                "bots": [],
            },
            "robots_disallow": {
                "code": "robots_disallow",
                "title": "Blocked by robots.txt",
                "details": "Robots rules likely disallow crawling.",
                "count": 0,
                "weighted": 0.0,
                "bots": [],
            },
            "indexing_directive": {
                "code": "indexing_directive",
                "title": "Restricted by robots directives",
                "details": "X-Robots-Tag or meta robots prevents indexing.",
                "count": 0,
                "weighted": 0.0,
                "bots": [],
            },
            "waf_challenge": {
                "code": "waf_challenge",
                "title": "WAF/CDN challenge for bots",
                "details": "Challenge pages or anti-bot checks affect crawler rendering/indexing.",
                "count": 0,
                "weighted": 0.0,
                "bots": [],
            },
        }

        for row in rows:
            bot_name = str(row.get("bot_name") or "")
            category = str(row.get("category") or "")
            weight = float(self.category_weights.get(category, 0.5))
            expected_ai_policy = (
                self.ai_block_expected
                and category == "AI"
                and (
                    (not row.get("accessible"))
                    or (row.get("robots_allowed") is False)
                    or bool((row.get("waf_cdn_signal", {}) or {}).get("detected"))
                )
            )
            if expected_ai_policy:
                continue

            def add(code: str) -> None:
                bucket = buckets[code]
                bucket["count"] += 1
                bucket["weighted"] += weight
                if bot_name and bot_name not in bucket["bots"]:
                    bucket["bots"].append(bot_name)

            if not row.get("accessible"):
                add("unreachable")
                continue
            if not row.get("crawlable"):
                add("robots_disallow")
            if not row.get("has_content"):
                add("empty_content")
            if row.get("robots_allowed") is False:
                add("robots_disallow")
            if row.get("x_robots_forbidden") or row.get("meta_forbidden"):
                add("indexing_directive")
            if (row.get("waf_cdn_signal", {}) or {}).get("detected"):
                add("waf_challenge")

        blockers: List[Dict[str, Any]] = []
        for bucket in buckets.values():
            if bucket["count"] <= 0:
                continue
            blockers.append(
                {
                    "code": bucket["code"],
                    "title": bucket["title"],
                    "details": bucket["details"],
                    "affected_bots": bucket["count"],
                    "weighted_impact": round(bucket["weighted"], 2),
                    "priority_score": round(bucket["weighted"] * 10.0, 1),
                    "sample_bots": bucket["bots"][:8],
                }
            )

        blockers.sort(key=lambda x: (float(x.get("priority_score", 0.0)), int(x.get("affected_bots", 0))), reverse=True)
        return blockers

    def _build_playbooks(self, blockers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        templates: Dict[str, Dict[str, Any]] = {
            "unreachable": {
                "owner": "DevOps",
                "title": "Unblock bot transport access",
                "actions": [
                    "Validate WAF/CDN/firewall policies for known bot user-agents and ASN ranges.",
                    "Reduce false positives in anti-bot challenge policies for crawl traffic.",
                    "Check 403/429 logs and create explicit allow policies for critical bots.",
                ],
            },
            "empty_content": {
                "owner": "Frontend/Platform",
                "title": "Fix empty bot responses",
                "actions": [
                    "Ensure SSR or pre-render returns meaningful HTML for bots.",
                    "Disable bot-facing challenge pages for verified crawlers.",
                    "Verify edge functions do not strip response body for bot traffic.",
                ],
            },
            "robots_disallow": {
                "owner": "SEO",
                "title": "Correct robots.txt blocking rules",
                "actions": [
                    "Review Disallow directives for critical bot groups.",
                    "Use Allow exceptions for important paths blocked by broad rules.",
                    "Validate final rules with robots tester against target paths.",
                ],
            },
            "indexing_directive": {
                "owner": "SEO/Dev",
                "title": "Align indexing directives",
                "actions": [
                    "Audit X-Robots-Tag/meta robots for unintended noindex/nofollow.",
                    "Align directive behavior between templates and middleware.",
                    "Re-test indexability after deploy and confirm in search consoles.",
                ],
            },
            "waf_challenge": {
                "owner": "DevOps/Platform",
                "title": "Reduce anti-bot challenge impact",
                "actions": [
                    "Exclude verified crawler traffic from JS/captcha challenge flows.",
                    "Tune WAF rules for bot user-agent + ASN/IP verification.",
                    "Monitor 403/429/challenge signatures by bot category after rollout.",
                ],
            },
        }
        rows: List[Dict[str, Any]] = []
        for blocker in blockers:
            code = str(blocker.get("code") or "")
            tpl = templates.get(code)
            if not tpl:
                continue
            rows.append(
                {
                    "blocker_code": code,
                    "priority_score": blocker.get("priority_score", 0),
                    "owner": tpl["owner"],
                    "title": tpl["title"],
                    "actions": tpl["actions"],
                }
            )
        return rows

    def _build_robots_linter(self, robots_text: Optional[str]) -> Dict[str, Any]:
        if not robots_text:
            return {"findings": [{"severity": "warning", "code": "robots_missing", "message": "robots.txt is not reachable"}]}
        findings: List[Dict[str, Any]] = []
        groups = _parse_robots_groups(robots_text)
        has_global_disallow_root = False
        for g in groups:
            uas = [str(x).lower() for x in (g.get("user_agents") or [])]
            if "*" in uas:
                for rule in (g.get("rules") or []):
                    if rule.get("directive") == "disallow" and str(rule.get("pattern") or "").strip() == "/":
                        has_global_disallow_root = True
        if has_global_disallow_root:
            findings.append({"severity": "critical", "code": "global_disallow_root", "message": "Disallow: / found for User-agent: *"})
        if "crawl-delay" in (robots_text or "").lower():
            findings.append({"severity": "info", "code": "crawl_delay_present", "message": "crawl-delay directives present; verify per-engine behavior."})
        if "googlebot" not in (robots_text or "").lower():
            findings.append({"severity": "info", "code": "no_explicit_googlebot", "message": "No explicit Googlebot group. Using wildcard behavior."})
        if "yandex" not in (robots_text or "").lower():
            findings.append({"severity": "info", "code": "no_explicit_yandex", "message": "No explicit Yandex group. Using wildcard behavior."})
        return {"findings": findings}

    def _build_allowlist_simulator(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        total = len(rows)
        current_indexable = sum(1 for r in rows if r.get("indexable"))
        current_renderable = sum(1 for r in rows if r.get("renderable"))

        def simulate(category_filter: str) -> Dict[str, Any]:
            projected_indexable = current_indexable
            projected_renderable = current_renderable
            affected = 0
            for r in rows:
                if str(r.get("category") or "") != category_filter:
                    continue
                blocked_by_transport = (not r.get("accessible")) or bool((r.get("waf_cdn_signal", {}) or {}).get("detected"))
                blocked_by_policy = (r.get("robots_allowed") is False) or bool(r.get("x_robots_forbidden")) or bool(r.get("meta_forbidden"))
                if blocked_by_transport or blocked_by_policy:
                    affected += 1
                    if not r.get("renderable"):
                        projected_renderable += 1
                    if not r.get("indexable"):
                        projected_indexable += 1
            return {
                "scenario": f"allow_{category_filter.lower()}",
                "category": category_filter,
                "affected_bots": affected,
                "projected_renderable": projected_renderable,
                "projected_indexable": projected_indexable,
                "delta_renderable": projected_renderable - current_renderable,
                "delta_indexable": projected_indexable - current_indexable,
                "projected_indexable_pct": round((projected_indexable / max(1, total)) * 100.0, 1),
            }

        return {"scenarios": [simulate("AI"), simulate("Search"), simulate("Google"), simulate("Yandex"), simulate("Bing")]}

    def _build_action_center(self, playbooks: List[Dict[str, Any]]) -> Dict[str, Any]:
        by_owner: Dict[str, List[Dict[str, Any]]] = {}
        for p in playbooks:
            owner = str(p.get("owner") or "Team")
            by_owner.setdefault(owner, []).append(p)
        for owner in by_owner:
            by_owner[owner].sort(key=lambda x: float(x.get("priority_score", 0) or 0), reverse=True)
        return {"by_owner": by_owner}

    def _build_alerts(self, summary: Dict[str, Any], baseline_diff: Dict[str, Any], trend: Dict[str, Any]) -> List[Dict[str, Any]]:
        alerts: List[Dict[str, Any]] = []
        total = int(summary.get("total", 0) or 0)
        idx = int(summary.get("indexable", 0) or 0)
        if total > 0 and (idx / total) < 0.7:
            alerts.append({"severity": "warning", "code": "low_indexable_rate", "message": f"Indexable rate is below 70% ({idx}/{total})."})
        for m in (baseline_diff.get("metrics") or []):
            if m.get("metric") == "indexable" and float(m.get("delta", 0) or 0) <= -3:
                alerts.append({"severity": "critical", "code": "indexable_drop_vs_baseline", "message": f"Indexable dropped by {m.get('delta')} vs baseline."})
            if m.get("metric") == "avg_response_time_ms" and float(m.get("delta", 0) or 0) >= 400:
                alerts.append({"severity": "warning", "code": "latency_regression", "message": f"Avg response time increased by {m.get('delta')} ms vs baseline."})
        delta = trend.get("delta_vs_previous") or {}
        if float(delta.get("indexable", 0) or 0) <= -3:
            alerts.append({"severity": "critical", "code": "indexable_drop_vs_previous", "message": f"Indexable dropped by {delta.get('indexable')} vs previous run."})
        if float(delta.get("critical_issues", 0) or 0) >= 3:
            alerts.append({"severity": "warning", "code": "critical_issues_growth", "message": f"Critical issues increased by {delta.get('critical_issues')} vs previous run."})
        return alerts

    def _build_evidence_pack(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        evidence_rows: List[Dict[str, Any]] = []
        for r in rows:
            if r.get("indexable"):
                continue
            evidence_rows.append(
                {
                    "bot": r.get("bot_name"),
                    "category": r.get("category"),
                    "status": r.get("status"),
                    "indexability_reason": r.get("indexability_reason"),
                    "waf_detected": bool((r.get("waf_cdn_signal", {}) or {}).get("detected")),
                    "waf_confidence": (r.get("waf_cdn_signal", {}) or {}).get("confidence", 0),
                    "waf_reason": (r.get("waf_cdn_signal", {}) or {}).get("reason", ""),
                    "robots_explain": (r.get("robots_evaluation", {}) or {}).get("explain", ""),
                    "response_sample": r.get("response_sample", ""),
                }
            )
        return {"rows": evidence_rows[:100]}

    def _build_sla_dashboard(self, category_stats: List[Dict[str, Any]]) -> Dict[str, Any]:
        total_categories = len(category_stats)
        met = sum(1 for c in category_stats if c.get("sla_met"))
        missed_rows = sorted(
            [c for c in category_stats if not c.get("sla_met")],
            key=lambda x: float(x.get("priority_risk_score", 0) or 0),
            reverse=True,
        )
        return {
            "total_categories": total_categories,
            "met_categories": met,
            "missed_categories": total_categories - met,
            "top_missed": missed_rows[:5],
        }

    def run(self, raw_url: str, selected_bots: Optional[List[str]] = None, bot_groups: Optional[List[str]] = None) -> Dict[str, Any]:
        url = normalize_url(raw_url)
        completed_at = datetime.utcnow().isoformat()
        robots_text, robots_status = self._load_robots(url)
        bots_to_check = self._resolve_bots(selected_bots, bot_groups)

        rows: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._check_one, url, bot, robots_text) for bot in bots_to_check]
            for fut in as_completed(futures):
                rows.append(fut.result())
        rows.sort(key=lambda x: (x["category"], x["bot_name"]))

        by_bot: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            by_bot[row["bot_name"]] = {
                "status": row.get("status"),
                "accessible": row.get("accessible"),
                "response_time": (row.get("response_time_ms") or 0) / 1000.0,
                "response_time_ms": row.get("response_time_ms"),
                "has_content": row.get("has_content"),
                "category": row.get("category"),
                "x_robots_tag": row.get("x_robots_tag"),
                "x_robots_forbidden": row.get("x_robots_forbidden"),
                "meta_robots": row.get("meta_robots"),
                "meta_forbidden": row.get("meta_forbidden"),
                "robots_allowed": row.get("robots_allowed"),
                "robots_evaluation": row.get("robots_evaluation", {}),
                "error": row.get("error"),
                "final_url": row.get("final_url"),
                "crawlable": row.get("crawlable"),
                "renderable": row.get("renderable"),
                "indexable": row.get("indexable"),
                "waf_cdn_signal": row.get("waf_cdn_signal", {}),
                "blocked_reasons": row.get("blocked_reasons", []),
                "bot_priority_weight": row.get("bot_priority_weight"),
                "sla_target_pct": row.get("sla_target_pct"),
            }

        total = len(rows)
        accessible = sum(1 for r in rows if r.get("accessible"))
        with_content = sum(1 for r in rows if r.get("has_content"))
        crawlable = sum(1 for r in rows if r.get("crawlable"))
        renderable = sum(1 for r in rows if r.get("renderable"))
        indexable = sum(
            1
            for r in rows
            if r.get("indexable")
        )
        robots_disallowed = sum(1 for r in rows if r.get("robots_allowed") is False)
        x_forbidden = sum(1 for r in rows if r.get("x_robots_forbidden"))
        meta_forbidden = sum(1 for r in rows if r.get("meta_forbidden"))
        waf_cdn_detected = sum(1 for r in rows if (r.get("waf_cdn_signal", {}) or {}).get("detected"))
        timing_rows = [r["response_time_ms"] for r in rows if r.get("response_time_ms") is not None]
        avg_ms = (sum(timing_rows) / len(timing_rows)) if timing_rows else 0.0

        issues = self._build_issues(rows)
        recommendations = self._build_recommendations(rows, issues=issues)
        category_stats = self._build_category_stats(rows)
        priority_blockers = self._build_priority_blockers(rows)
        playbooks = self._build_playbooks(priority_blockers)
        domain = urlparse(url).netloc
        host_consistency = self._host_consistency_check(url)
        waf_bypass_probe = self._run_waf_bypass_probe(url)
        expected_ai_blocked = 0
        if self.ai_block_expected:
            expected_ai_blocked = sum(
                1
                for r in rows
                if str(r.get("category") or "") == "AI"
                and (
                    (not r.get("accessible"))
                    or (r.get("robots_allowed") is False)
                    or bool((r.get("waf_cdn_signal", {}) or {}).get("detected"))
                )
            )

        summary = {
            "total": total,
            "accessible": accessible,
            "unavailable": total - accessible,
            "with_content": with_content,
            "without_content": total - with_content,
            "crawlable": crawlable,
            "non_crawlable": total - crawlable,
            "renderable": renderable,
            "non_renderable": total - renderable,
            "indexable": indexable,
            "non_indexable": total - indexable,
            "robots_disallowed": robots_disallowed,
            "x_robots_forbidden": x_forbidden,
            "meta_forbidden": meta_forbidden,
            "waf_cdn_detected": waf_cdn_detected,
            "expected_ai_policy_blocked": expected_ai_blocked,
            "avg_response_time_ms": round(avg_ms, 2),
        }
        summary["issues_total"] = len(issues)
        summary["critical_issues"] = sum(1 for x in issues if (x.get("severity") or "").lower() == "critical")
        summary["warning_issues"] = sum(1 for x in issues if (x.get("severity") or "").lower() == "warning")
        summary["info_issues"] = sum(1 for x in issues if (x.get("severity") or "").lower() == "info")
        baseline = self._load_baseline(domain)
        baseline_diff = self._compute_baseline_diff(baseline, summary)
        self._save_baseline(domain, summary)
        trend = self._append_trend_snapshot(domain=domain, url=url, summary=summary, completed_at=completed_at)
        robots_linter = self._build_robots_linter(robots_text)
        allowlist_simulator = self._build_allowlist_simulator(rows)
        action_center = self._build_action_center(playbooks)
        evidence_pack = self._build_evidence_pack(rows)
        sla_dashboard = self._build_sla_dashboard(category_stats)
        alerts = self._build_alerts(summary=summary, baseline_diff=baseline_diff, trend=trend)

        return {
            "task_type": "bot_check",
            "url": url,
            "completed_at": completed_at,
            "results": {
                "engine": "v2",
                "domain": domain,
                "retry_profile": self.retry_profile,
                "criticality_profile": self.criticality_profile_name,
                "sla_profile": self.sla_profile_name,
                "ai_block_expected": self.ai_block_expected,
                "bots_checked": [b.name for b in bots_to_check],
                "selected_bot_groups": bot_groups or [],
                "bot_results": by_bot,
                "bot_rows": rows,
                "summary": summary,
                "robots": {
                    "found": robots_text is not None,
                    "status_code": robots_status,
                },
                "host_consistency": host_consistency,
                "waf_bypass_probe": waf_bypass_probe,
                "category_stats": category_stats,
                "sla_dashboard": sla_dashboard,
                "priority_blockers": priority_blockers,
                "playbooks": playbooks,
                "action_center": action_center,
                "allowlist_simulator": allowlist_simulator,
                "robots_linter": robots_linter,
                "evidence_pack": evidence_pack,
                "baseline_diff": baseline_diff,
                "trend": trend,
                "alerts": alerts,
                "issues": issues,
                "recommendations": recommendations,
            },
        }

    def run_batch(self, raw_urls: List[str], selected_bots: Optional[List[str]] = None, bot_groups: Optional[List[str]] = None) -> Dict[str, Any]:
        urls = _normalize_batch_urls(raw_urls, limit=100)
        if not urls:
            return self.run("")
        runs = [self.run(u, selected_bots=selected_bots, bot_groups=bot_groups) for u in urls]
        primary = runs[0]
        batch_rows: List[Dict[str, Any]] = []
        for run in runs:
            rr = (run.get("results") or {})
            ss = (rr.get("summary") or {})
            batch_rows.append(
                {
                    "url": run.get("url"),
                    "indexable": ss.get("indexable", 0),
                    "total": ss.get("total", 0),
                    "renderable": ss.get("renderable", 0),
                    "crawlable": ss.get("crawlable", 0),
                    "critical_issues": ss.get("critical_issues", 0),
                    "warning_issues": ss.get("warning_issues", 0),
                    "avg_response_time_ms": ss.get("avg_response_time_ms", 0),
                    "alerts": rr.get("alerts", []),
                }
            )
        merged = dict(primary)
        results = dict((primary.get("results") or {}))
        results["batch_mode"] = True
        results["batch_urls_count"] = len(urls)
        results["batch_runs"] = batch_rows
        merged["results"] = results
        return merged
