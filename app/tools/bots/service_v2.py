"""
Bot accessibility checker v2.

Designed for API usage (no CLI/interactive behavior).
"""
from __future__ import annotations

import re
import time
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
    BotDefinition("Google-CloudVertexBot", "Mozilla/5.0 (compatible; Google-CloudVertexBot/1.0; +https://cloud.google.com/vertex-ai)", "AI"),
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


def _parse_robots_groups(robots_text: str) -> List[Tuple[str, List[str]]]:
    groups: List[Tuple[str, List[str]]] = []
    current_ua = ""
    current_rules: List[str] = []
    for raw in (robots_text or "").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = [x.strip() for x in line.split(":", 1)]
        key = key.lower()
        if key == "user-agent":
            if current_ua:
                groups.append((current_ua, current_rules))
            current_ua = value.lower()
            current_rules = []
        elif key in ("allow", "disallow"):
            current_rules.append(f"{key}:{value}")
    if current_ua:
        groups.append((current_ua, current_rules))
    return groups


def _robots_allows_bot(bot_name: str, user_agent: str, robots_text: Optional[str]) -> Optional[bool]:
    if not robots_text:
        return None
    groups = _parse_robots_groups(robots_text)
    targets = {
        bot_name.lower(),
        user_agent.lower().split("/", 1)[0].strip().lower(),
    }
    matched_rules: List[str] = []
    for ua, rules in groups:
        if ua == "*" or ua in targets or any(t in ua for t in targets):
            matched_rules = rules
            break
    if not matched_rules:
        return None
    for rule in matched_rules:
        if rule.startswith("disallow:"):
            path = rule.split(":", 1)[1].strip()
            if path in ("/", "/*"):
                return False
    return True


class BotAccessibilityServiceV2:
    def __init__(self, timeout: int = 15, max_workers: int = 10):
        self.timeout = timeout
        self.max_workers = max_workers
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=2,
            backoff_factor=0.4,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=100)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _load_robots(self, url: str) -> Tuple[Optional[str], Optional[int]]:
        robots_url = url.rstrip("/") + "/robots.txt"
        try:
            resp = self.session.get(robots_url, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.text, resp.status_code
            return None, resp.status_code
        except Exception:
            return None, None

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
            meta_robots = _extract_meta_robots(response.text[:100000]) if is_html else None
            x_robots = response.headers.get("X-Robots-Tag") or response.headers.get("X-Robots")
            robots_allowed = _robots_allows_bot(bot.name, bot.user_agent, robots_text)
            has_content = len(response.content or b"") > 0
            accessible = 200 <= response.status_code < 400

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
                "content_type": content_type,
                "final_url": response.url,
                "error": None,
            }
        except Exception as exc:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
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
                "robots_allowed": _robots_allows_bot(bot.name, bot.user_agent, robots_text),
                "content_type": None,
                "final_url": None,
                "error": str(exc),
            }

    def _build_issues(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []
        for row in rows:
            if row.get("accessible"):
                continue
            issues.append({
                "severity": "critical",
                "bot": row["bot_name"],
                "category": row["category"],
                "title": "Bot cannot access URL",
                "details": row.get("error") or f"HTTP {row.get('status')}",
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
            if row.get("x_robots_forbidden") or row.get("meta_forbidden"):
                issues.append({
                    "severity": "info",
                    "bot": row["bot_name"],
                    "category": row["category"],
                    "title": "Indexing restrictive directive found",
                    "details": f"X-Robots={row.get('x_robots_tag') or '-'}; Meta={row.get('meta_robots') or '-'}",
                })
        return issues

    def _build_recommendations(self, rows: List[Dict[str, Any]]) -> List[str]:
        recs: List[str] = []
        total = len(rows)
        unavailable = sum(1 for r in rows if not r.get("accessible"))
        empty = sum(1 for r in rows if r.get("accessible") and not r.get("has_content"))
        robots_disallow = sum(1 for r in rows if r.get("robots_allowed") is False)
        restrictive = sum(1 for r in rows if r.get("x_robots_forbidden") or r.get("meta_forbidden"))
        if unavailable:
            recs.append(f"{unavailable}/{total} bots cannot access the URL. Review WAF, CDN, and rate-limit rules for bot traffic.")
        if empty:
            recs.append(f"{empty}/{total} bots receive empty content. Verify SSR/edge rendering and anti-bot challenge behavior.")
        if robots_disallow:
            recs.append(f"{robots_disallow}/{total} bots look blocked by robots.txt rules. Validate User-agent groups and Disallow paths.")
        if restrictive:
            recs.append(f"{restrictive}/{total} bots receive restrictive indexing directives. Confirm this is intentional for the audited URL.")
        if not recs:
            recs.append("No critical accessibility findings detected for checked bot set.")
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
            restrictive = sum(1 for x in items if x.get("x_robots_forbidden") or x.get("meta_forbidden"))
            stats.append({
                "category": category,
                "total": total,
                "accessible": accessible,
                "with_content": with_content,
                "restrictive_directives": restrictive,
            })
        return stats

    def run(self, raw_url: str) -> Dict[str, Any]:
        url = normalize_url(raw_url)
        robots_text, robots_status = self._load_robots(url)

        rows: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._check_one, url, bot, robots_text) for bot in BOT_DEFINITIONS]
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
                "error": row.get("error"),
                "final_url": row.get("final_url"),
            }

        total = len(rows)
        accessible = sum(1 for r in rows if r.get("accessible"))
        with_content = sum(1 for r in rows if r.get("has_content"))
        robots_disallowed = sum(1 for r in rows if r.get("robots_allowed") is False)
        x_forbidden = sum(1 for r in rows if r.get("x_robots_forbidden"))
        meta_forbidden = sum(1 for r in rows if r.get("meta_forbidden"))
        timing_rows = [r["response_time_ms"] for r in rows if r.get("response_time_ms") is not None]
        avg_ms = (sum(timing_rows) / len(timing_rows)) if timing_rows else 0.0

        issues = self._build_issues(rows)
        recommendations = self._build_recommendations(rows)
        category_stats = self._build_category_stats(rows)
        domain = urlparse(url).netloc

        return {
            "task_type": "bot_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "engine": "v2",
                "domain": domain,
                "bots_checked": [b.name for b in BOT_DEFINITIONS],
                "bot_results": by_bot,
                "bot_rows": rows,
                "summary": {
                    "total": total,
                    "accessible": accessible,
                    "unavailable": total - accessible,
                    "with_content": with_content,
                    "without_content": total - with_content,
                    "robots_disallowed": robots_disallowed,
                    "x_robots_forbidden": x_forbidden,
                    "meta_forbidden": meta_forbidden,
                    "avg_response_time_ms": round(avg_ms, 2),
                },
                "robots": {
                    "found": robots_text is not None,
                    "status_code": robots_status,
                },
                "category_stats": category_stats,
                "issues": issues,
                "recommendations": recommendations,
            },
        }

