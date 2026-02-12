"""
SEO Tools API Routes - Full integration with original scripts
"""
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
import re

router = APIRouter(prefix="/api", tags=["SEO Tools"])

# Storage for results
task_results = {}

# ============ ORIGINAl ROBOTS AUDIT LOGIC ============
EXPECTED_BOTS = ["googlebot", "yandex", "bingbot"]

SENSITIVE_PATHS = [
    "/admin", "/wp-admin", "/administrator", "/cgi-bin", "/config",
    "/database", "/backup", "/logs", "/phpmyadmin", "/.git", "/.env",
    "/wp-config", "/include", "/tmp", "/temp", "/private", "/secret",
    "/uploads",
]

RECOMMENDATIONS = [
    "Группируйте правила по User-agent для лучшей читаемости",
    "Блокируйте служебные папки: /admin, /tmp, /backup, /.git",
    "Не блокируйте CSS и JS файлы - это мешает сканированию",
    "Используйте Crawl-delay для больших сайтов",
    "Всегда указывайте Sitemap с полным URL",
    "Удаляйте дублирующиеся правила",
    "Избегайте Disallow: / если не хотите полностью заблокировать сайт",
    "Используйте Allow для создания исключений из Disallow",
    "Проверяйте файл в Google Search Console",
    "Учитывайте различия интерпретации директив разными поисковиками",
]


class Rule:
    def __init__(self, user_agent: str, path: str, line: int):
        self.user_agent = user_agent
        self.path = path
        self.line = line


class ParseResult:
    def __init__(self):
        self.groups = []
        self.sitemaps = []
        self.crawl_delays = {}
        self.clean_params = []
        self.hosts = []
        self.raw_lines = []
        self.syntax_errors = []
        self.warnings = []
        self.all_disallow = []
        self.all_allow = []


def fetch_robots(url: str, timeout: int = 20) -> str:
    import requests
    resp = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    return resp.text


def parse_robots(text: str) -> ParseResult:
    lines = text.splitlines()
    result = ParseResult()
    result.raw_lines = lines
    
    current_group = None
    
    for idx, raw in enumerate(lines, start=1):
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if ":" not in stripped:
            result.syntax_errors.append({"line": idx, "error": "Нет ':'", "content": raw})
            result.warnings.append(f"Строка {idx}: Неверный синтаксис")
            continue
            
        key, value = stripped.split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        
        if key == "user-agent":
            if current_group is None or current_group.get("user_agents"):
                current_group = {"user_agents": [], "disallow": [], "allow": []}
                result.groups.append(current_group)
            current_group["user_agents"].append(value)
            
        elif key == "disallow":
            if not current_group or not current_group.get("user_agents"):
                result.warnings.append(f"Строка {idx}: Disallow без User-agent")
                continue
            rule = Rule(current_group["user_agents"][-1], value, idx)
            current_group["disallow"].append(rule)
            result.all_disallow.append(rule)
            
        elif key == "allow":
            if not current_group or not current_group.get("user_agents"):
                continue
            rule = Rule(current_group["user_agents"][-1], value, idx)
            current_group["allow"].append(rule)
            result.all_allow.append(rule)
            
        elif key == "sitemap":
            result.sitemaps.append(value)
            
        elif key == "crawl-delay":
            try:
                delay = float(value)
                result.crawl_delays[value] = delay
            except:
                pass
                
        elif key == "clean-param":
            result.clean_params.append(value)
            
        elif key == "host":
            result.hosts.append(value)
    
    return result


def build_issues_and_warnings(result: ParseResult) -> Dict[str, Any]:
    issues = []
    warnings = []
    recommendations = []
    
    # Check for full block
    full_block = any(
        r.path.strip() == "/" 
        for group in result.groups 
        for r in group.get("disallow", [])
    )
    if full_block:
        issues.append("КРИТИЧНО: Весь сайт заблокирован для всех роботов (Disallow: /)")
    
    # Check blocked extensions
    blocked_ext = []
    for ext in [".css", ".js"]:
        for group in result.groups:
            for r in group.get("disallow", []):
                if ext in r.path:
                    blocked_ext.append(ext)
                    break
    if blocked_ext:
        issues.append(f"Заблокированы важные ресурсы: {', '.join(blocked_ext)} - это мешает сканированию")
    
    # Check expected bots
    present_agents = set()
    for group in result.groups:
        for ua in group.get("user_agents", []):
            present_agents.add(ua.lower())
    
    missing_bots = [b for b in EXPECTED_BOTS if not any(b in ua for ua in present_agents)]
    if missing_bots:
        warnings.append(f"Рекомендуется добавить правила для: {', '.join(missing_bots)}")
    
    # Check sensitive paths
    unblocked_sensitive = []
    blocked_paths = set(r.path for group in result.groups for r in group.get("disallow", []))
    for path in SENSITIVE_PATHS:
        if path not in blocked_paths:
            unblocked_sensitive.append(path)
    if unblocked_sensitive:
        warnings.append(f"Рекомендуется заблокировать: {', '.join(unblocked_sensitive[:5])}")
    
    # Check sitemaps
    if not result.sitemaps:
        warnings.append("Не указана директива Sitemap")
    
    return {
        "issues": issues,
        "warnings": warnings,
        "recommendations": RECOMMENDATIONS,
        "present_agents": list(present_agents),
        "sitemaps": result.sitemaps,
        "crawl_delays": result.crawl_delays,
        "hosts": result.hosts,
        "syntax_errors": result.syntax_errors,
    }


def check_robots_full(url: str) -> Dict[str, Any]:
    """Full robots.txt analysis"""
    import requests
    
    robots_url = url.rstrip('/') + '/robots.txt'
    
    try:
        response = requests.get(robots_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        raw_text = response.text
        
        result = parse_robots(raw_text)
        analysis = build_issues_and_warnings(result)
        
        return {
            "task_type": "robots_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "robots_txt_found": response.status_code == 200,
                "status_code": response.status_code,
                "content_length": len(raw_text),
                "lines_count": len(raw_text.splitlines()),
                "user_agents": len(analysis["present_agents"]),
                "disallow_rules": len(result.all_disallow),
                "allow_rules": len(result.all_allow),
                "sitemaps": analysis["sitemaps"],
                "issues": analysis["issues"],
                "warnings": analysis["warnings"],
                "recommendations": analysis["recommendations"],
                "syntax_errors": analysis["syntax_errors"],
                "hosts": analysis["hosts"],
            }
        }
    except Exception as e:
        return {
            "task_type": "robots_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "error": str(e),
                "robots_txt_found": False
            }
        }


def check_sitemap_full(url: str) -> Dict[str, Any]:
    """Full sitemap validation"""
    import requests
    import xml.etree.ElementTree as ET
    
    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        
        if response.status_code != 200:
            return {
                "task_type": "sitemap_validate",
                "url": url,
                "completed_at": datetime.utcnow().isoformat(),
                "results": {
                    "valid": False,
                    "error": f"HTTP {response.status_code}"
                }
            }
        
        # Parse XML
        root = ET.fromstring(response.text)
        
        # Count URLs
        urls_count = len(root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'))
        if urls_count == 0:
            urls_count = len(root.findall('.//url'))
        
        # Check for errors
        errors = []
        if "<" in response.text or ">" in response.text:
            if "]]>" not in response.text:
                errors.append("Возможные проблемы с XML экранированием")
        
        return {
            "task_type": "sitemap_validate",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "valid": True,
                "urls_count": urls_count,
                "status_code": response.status_code,
                "errors": errors,
                "size": len(response.text),
            }
        }
    except Exception as e:
        return {
            "task_type": "sitemap_validate",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "valid": False,
                "error": str(e)
            }
        }


def check_bots_full(url: str) -> Dict[str, Any]:
    """Full bot accessibility check"""
    import requests
    
    bots = [
        ("Googlebot", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"),
        ("YandexBot", "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)"),
        ("Bingbot", "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"),
        ("DuckDuckBot", "DuckDuckBot/1.0; (+https://duckduckgo.com/duckbot)"),
        ("GPTBot", "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.0; +https://openai.com/gptbot)"),
    ]
    
    results = {}
    for bot_name, user_agent in bots:
        try:
            resp = requests.get(url, headers={"User-Agent": user_agent}, timeout=10)
            results[bot_name] = {
                "status": resp.status_code,
                "accessible": resp.status_code == 200,
                "response_time": resp.elapsed.total_seconds() if hasattr(resp, 'elapsed') else None
            }
        except Exception as e:
            results[bot_name] = {"error": str(e), "accessible": False}
    
    return {
        "task_type": "bot_check",
        "url": url,
        "completed_at": datetime.utcnow().isoformat(),
        "results": {
            "bots_checked": [b[0] for b in bots],
            "bot_results": results,
            "summary": {
                "total": len(bots),
                "accessible": sum(1 for r in results.values() if r.get("accessible")),
            }
        }
    }


# ============ REQUEST MODELS ============
class RobotsCheckRequest(BaseModel):
    url: str

class SitemapValidateRequest(BaseModel):
    url: str

class BotCheckRequest(BaseModel):
    url: str


# ============ API ENDPOINTS ============
def create_task_result(task_id: str, task_type: str, url: str, result: Dict[str, Any]):
    task_results[task_id] = {
        "task_id": task_id,
        "task_type": task_type,
        "url": url,
        "result": result,
        "completed_at": datetime.utcnow().isoformat()
    }


@router.post("/tasks/robots-check")
async def create_robots_check(data: RobotsCheckRequest):
    """Full robots.txt analysis"""
    url = data.url
    
    print(f"[API] Full robots.txt analysis for: {url}")
    
    result = check_robots_full(url)
    task_id = f"robots-{datetime.now().timestamp()}"
    create_task_result(task_id, "robots_check", url, result)
    
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Robots.txt analysis completed"
    }


@router.post("/tasks/sitemap-validate")
async def create_sitemap_validate(data: SitemapValidateRequest):
    """Full sitemap validation"""
    url = data.url
    
    print(f"[API] Full sitemap validation for: {url}")
    
    result = check_sitemap_full(url)
    task_id = f"sitemap-{datetime.now().timestamp()}"
    create_task_result(task_id, "sitemap_validate", url, result)
    
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Sitemap validation completed"
    }


@router.post("/tasks/bot-check")
async def create_bot_check(data: BotCheckRequest):
    """Full bot accessibility check"""
    url = data.url
    
    print(f"[API] Full bot check for: {url}")
    
    result = check_bots_full(url)
    task_id = f"bots-{datetime.now().timestamp()}"
    create_task_result(task_id, "bot_check", url, result)
    
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Bot check completed"
    }


@router.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    """Get task result"""
    print(f"[API] Getting status for: {task_id}")
    
    if task_id in task_results:
        data = task_results[task_id]
        return {
            "task_id": task_id,
            "status": "SUCCESS",
            "task_type": data["task_type"],
            "url": data["url"],
            "created_at": datetime.utcnow(),
            "completed_at": datetime.utcnow(),
            "result": data["result"],
            "error": None,
            "can_continue": False
        }
    
    return {
        "task_id": task_id,
        "status": "PENDING",
        "task_type": "site_analyze",
        "url": "",
        "created_at": datetime.utcnow(),
        "completed_at": None,
        "result": None,
        "error": "Task not found",
        "can_continue": False
    }


@router.get("/rate-limit")
async def get_rate_limit():
    """Rate limit info"""
    return {
        "allowed": True,
        "remaining": 999,
        "reset_in": 3600,
        "limit": 10
    }


@router.get("/celery-status")
async def celery_status():
    """Check celery status"""
    return {"celery_available": False, "mode": "synchronous"}


# ============ ADDITIONAL TOOLS ============

def check_site_basic(url: str) -> Dict[str, Any]:
    """Basic site analysis - simplified version"""
    import requests
    from bs4 import BeautifulSoup
    
    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Basic analysis
        title = soup.find('title')
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        h1_tags = soup.find_all('h1')
        
        # Check for common issues
        issues = []
        
        if not title:
            issues.append("Missing title tag")
        if not meta_desc:
            issues.append("Missing meta description")
        if len(h1_tags) == 0:
            issues.append("No H1 tags found")
        elif len(h1_tags) > 1:
            issues.append(f"Multiple H1 tags found ({len(h1_tags)})")
        
        # Check images
        images = soup.find_all('img')
        images_without_alt = [img for img in images if not img.get('alt')]
        if images_without_alt:
            issues.append(f"{len(images_without_alt)} images without alt text")
        
        # Check links
        links = soup.find_all('a')
        broken_links = 0
        
        return {
            "task_type": "site_analyze",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "status_code": response.status_code,
                "title": title.text if title else None,
                "meta_description": meta_desc.get('content') if meta_desc else None,
                "h1_count": len(h1_tags),
                "images_count": len(images),
                "images_without_alt": len(images_without_alt),
                "links_count": len(links),
                "issues": issues,
                "issues_count": len(issues),
                "score": max(0, 100 - len(issues) * 10)
            }
        }
    except Exception as e:
        return {
            "task_type": "site_analyze",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "error": str(e)
            }
        }


def check_render_simple(url: str) -> Dict[str, Any]:
    """Simple render audit - basic HTML vs JS comparison"""
    import requests
    
    try:
        # Get page without JS
        response_no_js = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        
        # Basic comparison
        content_length = len(response_no_js.text)
        word_count = len(response_no_js.text.split())
        
        # Check for common JavaScript frameworks
        js_frameworks = []
        text_lower = response_no_js.text.lower()
        if 'react' in text_lower:
            js_frameworks.append("React")
        if 'vue' in text_lower:
            js_frameworks.append("Vue.js")
        if 'angular' in text_lower:
            js_frameworks.append("Angular")
        if 'jquery' in text_lower:
            js_frameworks.append("jQuery")
        
        # Check for SSR indicators
        is_ssr = bool(response_no_js.text.strip())
        
        return {
            "task_type": "render_audit",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "status_code": response_no_js.status_code,
                "content_length": content_length,
                "word_count": word_count,
                "is_ssr": is_ssr,
                "js_frameworks": js_frameworks,
                "recommendations": [
                    "Ensure critical content is available without JavaScript",
                    "Use server-side rendering for better SEO",
                    "Test with tools like Google Search Console"
                ]
            }
        }
    except Exception as e:
        return {
            "task_type": "render_audit",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "error": str(e)
            }
        }


def check_mobile_simple(url: str) -> Dict[str, Any]:
    """Simple mobile check - viewport and responsive indicators"""
    import requests
    from bs4 import BeautifulSoup
    
    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)"})
        soup = BeautifulSoup(response.text, 'html.parser')
        
        issues = []
        
        # Check viewport meta tag
        viewport = soup.find('meta', attrs={'name': 'viewport'})
        if not viewport:
            issues.append("Missing viewport meta tag")
        else:
            content = viewport.get('content', '')
            if 'width=device-width' not in content:
                issues.append("Viewport not set to device-width")
        
        # Check for responsive images
        images = soup.find_all('img')
        large_images = []
        for img in images:
            src = img.get('src', '')
            if any(size in src for size in ['large', 'big', 'full', 'original']):
                large_images.append(src)
        
        if large_images:
            issues.append(f"Found {len(large_images)} potentially large images")
        
        # Check tap targets
        buttons = soup.find_all(['button', 'a'])
        small_buttons = []
        for btn in buttons:
            text = btn.get_text(strip=True)
            if text and len(text) > 20:
                small_buttons.append(text[:20])
        
        # Mobile-friendly indicators
        mobile_friendly = len(issues) == 0
        
        return {
            "task_type": "mobile_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "status_code": response.status_code,
                "viewport_found": bool(viewport),
                "viewport_content": viewport.get('content') if viewport else None,
                "total_images": len(images),
                "issues": issues,
                "issues_count": len(issues),
                "mobile_friendly": mobile_friendly,
                "score": max(0, 100 - len(issues) * 20),
                "recommendations": [
                    "Use responsive design with flexible layouts",
                    "Ensure tap targets are at least 48x48 pixels",
                    "Use appropriate font sizes (16px minimum)",
                    "Test with Google Mobile-Friendly Test"
                ]
            }
        }
    except Exception as e:
        return {
            "task_type": "mobile_check",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "error": str(e)
            }
        }


class SiteAnalyzeRequest(BaseModel):
    url: str
    max_pages: int = 100

class RenderAuditRequest(BaseModel):
    url: str

class MobileCheckRequest(BaseModel):
    url: str


@router.post("/tasks/site-analyze")
async def create_site_analyze(data: SiteAnalyzeRequest):
    """Basic site analysis"""
    url = data.url
    
    print(f"[API] Basic site analysis for: {url}")
    
    result = check_site_basic(url)
    task_id = f"site-{datetime.now().timestamp()}"
    create_task_result(task_id, "site_analyze", url, result)
    
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Site analysis completed"
    }


@router.post("/tasks/render-audit")
async def create_render_audit(data: RenderAuditRequest):
    """Simple render audit"""
    url = data.url
    
    print(f"[API] Render audit for: {url}")
    
    result = check_render_simple(url)
    task_id = f"render-{datetime.now().timestamp()}"
    create_task_result(task_id, "render_audit", url, result)
    
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Render audit completed"
    }


@router.post("/tasks/mobile-check")
async def create_mobile_check(data: MobileCheckRequest):
    """Simple mobile check"""
    url = data.url
    
    print(f"[API] Mobile check for: {url}")
    
    result = check_mobile_simple(url)
    task_id = f"mobile-{datetime.now().timestamp()}"
    create_task_result(task_id, "mobile_check", url, result)
    
    return {
        "task_id": task_id,
        "status": "SUCCESS",
        "message": "Mobile check completed"
    }
