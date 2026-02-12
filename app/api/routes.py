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

def check_site_full(url: str, max_pages: int = 20) -> Dict[str, Any]:
    """Full site analysis - multi-page crawling with deep analysis"""
    import requests
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin, urlparse
    from collections import defaultdict, Counter
    import re
    import time
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    })
    
    parsed_base = urlparse(url)
    base_domain = parsed_base.netloc.lower()
    base_normalized = base_domain[4:] if base_domain.startswith('www.') else base_domain
    
    visited = set()
    to_visit = [(url, 0)]
    all_pages_data = []
    broken_links = []
    external_links = []
    internal_links = []
    all_urls = set()
    redirects = []
    technology_stack = []
    content_issues = []
    
    stop_words = {
        'и', 'в', 'на', 'что', 'это', 'по', 'с', 'для', 'при', 'или', 'как', 'от', 'до',
        'a', 'the', 'is', 'are', 'was', 'were', 'be', 'been', 'of', 'to', 'in'
    }
    
    def is_internal(url_to_check: str) -> bool:
        try:
            parsed = urlparse(url_to_check)
            netloc = parsed.netloc.lower()
            netloc_norm = netloc[4:] if netloc.startswith('www.') else netloc
            return netloc_norm == base_normalized or netloc_norm.endswith('.' + base_normalized)
        except:
            return False
    
    def is_valid_page(url_to_check: str) -> bool:
        try:
            parsed = urlparse(url_to_check)
            path = parsed.path.lower()
            exts = ['.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.bmp', '.ico', '.pdf', 
                    '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.mp3', '.mp4', '.css', 
                    '.js', '.json', '.xml', '.rss', '.woff', '.woff2', '.ttf']
            for ext in exts:
                if path.endswith(ext):
                    return False
            admin_paths = ['/admin', '/wp-admin', '/administrator', '/manage', '/login', 
                          '/logout', '/signup', '/register', '/api/', '/static/', '/media/']
            for admin in admin_paths:
                if admin in path:
                    return False
            return True
        except:
            return False
    
    def extract_tech(html_text: str) -> list:
        tech = []
        text = html_text.lower()
        if 'react' in text and 'data-react' not in text:
            tech.append('React')
        if 'vue' in text or 'vue.js' in text:
            tech.append('Vue.js')
        if 'angular' in text:
            tech.append('Angular')
        if 'jquery' in text:
            tech.append('jQuery')
        if 'bootstrap' in text:
            tech.append('Bootstrap')
        if 'wordpress' in text:
            tech.append('WordPress')
        if 'bitrix' in text or '1c-bitrix' in text:
            tech.append('1C-Bitrix')
        if 'django' in text:
            tech.append('Django')
        if 'flask' in text:
            tech.append('Flask')
        if 'laravel' in text:
            tech.append('Laravel')
        if 'node.js' in text or 'nodejs' in text:
            tech.append('Node.js')
        if 'php' in text:
            tech.append('PHP')
        if 'asp.net' in text or 'asp.net' in text.lower():
            tech.append('ASP.NET')
        if 'gatsby' in text:
            tech.append('Gatsby')
        if 'next.js' in text or 'nextjs' in text:
            tech.append('Next.js')
        if 'webpack' in text:
            tech.append('Webpack')
        if 'gulp' in text:
            tech.append('Gulp')
        if 'grunt' in text:
            tech.append('Grunt')
        if 'google tag manager' in text:
            tech.append('Google Tag Manager')
        if 'yandex.metrika' in text or 'yandex metrika' in text:
            tech.append('Yandex.Metrika')
        if 'google analytics' in text:
            tech.append('Google Analytics')
        if 'gtm.js' in html_text:
            tech.append('Google Tag Manager')
        return list(set(tech))
    
    def analyze_page(page_url: str, depth: int, html: str) -> Dict:
        soup = BeautifulSoup(html, 'html.parser')
        
        title = soup.find('title')
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        meta_robots = soup.find('meta', attrs={'name': 'robots'})
        canonical = soup.find('link', rel='canonical')
        h1_tags = soup.find_all('h1')
        h2_tags = soup.find_all('h2')
        
        images = soup.find_all('img')
        images_without_alt = [img for img in images if not img.get('alt') or not img.get('alt').strip()]
        images_with_empty_alt = [img for img in images if img.get('alt', '').strip() == '']
        
        links = soup.find_all('a')
        page_links = []
        for link in links:
            href = link.get('href', '')
            if href and not href.startswith('#') and not href.startswith('javascript'):
                page_links.append(href)
        
        scripts = soup.find_all('script')
        styles = soup.find_all('style')
        iframes = soup.find_all('iframe')
        
        body = soup.find('body')
        text_content = body.get_text(strip=True) if body else ''
        word_count = len(text_content.split())
        
        has_og_tags = bool(soup.find('meta', property='og:title'))
        has_twitter_cards = bool(soup.find('meta', property='twitter:card'))
        has_schema = bool(soup.find('script', type='application/ld+json'))
        
        page_issues = []
        if not title:
            page_issues.append({'type': 'critical', 'issue': 'Missing title tag', 'url': page_url})
        if title and len(title.text) < 30:
            page_issues.append({'type': 'warning', 'issue': 'Title too short (< 30 chars)', 'url': page_url})
        if title and len(title.text) > 70:
            page_issues.append({'type': 'warning', 'issue': 'Title too long (> 70 chars)', 'url': page_url})
        if not meta_desc:
            page_issues.append({'type': 'critical', 'issue': 'Missing meta description', 'url': page_url})
        if meta_desc and len(meta_desc.get('content', '')) < 120:
            page_issues.append({'type': 'warning', 'issue': 'Meta description too short', 'url': page_url})
        if len(h1_tags) == 0:
            page_issues.append({'type': 'warning', 'issue': 'No H1 tags found', 'url': page_url})
        elif len(h1_tags) > 1:
            page_issues.append({'type': 'warning', 'issue': f'Multiple H1 tags ({len(h1_tags)})', 'url': page_url})
        if len(images_without_alt) > 0:
            page_issues.append({'type': 'warning', 'issue': f'{len(images_without_alt)} images without alt text', 'url': page_url})
        
        return {
            'url': page_url,
            'depth': depth,
            'status': 'success',
            'title': title.text if title else None,
            'title_length': len(title.text) if title else 0,
            'meta_description': meta_desc.get('content') if meta_desc else None,
            'meta_keywords': meta_keywords.get('content') if meta_keywords else None,
            'meta_robots': meta_robots.get('content') if meta_robots else None,
            'canonical': canonical.get('href') if canonical else None,
            'h1_count': len(h1_tags),
            'h2_count': len(h2_tags),
            'images_total': len(images),
            'images_without_alt': len(images_without_alt),
            'images_empty_alt': len(images_with_empty_alt),
            'links_total': len(links),
            'links_found': page_links,
            'scripts_count': len(scripts),
            'styles_count': len(styles),
            'iframes_count': len(iframes),
            'word_count': word_count,
            'has_og_tags': has_og_tags,
            'has_twitter_cards': has_twitter_cards,
            'has_schema_org': has_schema,
            'issues': page_issues,
            'tech_stack': extract_tech(html)
        }
    
    start_time = time.time()
    pages_crawled = 0
    
    try:
        while to_visit and pages_crawled < max_pages:
            current_url, depth = to_visit.pop(0)
            
            if current_url in visited:
                continue
            if not is_valid_page(current_url):
                continue
            
            visited.add(current_url)
            all_urls.add(current_url)
            
            try:
                response = session.get(current_url, timeout=10, allow_redirects=True)
                
                if response.history:
                    for r in response.history:
                        redirects.append({
                            'from': r.url,
                            'to': response.url,
                            'status': r.status_code
                        })
                    current_url = response.url
                
                if response.status_code >= 400:
                    broken_links.append({
                        'url': current_url,
                        'status': response.status_code
                    })
                    pages_crawled += 1
                    continue
                
                page_data = analyze_page(current_url, depth, response.text)
                all_pages_data.append(page_data)
                internal_links.extend([l for l in page_data['links_found'] if is_internal(l)])
                
                if depth < 2:
                    for link in page_data['links_found'][:50]:
                        if link not in visited and len(all_urls) < max_pages * 2:
                            full_link = urljoin(current_url, link)
                            if is_internal(full_link) and is_valid_page(full_link):
                                to_visit.append((full_link, depth + 1))
                                all_urls.add(full_link)
                
                external_links.extend([l for l in page_data['links_found'] if not is_internal(l)])
                
                pages_crawled += 1
                
            except requests.exceptions.Timeout:
                broken_links.append({'url': current_url, 'status': 'timeout'})
                pages_crawled += 1
            except requests.exceptions.ConnectionError:
                broken_links.append({'url': current_url, 'status': 'connection_error'})
                pages_crawled += 1
            except Exception as e:
                broken_links.append({'url': current_url, 'status': str(e)})
                pages_crawled += 1
        
        elapsed_time = time.time() - start_time
        
        all_tech = []
        for page in all_pages_data:
            all_tech.extend(page.get('tech_stack', []))
        tech_counts = Counter(all_tech)
        top_technologies = [{'tech': t, 'count': c} for t, c in tech_counts.most_common(10)]
        
        all_issues = []
        for page in all_pages_data:
            all_issues.extend(page.get('issues', []))
        
        critical_issues = [i for i in all_issues if i.get('type') == 'critical']
        warning_issues = [i for i in all_issues if i.get('type') == 'warning']
        
        total_images = sum(p.get('images_total', 0) for p in all_pages_data)
        total_images_without_alt = sum(p.get('images_without_alt', 0) for p in all_pages_data)
        total_links = sum(p.get('links_total', 0) for p in all_pages_data)
        
        pages_with_title = len([p for p in all_pages_data if p.get('title')])
        pages_with_desc = len([p for p in all_pages_data if p.get('meta_description')])
        pages_with_h1 = len([p for p in all_pages_data if p.get('h1_count', 0) > 0])
        pages_with_schema = len([p for p in all_pages_data if p.get('has_schema_org')])
        
        avg_word_count = sum(p.get('word_count', 0) for p in all_pages_data) / max(1, len(all_pages_data))
        
        redirect_count = len(redirects)
        broken_count = len(broken_links)
        
        seo_score = 100
        seo_score -= min(30, len(critical_issues) * 10)
        seo_score -= min(20, len(warning_issues) * 3)
        seo_score -= min(20, total_images_without_alt * 0.5)
        seo_score -= min(10, redirect_count * 2)
        seo_score -= min(10, broken_count * 2)
        seo_score = max(0, seo_score)
        
        return {
            "task_type": "site_analyze",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "summary": {
                    "pages_crawled": pages_crawled,
                    "pages_total_found": len(all_urls),
                    "crawl_time_seconds": round(elapsed_time, 2),
                    "internal_links_count": len(set(internal_links)),
                    "external_links_count": len(set(external_links)),
                    "broken_links_count": broken_count,
                    "redirects_count": redirect_count,
                    "seo_score": seo_score,
                    "critical_issues": len(critical_issues),
                    "warning_issues": len(warning_issues)
                },
                "content_analysis": {
                    "total_images": total_images,
                    "images_without_alt": total_images_without_alt,
                    "total_links": total_links,
                    "average_word_count": round(avg_word_count, 0),
                    "pages_with_title": pages_with_title,
                    "pages_with_meta_desc": pages_with_desc,
                    "pages_with_h1": pages_with_h1,
                    "pages_with_schema_org": pages_with_schema
                },
                "technology_stack": top_technologies,
                "pages_detail": all_pages_data[:50],
                "broken_links": broken_links[:50],
                "redirects": redirects[:20],
                "all_issues": all_issues[:100],
                "critical_issues": critical_issues[:20],
                "warning_issues": warning_issues[:50],
                "recommendations": generate_recommendations(critical_issues, warning_issues, tech_counts, seo_score)
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


def generate_recommendations(critical_issues: list, warning_issues: list, tech_counts: dict, score: int) -> list:
    """Generate recommendations based on analysis results"""
    recommendations = []
    
    if score < 50:
        recommendations.append({"priority": "high", "text": "Критически низкий SEO- score. Требуется полный аудит и исправления."})
    
    if any('title' in i.get('issue', '').lower() for i in critical_issues):
        recommendations.append({"priority": "high", "text": "Добавьте Title тег на все страницы (60-70 символов)"})
    
    if any('description' in i.get('issue', '').lower() for i in critical_issues):
        recommendations.append({"priority": "high", "text": "Добавьте Meta Description на все страницы (120-160 символов)"})
    
    if any('h1' in i.get('issue', '').lower() for i in warning_issues):
        recommendations.append({"priority": "medium", "text": "Добавьте H1 тег на каждую страницу (1 тег на страницу)"})
    
    img_issues = [i for i in warning_issues if 'image' in i.get('issue', '').lower() or 'alt' in i.get('issue', '').lower()]
    if img_issues:
        recommendations.append({"priority": "medium", "text": f"Добавьте alt-текст к {sum(1 for i in img_issues)} изображениям"})
    
    if 'WordPress' in tech_counts:
        recommendations.append({"priority": "medium", "text": "Установите SEO-плагин для WordPress (Yoast SEO или Rank Math)"})
    
    if not any('Google Analytics' in t or 'Yandex.Metrika' in t for t in tech_counts.keys()):
        recommendations.append({"priority": "low", "text": "Добавьте системы аналитики (Google Analytics, Yandex.Metrika)"})
    
    schema_issues = [i for i in critical_issues + warning_issues if 'schema' in i.get('issue', '').lower()]
    if schema_issues or len([p for p in [] if p.get('has_schema_org')]) == 0:
        recommendations.append({"priority": "medium", "text": "Добавьте Schema.org разметку для улучшения сниппетов в поиске"})
    
    recommendations.append({"priority": "low", "text": "Оптимизируйте скорость загрузки страниц"})
    recommendations.append({"priority": "low", "text": "Настройте канонические URL для дублирующихся страниц"})
    recommendations.append({"priority": "low", "text": "Добавьте Open Graph и Twitter Cards мета-теги"})
    
    return recommendations


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
    max_pages: int = 20

class RenderAuditRequest(BaseModel):
    url: str

class MobileCheckRequest(BaseModel):
    url: str


@router.post("/tasks/site-analyze")
async def create_site_analyze(data: SiteAnalyzeRequest):
    """Full site analysis with multi-page crawling"""
    url = data.url
    max_pages = min(data.max_pages, 50)
    
    print(f"[API] Full site analysis for: {url} (max_pages={max_pages})")
    
    result = check_site_full(url, max_pages)
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
