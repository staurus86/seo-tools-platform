"""
Site Analyze router.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import APIRouter

from app.validators import URLModel
from app.api.routers._task_store import create_task_result

router = APIRouter(tags=["SEO Tools"])


def check_site_full(input_url: str, max_pages: int = 20) -> Dict[str, Any]:
    """
    Full site analysis - максимально приближено к оригинальному seopro.py
    Краулинг, анализ контента, технологии, ссылки, редиректы
    """
    import requests
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin, urlparse
    from collections import defaultdict, Counter
    import re
    import time
    
    url = input_url  # Use input_url as url for compatibility
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    })
    session.verify = False
    
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
    sitemap_urls = set()
    
    # Для подсчёта внутренних ссылок (упрощённый page authority)
    internal_links_count = defaultdict(int)
    
    def is_internal(url_to_check: str) -> bool:
        try:
            parsed = urlparse(url_to_check)
            netloc = parsed.netloc.lower()
            netloc_norm = netloc[4:] if netloc.startswith('www.') else netloc
            return netloc_norm == base_normalized or netloc_norm.endswith('.' + base_normalized)
        except:
            return False
    
    def normalize_url(url: str, drop_query: bool = True) -> str:
        if not url:
            return ''
        try:
            parsed = urlparse(url)
            scheme = (parsed.scheme or '').lower()
            netloc = parsed.netloc or ''
            if netloc.endswith(':80'):
                netloc = netloc[:-3]
            if netloc.endswith(':443'):
                netloc = netloc[:-4]
            path = parsed.path or '/'
            if path != '/':
                path = path.rstrip('/')
            query = '' if drop_query else parsed.query
            return f"{scheme}://{netloc}{path}" + (f"?{query}" if query else '')
        except:
            return url.strip()
    
    def is_valid_page(url_to_check: str) -> bool:
        try:
            parsed = urlparse(url_to_check)
            path = parsed.path.lower()
            exts = ['.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.bmp', '.ico', '.pdf', 
                    '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.mp3', '.mp4', '.css', 
                    '.js', '.json', '.xml', '.rss', '.woff', '.woff2', '.ttf', '.eot', '.otf']
            for ext in exts:
                if path.endswith(ext):
                    return False
            admin_paths = ['/admin', '/wp-admin', '/administrator', '/manage', '/login', 
                          '/logout', '/signup', '/register', '/api/', '/static/', '/media/', 
                          '/uploads/', '/cdn/', '/captcha', '/recaptcha']
            for admin in admin_paths:
                if admin in path:
                    return False
            return True
        except:
            return False
    
    def looks_blocked(response) -> bool:
        try:
            if response is None:
                return True
            if response.status_code in (403, 429, 503):
                return True
            body = (response.text or '').lower()
            blocked_words = ['captcha', 'access denied', 'forbidden', 'cloudflare', 'ddos-guard', 
                           'ваш доступ заблокирован', 'доступ запрещён']
            if any(x in body for x in blocked_words):
                return True
            if len(body.strip()) < 800:
                return True
        except:
            return True
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
        if 'php' in text and 'php' not in tech:
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
        if 'google tag manager' in text or 'gtm.js' in html_text:
            tech.append('Google Tag Manager')
        if 'yandex.metrika' in text or 'яндекс.метрика' in text:
            tech.append('Yandex.Metrika')
        if 'google analytics' in text:
            tech.append('Google Analytics')
        return list(set(tech))
    
    def analyze_page(page_url: str, depth: int, html: str, response) -> Dict:
        soup = BeautifulSoup(html, 'html.parser')
        
        title = soup.find('title')
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        meta_robots = soup.find('meta', attrs={'name': 'robots'})
        canonical = soup.find('link', rel='canonical')
        og_title = soup.find('meta', property='og:title')
        og_desc = soup.find('meta', property='og:description')
        twitter_card = soup.find('meta', attrs={'name': 'twitter:card'})
        
        h1_tags = soup.find_all('h1')
        h2_tags = soup.find_all('h2')
        h3_tags = soup.find_all('h3')
        
        images = soup.find_all('img')
        images_without_alt = [img for img in images if not img.get('alt') or not img.get('alt').strip()]
        images_empty_alt = [img for img in images if img.get('alt', '').strip() == '']
        
        links = soup.find_all('a')
        page_links = []
        dofollow = 0
        nofollow = 0
        
        for link in links:
            href = link.get('href', '')
            if href and not href.startswith('#') and not href.startswith('javascript'):
                rel = link.get('rel', [])
                if 'nofollow' in rel:
                    nofollow += 1
                else:
                    dofollow += 1
                page_links.append(href)
        
        scripts = soup.find_all('script')
        styles = soup.find_all('style')
        iframes = soup.find_all('iframe')
        
        body = soup.find('body')
        text_content = body.get_text(strip=True) if body else ''
        word_count = len(text_content.split())
        char_count = len(text_content)
        
        has_json_ld = len(soup.find_all('script', {'type': 'application/ld+json'}))
        has_microdata = len(soup.find_all(attrs={'itemscope': True}))
        has_rdfa = len(soup.find_all(attrs={'typeof': True}))
        has_hreflang = len(soup.find_all('link', {'rel': 'alternate', 'hreflang': True}))
        
        breadcrumbs = soup.find_all(class_=re.compile(r'breadcrumb', re.I))
        has_breadcrumbs = 1 if breadcrumbs else 0
        
        page_issues = []
        
        if not title:
            page_issues.append({'type': 'critical', 'issue': 'Отсутствует тег Title', 'url': page_url})
        else:
            if len(title.text) < 30:
                page_issues.append({'type': 'warning', 'issue': f'Title слишком короткий ({len(title.text)} символов)', 'url': page_url})
            elif len(title.text) > 70:
                page_issues.append({'type': 'warning', 'issue': f'Title слишком длинный ({len(title.text)} символов)', 'url': page_url})
        
        if not meta_desc:
            page_issues.append({'type': 'critical', 'issue': 'Отсутствует Meta Description', 'url': page_url})
        elif len(meta_desc.get('content', '')) < 120:
            page_issues.append({'type': 'warning', 'issue': 'Meta Description слишком короткий', 'url': page_url})
        
        if len(h1_tags) == 0:
            page_issues.append({'type': 'warning', 'issue': 'Отсутствует тег H1', 'url': page_url})
        elif len(h1_tags) > 1:
            page_issues.append({'type': 'warning', 'issue': f'Много тегов H1 ({len(h1_tags)})', 'url': page_url})
        
        if len(images_without_alt) > 0:
            page_issues.append({'type': 'warning', 'issue': f'{len(images_without_alt)} изображений без alt-текста', 'url': page_url})
        
        if not has_json_ld and not has_microdata:
            page_issues.append({'type': 'info', 'issue': 'Отсутствует Schema.org разметка', 'url': page_url})
        
        if not og_title or not og_desc:
            page_issues.append({'type': 'info', 'issue': 'Отсутствуют Open Graph теги', 'url': page_url})
        
        if not twitter_card:
            page_issues.append({'type': 'info', 'issue': 'Отсутствует Twitter Cards', 'url': page_url})
        
        if has_breadcrumbs == 0:
            page_issues.append({'type': 'info', 'issue': 'Отсутствуют хлебные крошки', 'url': page_url})
        
        return {
            'url': page_url,
            'depth': depth,
            'status': 'success' if response and response.status_code < 400 else 'error',
            'status_code': response.status_code if response else None,
            'title': title.text if title else None,
            'title_length': len(title.text) if title else 0,
            'meta_description': meta_desc.get('content') if meta_desc else None,
            'meta_keywords': meta_keywords.get('content') if meta_keywords else None,
            'meta_robots': meta_robots.get('content') if meta_robots else None,
            'canonical': canonical.get('href') if canonical else None,
            'og_tags': bool(og_title and og_desc),
            'twitter_card': bool(twitter_card),
            'h1_count': len(h1_tags),
            'h2_count': len(h2_tags),
            'h3_count': len(h3_tags),
            'images_total': len(images),
            'images_without_alt': len(images_without_alt),
            'images_empty_alt': len(images_empty_alt),
            'links_total': len(links),
            'links_found': page_links,
            'dofollow': dofollow,
            'nofollow': nofollow,
            'scripts_count': len(scripts),
            'styles_count': len(styles),
            'iframes_count': len(iframes),
            'word_count': word_count,
            'char_count': char_count,
            'has_json_ld': has_json_ld,
            'has_microdata': has_microdata,
            'has_rdfa': has_rdfa,
            'has_hreflang': has_hreflang,
            'has_breadcrumbs': has_breadcrumbs,
            'tech_stack': extract_tech(html),
            'issues': page_issues,
            'load_time': response.elapsed.total_seconds() if response and hasattr(response, 'elapsed') else None
        }
    
    def load_sitemap() -> bool:
        """Загрузка sitemap.xml из robots.txt или стандартных путей"""
        try:
            robots_url = urljoin(url, '/robots.txt')
            try:
                response = session.get(robots_url, timeout=10)
                if response.status_code == 200:
                    for line in response.text.split('\n'):
                        line = line.strip()
                        if line.lower().startswith('sitemap:'):
                            sitemap_url = line.split(':', 1)[1].strip()
                            try:
                                sm_response = session.get(sitemap_url, timeout=15)
                                if sm_response.status_code == 200:
                                    soup = BeautifulSoup(sm_response.content, 'xml')
                                    for url_tag in soup.find_all('url'):
                                        loc = url_tag.find('loc')
                                        if loc and loc.text:
                                            sitemap_urls.add(loc.text.strip())
                            except:
                                pass
            except:
                pass
        except:
            pass
        
        standard_paths = ['/sitemap.xml', '/sitemap_index.xml', '/sitemaps/sitemap.xml', 
                          '/wp-sitemap.xml', '/sitemap/sitemap.xml']
        for path in standard_paths:
            sitemap_url = urljoin(url, path)
            try:
                response = session.get(sitemap_url, timeout=15)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'xml')
                    for url_tag in soup.find_all('url'):
                        loc = url_tag.find('loc')
                        if loc and loc.text:
                            sitemap_urls.add(loc.text.strip())
                    return True
            except:
                continue
        return False
    
    # Загружаем sitemap
    load_sitemap()
    
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
                            'from': normalize_url(r.url),
                            'to': normalize_url(response.url),
                            'status': r.status_code
                        })
                    current_url = response.url
                
                if response.status_code >= 400:
                    broken_links.append({
                        'url': normalize_url(current_url),
                        'status': response.status_code,
                        'error': response.reason if hasattr(response, 'reason') else 'Error'
                    })
                    pages_crawled += 1
                    continue
                
                if looks_blocked(response):
                    pages_crawled += 1
                    continue
                
                page_data = analyze_page(current_url, depth, response.text, response)
                all_pages_data.append(page_data)
                
                if depth < 2:
                    for link in page_data.get('links_found', [])[:30]:
                        if link:
                            full_link = urljoin(current_url, link)
                            norm_link = normalize_url(full_link)
                            if is_internal(full_link) and is_valid_page(full_link) and norm_link not in visited:
                                to_visit.append((full_link, depth + 1))
                                all_urls.add(full_link)
                                internal_links.append(norm_link)
                            elif not is_internal(full_link):
                                external_links.append({'url': norm_link, 'text': '', 'follow': 'dofollow'})
                
                pages_crawled += 1
                
            except requests.exceptions.Timeout:
                broken_links.append({'url': normalize_url(current_url), 'status': 'timeout', 'error': 'Connection timeout'})
                pages_crawled += 1
            except requests.exceptions.ConnectionError:
                broken_links.append({'url': normalize_url(current_url), 'status': 'connection_error', 'error': 'Connection error'})
                pages_crawled += 1
            except Exception as e:
                broken_links.append({'url': normalize_url(current_url), 'status': 'error', 'error': str(e)[:100]})
                pages_crawled += 1
        
        elapsed_time = time.time() - start_time
        
        # Агрегация результатов
        all_tech = []
        for page in all_pages_data:
            all_tech.extend(page.get('tech_stack', []))
        tech_counts = Counter(all_tech)
        top_technologies = [{'tech': t, 'count': c} for t, c in tech_counts.most_common(15)]
        
        all_issues = []
        for page in all_pages_data:
            all_issues.extend(page.get('issues', []))
        
        critical_issues = [i for i in all_issues if i.get('type') == 'critical']
        warning_issues = [i for i in all_issues if i.get('type') == 'warning']
        info_issues = [i for i in all_issues if i.get('type') == 'info']
        
        total_images = sum(p.get('images_total', 0) for p in all_pages_data)
        total_images_without_alt = sum(p.get('images_without_alt', 0) for p in all_pages_data)
        total_links = sum(p.get('links_total', 0) for p in all_pages_data)
        
        pages_with_title = len([p for p in all_pages_data if p.get('title')])
        pages_with_desc = len([p for p in all_pages_data if p.get('meta_description')])
        pages_with_h1 = len([p for p in all_pages_data if p.get('h1_count', 0) > 0])
        pages_with_schema = len([p for p in all_pages_data if p.get('has_json_ld') or p.get('has_microdata')])
        pages_with_og = len([p for p in all_pages_data if p.get('og_tags')])
        pages_with_breadcrumbs = len([p for p in all_pages_data if p.get('has_breadcrumbs')])
        
        avg_word_count = sum(p.get('word_count', 0) for p in all_pages_data) / max(1, len(all_pages_data))
        avg_load_time = sum(p.get('load_time', 0) for p in all_pages_data if p.get('load_time')) / max(1, len([p for p in all_pages_data if p.get('load_time')]))
        
        # SEO Score расчёт
        seo_score = 100
        seo_score -= min(30, len(critical_issues) * 10)
        seo_score -= min(20, len(warning_issues) * 3)
        seo_score -= min(10, total_images_without_alt * 0.5)
        seo_score -= min(10, len(broken_links) * 2)
        seo_score -= min(5, len(redirects) * 1)
        seo_score -= min(5, len(info_issues) * 0.5)
        seo_score = max(0, min(100, seo_score))
        
        # Категоризация по страницам
        good_pages = len([p for p in all_pages_data if len([i for i in p.get('issues', []) if i.get('type') in ['critical', 'warning']]) == 0])
        average_pages = len([p for p in all_pages_data if len([i for i in p.get('issues', []) if i.get('type') == 'critical']) == 0 and len([i for i in p.get('issues', []) if i.get('type') == 'warning']) <= 2])
        bad_pages = len(all_pages_data) - good_pages - average_pages
        
        return {
            "task_type": "site_analyze",
            "url": url,
            "completed_at": datetime.utcnow().isoformat(),
            "results": {
                "summary": {
                    "pages_crawled": pages_crawled,
                    "pages_total_found": len(all_urls),
                    "crawl_time_seconds": round(elapsed_time, 2),
                    "sitemap_urls_found": len(sitemap_urls),
                    "internal_links_count": len(set(internal_links)),
                    "external_links_count": len(set([e.get('url') for e in external_links])),
                    "broken_links_count": len(broken_links),
                    "redirects_count": len(redirects),
                    "seo_score": seo_score,
                    "critical_issues": len(critical_issues),
                    "warning_issues": len(warning_issues),
                    "info_issues": len(info_issues),
                    "good_pages": good_pages,
                    "average_pages": average_pages,
                    "bad_pages": max(0, bad_pages)
                },
                "content_analysis": {
                    "total_images": total_images,
                    "images_without_alt": total_images_without_alt,
                    "total_links": total_links,
                    "average_word_count": round(avg_word_count, 0),
                    "average_load_time": round(avg_load_time, 3),
                    "pages_with_title": pages_with_title,
                    "pages_with_meta_desc": pages_with_desc,
                    "pages_with_h1": pages_with_h1,
                    "pages_with_schema_org": pages_with_schema,
                    "pages_with_og_tags": pages_with_og,
                    "pages_with_breadcrumbs": pages_with_breadcrumbs
                },
                "technology_stack": top_technologies,
                "pages_detail": all_pages_data[:30],
                "broken_links": broken_links[:30],
                "redirects": redirects[:20],
                "sitemap_urls": list(sitemap_urls)[:50],
                "all_issues": all_issues[:100],
                "critical_issues": critical_issues[:20],
                "warning_issues": warning_issues[:50],
                "info_issues": info_issues[:30],
                "external_links_sample": external_links[:30],
                "recommendations": generate_recommendations_full(url, critical_issues, warning_issues, info_issues, tech_counts, seo_score, pages_crawled)
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
            page_issues.append({'type': 'critical', 'issue': 'Отсутствует тег title', 'url': page_url})
        if title and len(title.text) < 30:
            page_issues.append({'type': 'warning', 'issue': 'Слишком короткий title (< 30 символов)', 'url': page_url})
        if title and len(title.text) > 70:
            page_issues.append({'type': 'warning', 'issue': 'Слишком длинный title (> 70 символов)', 'url': page_url})
        if not meta_desc:
            page_issues.append({'type': 'critical', 'issue': 'Отсутствует meta description', 'url': page_url})
        if meta_desc and len(meta_desc.get('content', '')) < 120:
            page_issues.append({'type': 'warning', 'issue': 'Слишком короткий meta description', 'url': page_url})
        if len(h1_tags) == 0:
            page_issues.append({'type': 'warning', 'issue': 'Не найден H1', 'url': page_url})
        elif len(h1_tags) > 1:
            page_issues.append({'type': 'warning', 'issue': f'Несколько H1 ({len(h1_tags)})', 'url': page_url})
        if len(images_without_alt) > 0:
            page_issues.append({'type': 'warning', 'issue': f'Изображений без alt: {len(images_without_alt)}', 'url': page_url})
        
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


def generate_recommendations_full(site_url: str, critical_issues: list, warning_issues: list, info_issues: list, tech_counts: dict, score: int, pages_crawled: int) -> list:
    """Генерация рекомендаций на основе анализа - приближено к оригинальному скрипту"""
    recommendations = []
    
    if score < 30:
        recommendations.append({"priority": "critical", "text": "КРИТИЧЕСКИ НИЗКИЙ SEO SCORE! Требуется немедленный аудит и исправления всех критических проблем."})
    elif score < 50:
        recommendations.append({"priority": "high", "text": "Низкая SEO-оценка. Необходимо исправить критические проблемы в первую очередь."})
    elif score < 70:
        recommendations.append({"priority": "medium", "text": "Средняя SEO-оценка. Есть возможности для улучшения."})
    
    # Критические проблемы
    if any('title' in i.get('issue', '').lower() for i in critical_issues):
        count = len([i for i in critical_issues if 'title' in i.get('issue', '').lower()])
        recommendations.append({"priority": "high", "text": f"Добавьте Title теги на {count} страниц (оптимальная длина 60-70 символов)"})
    
    if any('description' in i.get('issue', '').lower() for i in critical_issues):
        count = len([i for i in critical_issues if 'description' in i.get('issue', '').lower()])
        recommendations.append({"priority": "high", "text": f"Добавьте Meta Description на {count} страниц (оптимальная длина 120-160 символов)"})
    
    if any('h1' in i.get('issue', '').lower() for i in warning_issues):
        count = len([i for i in warning_issues if 'h1' in i.get('issue', '').lower()])
        recommendations.append({"priority": "medium", "text": f"Исправьте теги H1 на {count} страниц (должен быть 1 тег H1 на страницу)"})
    
    # Изображения
    img_issues = [i for i in warning_issues if 'image' in i.get('issue', '').lower() or 'alt' in i.get('issue', '').lower()]
    if img_issues:
        total_img = sum(1 for i in img_issues)
        recommendations.append({"priority": "medium", "text": f"Добавьте alt-текст к {total_img} изображениям для улучшения SEO и доступности"})
    
    # Технологии
    if 'WordPress' in tech_counts:
        recommendations.append({"priority": "medium", "text": "Для WordPress установите SEO-плагин: Yoast SEO или Rank Math"})
    
    if 'jQuery' in tech_counts and 'React' not in tech_counts and 'Vue.js' not in tech_counts:
        recommendations.append({"priority": "low", "text": "Сайт использует jQuery. Рассмотрите переход на современный JS-фреймворк"})
    
    # Schema.org
    schema_issues = [i for i in info_issues if 'schema' in i.get('issue', '').lower()]
    if schema_issues:
        recommendations.append({"priority": "medium", "text": "Добавьте Schema.org разметку (Organization, Breadcrumb, Product, Article) для улучшения сниппетов в поиске"})
    
    # Open Graph
    og_issues = [i for i in info_issues if 'open graph' in i.get('issue', '').lower()]
    if og_issues:
        recommendations.append({"priority": "medium", "text": "Добавьте Open Graph теги (og:title, og:description, og:image) для улучшения шаринга в соцсетях"})
    
    # Twitter Cards
    twitter_issues = [i for i in info_issues if 'twitter' in i.get('issue', '').lower()]
    if twitter_issues:
        recommendations.append({"priority": "low", "text": "Добавьте Twitter Cards мета-теги для красивого отображения ссылок в Twitter"})
    
    # Хлебные крошки
    breadcrumb_issues = [i for i in info_issues if 'хлеб' in i.get('issue', '').lower() or 'breadcrumb' in i.get('issue', '').lower()]
    if breadcrumb_issues:
        recommendations.append({"priority": "medium", "text": "Добавьте хлебные крошки на страницы для улучшения навигации и SEO"})
    
    # Аналитика
    has_analytics = any('Analytics' in t or 'Metrika' in t for t in tech_counts.keys())
    if not has_analytics:
        recommendations.append({"priority": "medium", "text": "Подключите системы аналитики: Google Analytics и/или Yandex.Metrika для отслеживания посещаемости"})
    
    # Скорость
    recommendations.append({"priority": "high", "text": "Оптимизируйте скорость загрузки страниц (сжатие изображений, минификация CSS/JS, кэширование)"})
    
    # Битые ссылки
    broken_count = len([i for i in critical_issues if 'broken' in i.get('issue', '').lower() or any(x in i.get('issue', '').lower() for x in ['404', 'ошибка', 'error'])])
    if broken_count > 0:
        recommendations.append({"priority": "high", "text": f"Найдено {broken_count} битых ссылок. Проверьте и исправьте или удалите их"})
    
    # Канонические URL
    canon_issues = [i for i in warning_issues if 'canonical' in i.get('issue', '').lower()]
    if canon_issues:
        recommendations.append({"priority": "medium", "text": "Настройте канонические URL для всех страниц во избежание дублирования контента"})
    
    # Мобильная адаптация
    mobile_issues = [i for i in critical_issues if 'mobile' in i.get('issue', '').lower() or 'viewport' in i.get('issue', '').lower()]
    if mobile_issues:
        recommendations.append({"priority": "high", "text": "Адаптируйте сайт для мобильных устройств (Viewport, Responsive Design)"})
    
    # HTTPS
    if not site_url.startswith('https://'):
        recommendations.append({"priority": "high", "text": "Перенесите сайт на HTTPS для безопасности и SEO"})
    
    return recommendations


class SiteAnalyzeRequest(URLModel):
    url: str
    max_pages: int = 20


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
