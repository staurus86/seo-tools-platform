"""
Geo-proxy rotation for SEO Tools Platform.

Reads GEO_PROXY_LIST env var (Webshare format: ip:port:user:pass, one per line or comma-separated).
Provides helpers for requests, aiohttp, and Playwright.
"""
import itertools
import logging
import os
import threading
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_cycle = None
_proxies: list = []


def _parse_proxy_list() -> list[Dict[str, str]]:
    raw = os.getenv("GEO_PROXY_LIST", "").strip()
    if not raw:
        return []
    entries = []
    for line in raw.replace(",", "\n").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(":")
        if len(parts) == 4:
            ip, port, user, pwd = parts
            entries.append({"ip": ip, "port": port, "user": user, "pass": pwd})
        elif len(parts) == 2:
            ip, port = parts
            entries.append({"ip": ip, "port": port, "user": "", "pass": ""})
    return entries


def _get_cycle():
    global _cycle, _proxies
    with _lock:
        if _cycle is None:
            _proxies = _parse_proxy_list()
            if _proxies:
                _cycle = itertools.cycle(_proxies)
                logger.info("Geo-proxy pool loaded: %d proxies", len(_proxies))
            else:
                logger.info("No geo-proxies configured (GEO_PROXY_LIST empty)")
        return _cycle


def _next_proxy() -> Optional[Dict[str, str]]:
    cycle = _get_cycle()
    if cycle is None:
        return None
    with _lock:
        return next(cycle)


def is_proxy_available() -> bool:
    _get_cycle()
    return len(_proxies) > 0


def get_requests_proxies() -> Optional[Dict[str, str]]:
    """Return proxies dict for requests.Session().proxies or requests.get(proxies=...)."""
    p = _next_proxy()
    if not p:
        return None
    if p["user"]:
        url = f"http://{p['user']}:{p['pass']}@{p['ip']}:{p['port']}"
    else:
        url = f"http://{p['ip']}:{p['port']}"
    return {"http": url, "https": url}


def get_aiohttp_proxy() -> Optional[str]:
    """Return proxy URL string for aiohttp.ClientSession.get(proxy=...)."""
    p = _next_proxy()
    if not p:
        return None
    if p["user"]:
        return f"http://{p['user']}:{p['pass']}@{p['ip']}:{p['port']}"
    return f"http://{p['ip']}:{p['port']}"


def get_playwright_proxy() -> Optional[Dict[str, str]]:
    """Return proxy dict for playwright browser.launch(proxy=...) or browser.new_context(proxy=...)."""
    p = _next_proxy()
    if not p:
        return None
    proxy = {"server": f"http://{p['ip']}:{p['port']}"}
    if p["user"]:
        proxy["username"] = p["user"]
        proxy["password"] = p["pass"]
    return proxy
