"""SSRF and URL safety helpers for LLM crawler."""
from __future__ import annotations

import ipaddress
import re
import socket
import time
from typing import Dict, FrozenSet, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse


ALLOWED_SCHEMES = {"http", "https"}

_DNS_CACHE_TTL_SEC = 30
_dns_cache: Dict[str, Tuple[List[str], float]] = {}


def _normalize_hostname(hostname: str) -> str:
    return str(hostname or "").strip().lower()


def resolve_hostname_ips_cached(hostname: str) -> List[str]:
    """Resolve hostname with TTL-based cache to mitigate DNS rebinding attacks."""
    global _dns_cache
    
    normalized = _normalize_hostname(hostname)
    if not normalized:
        return []
    
    now = time.time()
    cached = _dns_cache.get(normalized)
    if cached is not None:
        ips, cached_at = cached
        if now - cached_at < _DNS_CACHE_TTL_SEC:
            return ips
    
    resolved = resolve_hostname_ips_uncached(normalized)
    _dns_cache[normalized] = (resolved, now)
    
    if len(_dns_cache) > 1000:
        oldest_keys = sorted(_dns_cache.keys(), key=lambda k: _dns_cache[k][1])[:500]
        for key in oldest_keys:
            del _dns_cache[key]
    
    return resolved


def resolve_hostname_ips_uncached(hostname: str) -> List[str]:
    """Resolve hostname to IP addresses (no cache)."""
    resolved: List[str] = []
    try:
        infos = socket.getaddrinfo(hostname, None)
        for info in infos:
            sockaddr = info[4]
            if not sockaddr:
                continue
            ip_val = str(sockaddr[0] or "").strip()
            if ip_val and ip_val not in resolved:
                resolved.append(ip_val)
    except socket.gaierror:
        pass
    return resolved


def get_allowed_ips_for_url(url: str) -> FrozenSet[str]:
    """Get allowed IPs for a URL - useful for redirect chain validation."""
    parsed = urlparse(url)
    hostname = str(parsed.hostname or "").strip()
    if not hostname:
        return frozenset()
    return frozenset(resolve_hostname_ips_cached(hostname))


def normalize_http_url(raw_url: str, max_length: int = 2048) -> str:
    value = str(raw_url or "").strip()
    if not value or len(value) > max_length:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", value):
        value = f"https://{value}"
    parsed = urlparse(value)
    if parsed.scheme.lower() not in ALLOWED_SCHEMES:
        return ""
    if not parsed.netloc:
        return ""
    path = parsed.path or "/"
    return urlunparse((parsed.scheme.lower(), parsed.netloc, path, "", parsed.query or "", ""))


def _is_forbidden_hostname(hostname: str) -> bool:
    host = str(hostname or "").strip().lower().strip(".")
    if not host:
        return True
    if host in {"localhost", "localhost.localdomain"}:
        return True
    if host.endswith(".internal"):
        return True
    return False


def _is_forbidden_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except Exception:
        return True
    return bool(
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def resolve_hostname_ips(hostname: str) -> List[str]:
    """Resolve hostname to IP addresses (backward compatibility wrapper)."""
    return resolve_hostname_ips_cached(hostname)


def assert_safe_url(url: str) -> None:
    parsed = urlparse(url)
    scheme = str(parsed.scheme or "").lower()
    hostname = str(parsed.hostname or "").strip()
    if scheme not in ALLOWED_SCHEMES:
        raise ValueError("Only http/https URLs are allowed")
    if _is_forbidden_hostname(hostname):
        raise ValueError("Hostname is blocked by SSRF policy")
    ips = resolve_hostname_ips(hostname)
    if not ips:
        raise ValueError("Cannot resolve hostname")
    for ip in ips:
        if _is_forbidden_ip(ip):
            raise ValueError(f"Blocked target IP by SSRF policy: {ip}")


def safe_redirect_target(current_url: str, location: str, allowed_ips: Optional[FrozenSet[str]] = None) -> str:
    """Validate redirect target against allowed IPs to prevent DNS rebinding."""
    target = urljoin(current_url, str(location or "").strip())
    normalized = normalize_http_url(target)
    if not normalized:
        raise ValueError("Unsafe redirect target")
    
    target_allowed_ips = get_allowed_ips_for_url(normalized)
    
    if allowed_ips is not None and target_allowed_ips:
        if not allowed_ips.intersection(target_allowed_ips):
            raise ValueError("Redirect leads to different IP range (DNS rebinding blocked)")
    
    assert_safe_url(normalized)
    return normalized

