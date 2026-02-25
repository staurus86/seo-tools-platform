"""SSRF and URL safety helpers for LLM crawler."""
from __future__ import annotations

import ipaddress
import re
import socket
from typing import List
from urllib.parse import urljoin, urlparse, urlunparse


ALLOWED_SCHEMES = {"http", "https"}


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
    resolved: List[str] = []
    infos = socket.getaddrinfo(hostname, None)
    for info in infos:
        sockaddr = info[4]
        if not sockaddr:
            continue
        ip_val = str(sockaddr[0] or "").strip()
        if ip_val and ip_val not in resolved:
            resolved.append(ip_val)
    return resolved


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


def safe_redirect_target(current_url: str, location: str) -> str:
    target = urljoin(current_url, str(location or "").strip())
    normalized = normalize_http_url(target)
    if not normalized:
        raise ValueError("Unsafe redirect target")
    assert_safe_url(normalized)
    return normalized

