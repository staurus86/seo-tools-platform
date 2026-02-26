"""
Input validation utilities for SEO Tools Platform.
Centralised URL sanitisation applied to all tool request models.
"""
import re
from urllib.parse import urlparse
from typing import Any

from pydantic import BaseModel, field_validator

_DANGEROUS_SCHEMES = {"javascript", "data", "vbscript", "file"}
_MAX_URL_LENGTH = 2048


def validate_url(v: Any) -> str:
    """
    Validate and sanitise a URL value.

    Checks performed:
    - Must be a non-empty string
    - Maximum length 2 048 chars
    - Scheme must be http or https (rejects javascript:, data:, vbscript:, file:)
    - Must have a valid netloc (domain)

    Returns the stripped URL string on success, raises ValueError otherwise.
    """
    if not isinstance(v, str):
        raise ValueError("URL must be a string")

    v = v.strip()

    if not v:
        raise ValueError("URL cannot be empty")

    if len(v) > _MAX_URL_LENGTH:
        raise ValueError(f"URL too long (max {_MAX_URL_LENGTH} characters)")

    # Reject dangerous schemes before full parsing (handles obfuscated variants)
    low = v.lower().lstrip()
    for scheme in _DANGEROUS_SCHEMES:
        if low.startswith(scheme + ":"):
            raise ValueError(f"URL scheme '{scheme}' is not permitted")

    parsed = urlparse(v)

    if parsed.scheme not in ("http", "https"):
        raise ValueError("URL must start with http:// or https://")

    if not parsed.netloc:
        raise ValueError("URL must contain a valid domain")

    return v


def normalize_http_input(raw_value: str) -> str:
    """Prepend https:// if no URL scheme present; validate http/https scheme."""
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", value):
        value = f"https://{value}"
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return ""
    return value


class URLModel(BaseModel):
    """Base Pydantic model that validates the ``url`` field on all subclasses."""

    @field_validator("url", mode="before", check_fields=False)
    @classmethod
    def _sanitise_url(cls, v: Any) -> Any:
        if v is None or v == "":
            return v
        return validate_url(v)
