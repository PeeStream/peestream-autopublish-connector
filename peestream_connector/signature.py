"""Verification of PeeStream webhook signatures (contract v1.1, section 4).

Header format: ``X-PeeStream-Signature: sha256=<hex>`` where ``hex`` is
``HMAC-SHA256(webhook_secret, RAW_BODY)`` over the exact bytes that PeeStream
sent. The comparison is constant time (``hmac.compare_digest``).
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Optional

PREFIX = "sha256="


def compute(secret: str, raw_body: bytes) -> str:
    """Return the header value PeeStream would send for ``raw_body``."""
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return PREFIX + digest


def verify(secret: str, raw_body: bytes, header_value: Optional[str]) -> bool:
    """Constant-time check of a received signature header.

    Any malformed input (missing header, wrong algorithm, non-hex payload) is a
    rejection - never an exception - so the HTTP layer can answer 401 uniformly.
    """
    if not header_value or not isinstance(header_value, str):
        return False
    value = header_value.strip()
    if not value.startswith(PREFIX):
        return False
    provided = value[len(PREFIX):]
    if len(provided) != 64:
        return False
    try:
        bytes.fromhex(provided)
    except ValueError:
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, provided.lower())
