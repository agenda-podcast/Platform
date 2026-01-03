"""Base62 random ID generator with fixed length.

Alphabet: 0-9, A-Z, a-z (62 chars)
"""
from __future__ import annotations

import secrets
from typing import Final

BASE62_ALPHABET: Final[str] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_BASE: Final[int] = 62


def base62_random(length: int) -> str:
    """Return a cryptographically-random Base62 string of exact `length`."""
    if not isinstance(length, int) or length <= 0:
        raise ValueError("length must be a positive integer")

    # Use randbelow(62) per-character for uniform distribution.
    return "".join(BASE62_ALPHABET[secrets.randbelow(_BASE)] for _ in range(length))
