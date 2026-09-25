"""UUIDv7 identifiers (Jack's ruling, 2026-08-31): time-ordered, so inserts stay local in the index.

Python 3.11 has no uuid7 in the standard library, so it is built here from RFC 9562 section 5.7:
48 bits of Unix milliseconds, the version nibble, 12 random bits, the variant, 62 random bits.
"""

from __future__ import annotations

import os
import time
import uuid

_MASK_48 = (1 << 48) - 1
_MASK_12 = (1 << 12) - 1
_MASK_62 = (1 << 62) - 1


def uuid7(*, now_ms: int | None = None, rand: bytes | None = None) -> uuid.UUID:
    """A new UUIDv7. ``now_ms`` and ``rand`` (10 bytes) exist so tests can pin the output."""
    ms = (time.time_ns() // 1_000_000 if now_ms is None else now_ms) & _MASK_48
    bits = int.from_bytes(os.urandom(10) if rand is None else rand, "big")
    rand_a = (bits >> 62) & _MASK_12
    rand_b = bits & _MASK_62
    return uuid.UUID(
        int=(ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    )
