"""Consistent-hashing primitives with deterministic, auditable placement.

Two algorithms are provided:

* :func:`jump_hash` — Lamping & Veach's jump consistent hash: O(log n) time,
  O(1) memory, moves ~1/(n+1) of keys when a bucket is added.
* :func:`rendezvous` — highest-random-weight hashing: O(n) time, supports
  weights but moves ~1/(n+1) of keys on average when a bucket is added.

Both are pure functions of ``(key, buckets)``; no state, no randomness.
"""

from __future__ import annotations

import hashlib
import math

__all__ = [
    "jump_hash",
    "rendezvous",
    "weighted_rendezvous",
    "score",
    "audit",
]

_MASK64 = (1 << 64) - 1
_GOLDEN = 0x9E3779B97F4A7C15


def _as_bytes(key: object) -> bytes:
    if isinstance(key, bytes):
        return key
    if isinstance(key, str):
        return key.encode("utf-8")
    if isinstance(key, int):
        return key.to_bytes(8, "little", signed=False)
    raise TypeError(f"unsupported key type: {type(key).__name__}")


def _u64(key: object, bucket: int, salt: bytes = b"") -> int:
    digest = hashlib.blake2b(
        _as_bytes(key) + salt + bucket.to_bytes(8, "little"), digest_size=8
    ).digest()
    return int.from_bytes(digest, "little")


def jump_hash(key: int, buckets: int) -> int:
    """Return the bucket for ``key`` in ``[0, buckets)`` (Lamping & Veach, 2014)."""
    if buckets < 1:
        raise ValueError("buckets must be >= 1")
    key &= _MASK64
    b = -1
    j = 0
    while j < buckets:
        b = j
        key = (key * 2862933555777941757 + 1) & _MASK64
        j = int((b + 1) * (1 << 31) / ((key >> 33) + 1))
    return b


def score(key: object, bucket: int, weight: float = 1.0, salt: bytes = b"") -> float:
    """Rendezvous score: larger wins. ``weight`` biases the choice."""
    if weight <= 0:
        raise ValueError("weight must be > 0")
    return -weight / math.log((_u64(key, bucket, salt) + 1) / 2**64)


def rendezvous(key: object, buckets: int, salt: bytes = b"") -> int:
    """Return the highest-random-weight bucket for ``key``."""
    if buckets < 1:
        raise ValueError("buckets must be >= 1")
    return max(range(buckets), key=lambda b: score(key, b, 1.0, salt))


def weighted_rendezvous(
    key: object, weights: list[float] | tuple[float, ...], salt: bytes = b""
) -> int:
    """Rendezvous hashing with per-bucket weights (larger weight gets more keys)."""
    if not weights:
        raise ValueError("weights must not be empty")
    if any(w <= 0 for w in weights):
        raise ValueError("all weights must be > 0")
    return max(range(len(weights)), key=lambda b: score(key, b, weights[b], salt))


def audit(keys: list[object], buckets: int, salt: bytes = b"") -> dict[str, object]:
    """Count placements and report balance; ``chi2`` is against a uniform split."""
    if buckets < 1:
        raise ValueError("buckets must be >= 1")
    counts = [0] * buckets
    for key in keys:
        counts[rendezvous(key, buckets, salt)] += 1
    n = len(keys)
    expected = n / buckets
    chi2 = sum((c - expected) ** 2 / expected for c in counts) if n else 0.0
    return {
        "keys": n,
        "buckets": buckets,
        "counts": counts,
        "min": min(counts) if counts else 0,
        "max": max(counts) if counts else 0,
        "mean": expected,
        "max_over_mean": (max(counts) / expected) if expected else 0.0,
        "chi2": chi2,
        "dof": buckets - 1,
    }
