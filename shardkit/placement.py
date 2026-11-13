"""Bounded-load and replica-aware placement with machine-checkable certificates.

Unlike the stateless primitives in :mod:`shardkit.core`, placement here depends
on the batch of keys: the capacity of every bucket is computed once from the
batch size and then enforced while assigning.

For ``n`` keys, ``m`` buckets and ``overload >= 1.0`` the capacity is::

    capacity = ceil(overload * n / m)

A key walks its rendezvous preference order and takes the first bucket whose
load is below capacity.  If every bucket is full the key falls back to the
least-loaded bucket in its preference order.  That fallback cannot break the
cap: before the last assignment at most ``n - 1`` slots are taken, so the
least-loaded bucket has load ``<= floor((n - 1) / m) < ceil(n / m)``.  The
result is therefore not a statistical claim but an arithmetic one, and
:func:`audit_placement` recomputes it from the placements alone.
"""

from __future__ import annotations

import math

from .core import score

__all__ = [
    "bounded_assign",
    "bounded_stream",
    "replicated_assign",
    "rebalance",
    "repair_failure",
    "audit_placement",
    "audit_replicas",
]


def _check_buckets(buckets: int) -> None:
    if not isinstance(buckets, int) or isinstance(buckets, bool):
        raise TypeError("buckets must be an int")
    if buckets < 1:
        raise ValueError("buckets must be >= 1")


def _check_overload(overload: float) -> None:
    if not isinstance(overload, (int, float)) or isinstance(overload, bool):
        raise TypeError("overload must be a number")
    if not math.isfinite(overload) or overload < 1.0:
        raise ValueError("overload must be a finite number >= 1.0")


def _capacity(n: int, buckets: int, overload: float) -> int:
    return math.ceil(overload * n / buckets)


def _preference(key: object, buckets: int, salt: bytes) -> list[int]:
    return sorted(
        range(buckets), key=lambda b: score(key, b, 1.0, salt), reverse=True
    )


def bounded_assign(
    keys: list[object],
    buckets: int,
    overload: float = 1.0,
    salt: bytes = b"",
) -> dict[str, object]:
    """Assign every key to a bucket while keeping every load <= capacity.

    Returns a dict with the aligned ``placements`` list, the resulting
    ``loads``, the ``capacity`` used, ``max_load``, ``bound_holds`` and the
    probe-depth ``histogram`` (1 means the key's first rendezvous choice had
    room).  ``fallbacks`` counts keys that had to use the least-loaded
    fallback; with the batch capacity above it is always 0.
    """
    _check_buckets(buckets)
    _check_overload(overload)
    keys = list(keys)
    n = len(keys)
    capacity = _capacity(n, buckets, overload)
    loads = [0] * buckets
    placements: list[int] = []
    histogram: dict[int, int] = {}
    probe_total = 0
    fallbacks = 0

    for key in keys:
        order = _preference(key, buckets, salt)
        chosen = -1
        for depth, bucket in enumerate(order, start=1):
            if loads[bucket] < capacity:
                chosen = bucket
                histogram[depth] = histogram.get(depth, 0) + 1
                probe_total += depth
                break
        if chosen < 0:  # pragma: no cover - unreachable by the pigeonhole bound
            fallbacks += 1
            chosen = min(order, key=lambda b: loads[b])
            histogram[buckets] = histogram.get(buckets, 0) + 1
            probe_total += buckets
        loads[chosen] += 1
        placements.append(chosen)

    return {
        "keys": n,
        "buckets": buckets,
        "overload": overload,
        "capacity": capacity,
        "placements": placements,
        "loads": loads,
        "max_load": max(loads) if loads else 0,
        "bound_holds": all(load <= capacity for load in loads),
        "fallbacks": fallbacks,
        "probes": probe_total,
        "mean_probe": (probe_total / n) if n else 0.0,
        "histogram": {int(k): v for k, v in sorted(histogram.items())},
    }


def bounded_stream(
    keys: list[object],
    buckets: int,
    overload: float = 1.0,
    salt: bytes = b"",
) -> dict[str, object]:
    """Online variant: the cap holds for **every prefix**, and no key ever moves.

    Capacity is recomputed as ``ceil(overload * k / m)`` before the k-th
    insertion.  It is non-decreasing in ``k``, so a slot always exists and the
    least-loaded fallback (if ever needed) cannot exceed the current cap.  The
    price is probe depth: at ``overload=1.0`` the ring runs saturated, so many
    keys find their first rendezvous choice full.  The returned report includes
    ``mean_probe`` and ``prefix_bound_holds`` so that price is measured, not
    assumed.
    """
    _check_buckets(buckets)
    _check_overload(overload)
    keys = list(keys)
    loads = [0] * buckets
    placements: list[int] = []
    histogram: dict[int, int] = {}
    probe_total = 0
    fallbacks = 0
    prefix_holds = True

    for index, key in enumerate(keys, start=1):
        capacity = _capacity(index, buckets, overload)
        order = _preference(key, buckets, salt)
        chosen = -1
        for depth, bucket in enumerate(order, start=1):
            if loads[bucket] < capacity:
                chosen = bucket
                histogram[depth] = histogram.get(depth, 0) + 1
                probe_total += depth
                break
        if chosen < 0:  # pragma: no cover - monotone cap guarantees a slot
            fallbacks += 1
            chosen = min(order, key=lambda b: loads[b])
            probe_total += buckets
        loads[chosen] += 1
        if max(loads) > capacity:
            prefix_holds = False
        placements.append(chosen)

    n = len(keys)
    capacity = _capacity(n, buckets, overload)
    return {
        "keys": n,
        "buckets": buckets,
        "overload": overload,
        "capacity": capacity,
        "placements": placements,
        "loads": loads,
        "max_load": max(loads) if loads else 0,
        "bound_holds": all(load <= capacity for load in loads),
        "prefix_bound_holds": prefix_holds,
        "moved": 0,
        "fallbacks": fallbacks,
        "probes": probe_total,
        "mean_probe": (probe_total / n) if n else 0.0,
        "histogram": {int(k): v for k, v in sorted(histogram.items())},
    }


def replicated_assign(
    keys: list[object],
    buckets: int,
    replicas: int = 3,
    domains: list[object] | dict[int, object] | None = None,
    overload: float = 1.0,
    salt: bytes = b"",
) -> dict[str, object]:
    """Place ``replicas`` copies of every key on distinct failure domains.

    ``domains`` maps bucket index -> domain label.  When omitted every bucket
    is its own domain (so the only constraint is that replicas land on
    distinct buckets).  Keys walk their rendezvous order and take the first
    eligible bucket below capacity, so the same capacity proof applies; the
    domain constraint can consume the slack, though, and then a replica is
    placed above capacity.  Such placements are counted in ``violations``
    rather than hidden: with tight capacity the caller should raise
    ``overload`` until the count is 0.
    """
    _check_buckets(buckets)
    _check_overload(overload)
    if not isinstance(replicas, int) or isinstance(replicas, bool):
        raise TypeError("replicas must be an int")
    if replicas < 1 or replicas > buckets:
        raise ValueError("replicas must satisfy 1 <= replicas <= buckets")

    if domains is None:
        domain_of = list(range(buckets))
    elif isinstance(domains, dict):
        missing = [b for b in range(buckets) if b not in domains]
        if missing:
            raise ValueError(f"domains missing bucket indices: {missing}")
        domain_of = [domains[b] for b in range(buckets)]
    else:
        domain_of = list(domains)
        if len(domain_of) != buckets:
            raise ValueError("domains must have one label per bucket")
    if replicas > len(set(domain_of)):
        raise ValueError(
            f"cannot place {replicas} replicas on {len(set(domain_of))} domains"
        )

    keys = list(keys)
    n = len(keys)
    capacity = _capacity(n * replicas, buckets, overload)
    loads = [0] * buckets
    placements: list[list[int]] = []
    violations = 0
    unplaced = 0
    histogram: dict[int, int] = {}
    probe_total = 0

    for key in keys:
        order = _preference(key, buckets, salt)
        chosen: list[int] = []
        used: set[int] = set()
        used_domains: set[object] = set()
        depth = 0
        for bucket in order:
            depth += 1
            if bucket in used or domain_of[bucket] in used_domains:
                continue
            if loads[bucket] < capacity:
                chosen.append(bucket)
                used.add(bucket)
                used_domains.add(domain_of[bucket])
                if len(chosen) == replicas:
                    break
        probe_total += depth
        histogram[depth] = histogram.get(depth, 0) + 1

        if len(chosen) < replicas:
            candidates = [
                b
                for b in order
                if b not in used and domain_of[b] not in used_domains
            ]
            candidates.sort(key=lambda b: loads[b])
            for bucket in candidates[: replicas - len(chosen)]:
                chosen.append(bucket)
                used.add(bucket)
                used_domains.add(domain_of[bucket])

        if len(chosen) < replicas:
            unplaced += 1
        for bucket in chosen:
            if loads[bucket] >= capacity:
                violations += 1
            loads[bucket] += 1
        placements.append(chosen)

    return {
        "keys": n,
        "buckets": buckets,
        "replicas": replicas,
        "overload": overload,
        "capacity": capacity,
        "placements": placements,
        "loads": loads,
        "max_load": max(loads) if loads else 0,
        "bound_holds": all(load <= capacity for load in loads),
        "violations": violations,
        "unplaced": unplaced,
        "domains": domain_of,
        "mean_probe": (probe_total / n) if n else 0.0,
        "histogram": {int(k): v for k, v in sorted(histogram.items())},
    }


def rebalance(
    keys: list[object],
    placements: list[int],
    buckets: int,
    failed: set[int] | list[int],
    overload: float = 1.0,
    salt: bytes = b"",
) -> dict[str, object]:
    """Re-place every key on the surviving buckets and report what moved.

    ``forced`` is the number of keys that *had* to move (their bucket failed);
    ``moved`` can be larger because the capacity is recomputed for the smaller
    ring.  Both numbers are returned instead of only the flattering one.
    """
    _check_buckets(buckets)
    _check_overload(overload)
    failed_set = set(failed)
    if any(not isinstance(b, int) or b < 0 or b >= buckets for b in failed_set):
        raise ValueError("failed bucket index out of range")
    survivors = [b for b in range(buckets) if b not in failed_set]
    if not survivors:
        raise ValueError("cannot rebalance with every bucket failed")
    keys = list(keys)
    if len(keys) != len(placements):
        raise ValueError("keys and placements must be aligned")

    sub = bounded_assign(keys, len(survivors), overload, salt)
    new_placements = [survivors[b] for b in sub["placements"]]
    moved = sum(old != new for old, new in zip(placements, new_placements))
    forced = sum(old in failed_set for old in placements)
    return {
        "placements": new_placements,
        "keys": len(keys),
        "survivors": survivors,
        "capacity": sub["capacity"],
        "loads": sub["loads"],
        "max_load": sub["max_load"],
        "bound_holds": sub["bound_holds"],
        "moved": moved,
        "moved_fraction": (moved / len(keys)) if keys else 0.0,
        "forced": forced,
        "forced_fraction": (forced / len(keys)) if keys else 0.0,
        "extra": moved - forced,
    }


def repair_failure(
    keys: list[object],
    placements: list[int],
    buckets: int,
    failed: set[int] | list[int],
    overload: float = 1.0,
    salt: bytes = b"",
) -> dict[str, object]:
    """Move only the keys that lived on failed buckets, and nothing else.

    Survivors keep their key exactly where it was.  The new capacity is
    ``ceil(overload * n / |survivors|)``, which is at least the old load of
    every survivor, and the spare room is enough for every displaced key, so
    the bound still holds while ``moved == forced`` (the lower bound).  This is
    the cheap alternative to :func:`rebalance`, which re-places the whole batch
    and moves far more keys than necessary.
    """
    _check_buckets(buckets)
    _check_overload(overload)
    failed_set = set(failed)
    if any(not isinstance(b, int) or b < 0 or b >= buckets for b in failed_set):
        raise ValueError("failed bucket index out of range")
    survivors = [b for b in range(buckets) if b not in failed_set]
    if not survivors:
        raise ValueError("cannot repair with every bucket failed")
    keys = list(keys)
    if len(keys) != len(placements):
        raise ValueError("keys and placements must be aligned")

    cap = _capacity(len(keys), len(survivors), overload)
    loads = [0] * buckets
    for bucket in placements:
        loads[bucket] += 1
    new_placements = list(placements)
    moved = 0
    probe_total = 0
    fallbacks = 0
    histogram: dict[int, int] = {}

    for index, (key, old_bucket) in enumerate(zip(keys, placements)):
        if old_bucket not in failed_set:
            continue
        order = _preference(key, len(survivors), salt)
        chosen = -1
        for depth, sub_bucket in enumerate(order, start=1):
            bucket = survivors[sub_bucket]
            if loads[bucket] < cap:
                chosen = bucket
                histogram[depth] = histogram.get(depth, 0) + 1
                probe_total += depth
                break
        if chosen < 0:  # pragma: no cover - spare capacity guarantees a slot
            fallbacks += 1
            chosen = survivors[min(order, key=lambda j: loads[survivors[j]])]
            probe_total += len(survivors)
        loads[old_bucket] -= 1
        loads[chosen] += 1
        new_placements[index] = chosen
        moved += 1

    return {
        "placements": new_placements,
        "keys": len(keys),
        "survivors": survivors,
        "capacity": cap,
        "loads": loads,
        "max_load": max(loads) if loads else 0,
        "bound_holds": all(loads[b] <= cap for b in survivors),
        "moved": moved,
        "forced": moved,
        "moved_fraction": (moved / len(keys)) if keys else 0.0,
        "mean_probe": (probe_total / moved) if moved else 0.0,
        "fallbacks": fallbacks,
        "histogram": {int(k): v for k, v in sorted(histogram.items())},
    }


def audit_placement(
    placements: list[int], buckets: int, capacity: int | None = None
) -> dict[str, object]:
    """Recompute loads and the bound from an assignment, independent of how it was made."""
    _check_buckets(buckets)
    loads = [0] * buckets
    for bucket in placements:
        if not isinstance(bucket, int) or bucket < 0 or bucket >= buckets:
            raise ValueError(f"bucket out of range: {bucket!r}")
        loads[bucket] += 1
    n = len(placements)
    maximum = max(loads) if loads else 0
    result = {
        "keys": n,
        "buckets": buckets,
        "loads": loads,
        "min": min(loads) if loads else 0,
        "max": maximum,
        "mean": (n / buckets) if buckets else 0.0,
        "max_over_mean": (maximum / (n / buckets)) if n else 0.0,
        "capacity": capacity,
    }
    if capacity is not None:
        result["bound_holds"] = all(load <= capacity for load in loads)
    return result


def audit_replicas(
    placements: list[list[int]],
    buckets: int,
    domains: list[object] | dict[int, object] | None = None,
) -> dict[str, object]:
    """Check distinct-bucket and distinct-domain separation for each key."""
    _check_buckets(buckets)
    if domains is None:
        domain_of = list(range(buckets))
    elif isinstance(domains, dict):
        domain_of = [domains[b] for b in range(buckets)]
    else:
        domain_of = list(domains)
        if len(domain_of) != buckets:
            raise ValueError("domains must have one label per bucket")

    loads = [0] * buckets
    duplicate_bucket = 0
    duplicate_domain = 0
    for replica_set in placements:
        if len(set(replica_set)) != len(replica_set):
            duplicate_bucket += 1
        labels = [domain_of[b] for b in replica_set]
        if len(set(labels)) != len(labels):
            duplicate_domain += 1
        for bucket in replica_set:
            loads[bucket] += 1
    return {
        "keys": len(placements),
        "replicas": len(placements[0]) if placements else 0,
        "loads": loads,
        "max_load": max(loads) if loads else 0,
        "duplicate_bucket": duplicate_bucket,
        "duplicate_domain": duplicate_domain,
    }
