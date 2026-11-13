"""Bounded-load / replica placement: cost, certificate, skew and failure.

Run: python bench/bounded.py > bench/bounded.json
Keys are deterministic (``user:0 .. user:n-1``); no RNG is involved, so the
assignment is reproducible bit for bit.
"""

from __future__ import annotations

import hashlib
import json
import platform
import statistics
import time

from shardkit import (
    audit_placement,
    audit_replicas,
    bounded_assign,
    bounded_stream,
    jump_hash,
    rendezvous,
    rebalance,
    repair_failure,
    replicated_assign,
)

N = 100_000
BUCKETS = 8
REPEATS = 3
KEYS = [f"user:{i}" for i in range(N)]


def timed(fn, repeats: int = REPEATS) -> tuple[list[float], dict]:
    samples: list[float] = []
    report: dict = {}
    for _ in range(repeats):
        started = time.perf_counter()
        report = fn()
        samples.append(time.perf_counter() - started)
    return samples, report


def summary(samples: list[float], n: int) -> dict:
    median = statistics.median(samples)
    return {
        "seconds": [round(s, 4) for s in samples],
        "median_seconds": round(median, 4),
        "ns_per_key": round(median / n * 1e9, 1),
        "min": round(min(samples), 4),
        "max": round(max(samples), 4),
    }


def main() -> None:
    out: dict = {
        "python": platform.python_version(),
        "machine": platform.machine(),
        "keys": N,
        "buckets": BUCKETS,
        "repeats": REPEATS,
        "seed": "none (keys are user:0 .. user:%d)" % (N - 1),
    }

    # --- bounded load vs plain rendezvous on the uniform key space ---------
    samples, report = timed(lambda: bounded_assign(KEYS, BUCKETS, 1.0))
    plain_counts = [0] * BUCKETS
    for key in KEYS:
        plain_counts[rendezvous(key, BUCKETS)] += 1
    out["bounded_c100"] = {
        **summary(samples, N),
        "capacity": report["capacity"],
        "max_load": report["max_load"],
        "bound_holds": report["bound_holds"],
        "all_buckets_exactly_full": report["loads"] == [report["capacity"]] * BUCKETS,
        "loads": report["loads"],
        "mean_probe": round(report["mean_probe"], 3),
        "histogram": report["histogram"],
        "fallbacks": report["fallbacks"],
        "plain_rendezvous_max_over_mean": round(
            max(plain_counts) / (N / BUCKETS), 4
        ),
    }

    samples, report = timed(lambda: bounded_assign(KEYS, BUCKETS, 1.25))
    out["bounded_c125"] = {
        **summary(samples, N),
        "capacity": report["capacity"],
        "max_load": report["max_load"],
        "bound_holds": report["bound_holds"],
        "loads": report["loads"],
        "mean_probe": round(report["mean_probe"], 3),
    }

    for label, overload in (("c100", 1.0), ("c105", 1.05)):
        samples, report = timed(
            lambda overload=overload: bounded_stream(KEYS, BUCKETS, overload)
        )
        out[f"stream_{label}"] = {
            **summary(samples, N),
            "capacity": report["capacity"],
            "max_load": report["max_load"],
            "bound_holds": report["bound_holds"],
            "prefix_bound_holds": report["prefix_bound_holds"],
            "all_buckets_exactly_full": report["loads"] == [report["capacity"]] * BUCKETS,
            "mean_probe": round(report["mean_probe"], 3),
            "histogram": report["histogram"],
        }

    # --- cost floor: the plain primitives on the same key space ------------
    samples, _ = timed(lambda: [jump_hash(i, BUCKETS) for i in range(N)])
    out["jump_hash"] = summary(samples, N)
    samples, _ = timed(lambda: [rendezvous(k, BUCKETS) for k in KEYS])
    out["rendezvous"] = summary(samples, N)

    # --- hot-key skew: the cap is distribution-independent ------------------
    hot = [f"hot:{i % 200}" for i in range(N)]
    _, skew_plain = timed(lambda: [rendezvous(k, BUCKETS) for k in hot], repeats=1)
    counts = [0] * BUCKETS
    for bucket in skew_plain:
        counts[bucket] += 1
    skew_bounded = bounded_assign(hot, BUCKETS, 1.0)
    out["skew_200_hot_keys"] = {
        "plain_max_over_mean": round(max(counts) / (N / BUCKETS), 2),
        "bounded_max_load": skew_bounded["max_load"],
        "bounded_capacity": skew_bounded["capacity"],
        "bounded_bound_holds": skew_bounded["bound_holds"],
        "bounded_mean_probe": round(skew_bounded["mean_probe"], 2),
        "plain_loads": counts,
    }

    # --- rack-aware replicas: how much overload does the domain rule need? --
    domains = [f"zone-{b % 4}" for b in range(BUCKETS)]
    replication: dict = {"replicas": 3, "domains": 4, "overloads": {}}
    placement_hashes: dict = {}
    for overload in (1.0, 1.1, 1.25):
        samples, rep = timed(
            lambda overload=overload: replicated_assign(
                KEYS, BUCKETS, 3, domains, overload
            ),
            repeats=2,
        )
        audit = audit_replicas(rep["placements"], BUCKETS, domains)
        placement_hashes[str(overload)] = hashlib.sha256(
            json.dumps(rep["placements"], separators=(",", ":")).encode()
        ).hexdigest()
        replication["overloads"][str(overload)] = {
            "median_seconds": round(statistics.median(samples), 4),
            "ns_per_key": round(statistics.median(samples) / N * 1e9, 1),
            "capacity": rep["capacity"],
            "max_load": rep["max_load"],
            "bound_holds": rep["bound_holds"],
            "violations": rep["violations"],
            "unplaced": rep["unplaced"],
            "duplicate_bucket": audit["duplicate_bucket"],
            "duplicate_domain": audit["duplicate_domain"],
        }
    replication["placement_sha256"] = placement_hashes
    replication["identical_overload_pairs"] = [
        [a, b]
        for i, a in enumerate(placement_hashes)
        for b in list(placement_hashes)[i + 1 :]
        if placement_hashes[a] == placement_hashes[b]
    ]
    out["replication"] = replication

    # --- one bucket dies: full rebalance vs targeted repair -----------------
    before = bounded_assign(KEYS, BUCKETS, 1.0)
    audit_before = audit_placement(before["placements"], BUCKETS, before["capacity"])
    samples_naive, after_naive = timed(
        lambda: rebalance(KEYS, before["placements"], BUCKETS, {3}, 1.0), repeats=2
    )
    samples_repair, after_repair = timed(
        lambda: repair_failure(KEYS, before["placements"], BUCKETS, {3}, 1.0),
        repeats=2,
    )
    out["failure_of_bucket_3"] = {
        "keys": N,
        "failed_load": before["loads"][3],
        "forced": after_naive["forced"],
        "forced_fraction": round(after_naive["forced_fraction"], 5),
        "loads_before": audit_before["loads"],
        "naive_rebalance": {
            "median_seconds": round(statistics.median(samples_naive), 4),
            "moved": after_naive["moved"],
            "moved_fraction": round(after_naive["moved_fraction"], 5),
            "extra": after_naive["extra"],
            "capacity_after": after_naive["capacity"],
            "max_load_after": after_naive["max_load"],
            "bound_holds_after": after_naive["bound_holds"],
        },
        "targeted_repair": {
            "median_seconds": round(statistics.median(samples_repair), 4),
            "moved": after_repair["moved"],
            "moved_fraction": round(after_repair["moved_fraction"], 5),
            "extra": after_repair["moved"] - after_repair["forced"],
            "capacity_after": after_repair["capacity"],
            "max_load_after": after_repair["max_load"],
            "bound_holds_after": after_repair["bound_holds"],
            "mean_probe": round(after_repair["mean_probe"], 3),
            "fallbacks": after_repair["fallbacks"],
            "loads_after": after_repair["loads"],
        },
    }

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
