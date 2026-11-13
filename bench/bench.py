"""Placement cost, balance, and disruption on 200k synthetic keys."""
import json
import statistics
import time

from shardkit import audit, jump_hash, rendezvous

KEYS = [f"user:{i}" for i in range(200_000)]
INTS = list(range(200_000))


def timed(fn, keys, repeats=5):
    samples = []
    for _ in range(repeats):
        started = time.perf_counter()
        for key in keys:
            fn(key)
        samples.append(time.perf_counter() - started)
    total = statistics.median(samples)
    return {"seconds": round(total, 4), "ns_per_lookup": round(total / len(keys) * 1e9, 1)}


def main():
    jump = timed(lambda k: jump_hash(k, 8), INTS)
    rend = timed(lambda k: rendezvous(k, 8), KEYS)
    report = audit(KEYS, 8)
    moved = sum(jump_hash(i, 8) != jump_hash(i, 9) for i in range(100_000))
    print(json.dumps({
        "keys": len(KEYS),
        "buckets": 8,
        "jump_hash": jump,
        "rendezvous_blake2b": rend,
        "balance": {k: report[k] for k in ("min", "max", "mean", "max_over_mean", "chi2", "dof")},
        "growth_8_to_9": {
            "keys": 100_000,
            "moved": moved,
            "moved_fraction": round(moved / 100_000, 4),
            "ideal": round(1 / 9, 4),
        },
    }, indent=2))


if __name__ == "__main__":
    main()
