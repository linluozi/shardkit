import json
import math
import subprocess
import sys
from pathlib import Path

import pytest

from shardkit import (
    audit_placement,
    audit_replicas,
    bounded_assign,
    bounded_stream,
    rebalance,
    repair_failure,
    replicated_assign,
    weighted_bounded_assign,
)

ROOT = Path(__file__).resolve().parents[1]


def test_bounded_bound_holds_at_overload_one():
    n, m = 20_000, 7
    report = bounded_assign([f"u{i}" for i in range(n)], m, 1.0)
    assert report["capacity"] == math.ceil(n / m)
    assert report["bound_holds"] is True
    assert report["max_load"] <= report["capacity"]
    assert sum(report["loads"]) == n
    assert len(report["placements"]) == n
    assert set(report["placements"]) <= set(range(m))
    assert report["fallbacks"] == 0


def test_bounded_bound_holds_at_overload_125():
    n, m = 20_000, 7
    report = bounded_assign([f"u{i}" for i in range(n)], m, 1.25)
    assert report["capacity"] == math.ceil(1.25 * n / m)
    assert report["bound_holds"] is True
    assert report["max_load"] <= report["capacity"]
    # The extra slack must actually be usable: strictly higher cap than 1.0.
    assert report["capacity"] > math.ceil(n / m)


def test_bounded_single_bucket_and_empty_batch():
    assert [p for p in bounded_assign([], 3)["placements"]] == []
    empty = bounded_assign([], 3)
    assert empty["capacity"] == 0 and empty["max_load"] == 0 and empty["bound_holds"]
    one = bounded_assign([f"u{i}" for i in range(500)], 1)
    assert set(one["placements"]) == {0}
    assert one["loads"] == [500]


def test_bounded_more_buckets_than_keys_forces_distinct():
    report = bounded_assign(["a", "b", "c"], 64, 1.0)
    assert report["capacity"] == 1
    assert len(set(report["placements"])) == 3
    assert report["max_load"] == 1


def test_bounded_deterministic_and_salted():
    keys = [f"u{i}" for i in range(3_000)]
    first = bounded_assign(keys, 8)["placements"]
    assert first == bounded_assign(keys, 8)["placements"]
    salted = bounded_assign(keys, 8, 1.0, b"salt")["placements"]
    assert first != salted


def test_bounded_probe_histogram_is_consistent():
    n = 5_000
    report = bounded_assign([f"u{i}" for i in range(n)], 8)
    histogram = report["histogram"]
    assert sum(histogram.values()) == n
    assert min(histogram) >= 1
    assert report["probes"] == sum(d * c for d, c in histogram.items())
    assert report["mean_probe"] == pytest.approx(report["probes"] / n)
    # Some keys must be displaced from their first rendezvous choice, otherwise
    # the "bounded" part would be a no-op on this load.
    assert report["mean_probe"] > 1.0


def test_bounded_rejects_bad_arguments():
    with pytest.raises(ValueError):
        bounded_assign(["a"], 0)
    with pytest.raises(ValueError):
        bounded_assign(["a"], 4, 0.99)
    with pytest.raises(ValueError):
        bounded_assign(["a"], 4, float("nan"))
    with pytest.raises(TypeError):
        bounded_assign(["a"], 4, True)
    with pytest.raises(TypeError):
        bounded_assign(["a"], True)


def test_audit_placement_recomputes_and_detects_violation():
    report = bounded_assign([f"u{i}" for i in range(2_000)], 6, 1.0)
    audit = audit_placement(report["placements"], 6, report["capacity"])
    assert audit["loads"] == report["loads"]
    assert audit["bound_holds"] is True
    assert audit["max_over_mean"] == pytest.approx(max(audit["loads"]) / (2_000 / 6))

    bad = audit_placement([0] * 6 + [1], 2, capacity=3)
    assert bad["bound_holds"] is False
    with pytest.raises(ValueError):
        audit_placement([2], 2)


def test_replicated_separation_and_capacity():
    n, m, r = 5_000, 8, 3
    domains = [f"zone-{b % 4}" for b in range(m)]
    report = replicated_assign(
        [f"u{i}" for i in range(n)], m, replicas=r, domains=domains, overload=1.25
    )
    assert report["capacity"] == math.ceil(1.25 * n * r / m)
    assert report["violations"] == 0
    assert report["unplaced"] == 0
    assert report["bound_holds"] is True
    assert all(len(replica_set) == r for replica_set in report["placements"])
    audit = audit_replicas(report["placements"], m, domains)
    assert audit["duplicate_bucket"] == 0
    assert audit["duplicate_domain"] == 0
    assert audit["loads"] == report["loads"]
    assert sum(report["loads"]) == n * r


def test_replicated_violations_are_reported_not_hidden():
    n, m, r = 3_000, 8, 3
    domains = [b % 4 for b in range(m)]
    report = replicated_assign(
        [f"u{i}" for i in range(n)], m, replicas=r, domains=domains, overload=1.0
    )
    # The certificate must agree with the loads it reports, whichever way the
    # tight capacity resolves.
    assert report["bound_holds"] == (report["max_load"] <= report["capacity"])
    assert (report["violations"] > 0) == (report["max_load"] > report["capacity"])


def test_replicated_rejects_impossible_domains():
    with pytest.raises(ValueError):
        replicated_assign(["a"], 4, replicas=3, domains=["x", "x", "y", "y"])
    with pytest.raises(ValueError):
        replicated_assign(["a"], 4, replicas=5)
    with pytest.raises(ValueError):
        replicated_assign(["a"], 4, replicas=2, domains=["x", "y"])
    with pytest.raises(ValueError):
        replicated_assign(["a"], 4, replicas=0)


def test_rebalance_minimal_movement_and_bound():
    n, m = 10_000, 8
    keys = [f"u{i}" for i in range(n)]
    before = bounded_assign(keys, m, 1.0)
    failed = {3}
    after = rebalance(keys, before["placements"], m, failed, 1.0)
    assert after["survivors"] == [0, 1, 2, 4, 5, 6, 7]
    assert after["forced"] == before["loads"][3]
    assert after["moved"] >= after["forced"]
    assert after["extra"] == after["moved"] - after["forced"]
    assert 3 not in set(after["placements"])
    assert after["max_load"] <= after["capacity"]
    assert sum(after["loads"]) == n
    assert after["capacity"] == math.ceil(n / (m - 1))


def test_rebalance_rejects_bad_input():
    with pytest.raises(ValueError):
        rebalance(["a"], [0], 2, [0, 1])
    with pytest.raises(ValueError):
        rebalance(["a"], [0, 1], 2, [0])
    with pytest.raises(ValueError):
        rebalance(["a"], [0], 2, [5])


def test_repair_moves_only_forced_and_keeps_bound():
    n, m = 10_000, 8
    keys = [f"u{i}" for i in range(n)]
    before = bounded_assign(keys, m, 1.0)
    failed = {3}
    after = repair_failure(keys, before["placements"], m, failed, 1.0)
    forced = before["loads"][3]
    assert after["moved"] == forced == after["forced"]
    assert after["capacity"] == math.ceil(n / (m - 1))
    assert after["max_load"] <= after["capacity"]
    assert after["bound_holds"] is True
    assert after["loads"][3] == 0
    assert sum(after["loads"]) == n
    assert 3 not in set(after["placements"])
    # Every key that was not on the failed bucket keeps its exact bucket.
    for index, old in enumerate(before["placements"]):
        if old != 3:
            assert after["placements"][index] == old


def test_repair_survives_multiple_failures():
    n, m = 9_000, 8
    keys = [f"u{i}" for i in range(n)]
    before = bounded_assign(keys, m, 1.0)
    failed = {1, 6}
    after = repair_failure(keys, before["placements"], m, failed, 1.0)
    assert after["moved"] == before["loads"][1] + before["loads"][6]
    assert after["capacity"] == math.ceil(n / 6)
    assert after["bound_holds"] is True
    assert after["loads"][1] == after["loads"][6] == 0


def test_repair_rejects_bad_input():
    with pytest.raises(ValueError):
        repair_failure(["a"], [0], 2, [0, 1])
    with pytest.raises(ValueError):
        repair_failure(["a"], [0, 1], 2, [0])
    with pytest.raises(ValueError):
        repair_failure(["a"], [0], 2, [9])


def test_stream_prefix_bound_and_prefix_consistency():
    n, m = 3_000, 7
    keys = [f"u{i}" for i in range(n)]
    report = bounded_stream(keys, m, 1.0)
    assert report["prefix_bound_holds"] is True
    assert report["bound_holds"] is True
    assert report["moved"] == 0
    assert sum(report["loads"]) == n
    assert report["max_load"] <= report["capacity"]
    k = 1_500
    prefix = bounded_stream(keys[:k], m, 1.0)
    assert report["placements"][:k] == prefix["placements"]


def test_stream_probe_cost_is_measurable():
    keys = [f"u{i}" for i in range(5_000)]
    tight = bounded_stream(keys, 8, 1.0)
    relaxed = bounded_stream(keys, 8, 1.05)
    # Deterministic probe counts, not timings: saturated ring costs > 1.5
    # hashes per key, 5 % headroom brings it back to ~1.0.
    assert tight["mean_probe"] > 1.5
    assert relaxed["mean_probe"] < 1.1
    assert tight["max_load"] <= tight["capacity"]
    assert relaxed["max_load"] <= relaxed["capacity"]
    assert tight["prefix_bound_holds"] and relaxed["prefix_bound_holds"]


def test_stream_empty_and_invalid():
    empty = bounded_stream([], 4)
    assert empty["placements"] == []
    assert empty["max_load"] == 0
    assert empty["prefix_bound_holds"] is True
    with pytest.raises(ValueError):
        bounded_stream(["a"], 4, 0.99)
    with pytest.raises(ValueError):
        bounded_stream(["a"], 0)


def test_weighted_bounded_capacities_and_ratio():
    n = 12_000
    report = weighted_bounded_assign([f"u{i}" for i in range(n)], [1, 1, 4], 1.0)
    assert report["capacities"] == [2_000, 2_000, 8_000]
    assert report["bound_holds"] is True
    assert report["loads"] == [2_000, 2_000, 8_000]
    assert 3.5 < report["loads"][2] / report["loads"][0] < 4.5
    assert report["fallbacks"] == 0


def test_weighted_bounded_nondivisible_and_empty():
    report = weighted_bounded_assign(["a"] * 10, [1, 2, 3], 1.0)
    assert report["capacities"] == [2, 4, 5]
    assert report["bound_holds"] is True
    assert sum(report["loads"]) == 10
    empty = weighted_bounded_assign([], [1, 1, 4])
    assert empty["placements"] == [] and empty["bound_holds"] is True


def test_weighted_bounded_invalid():
    for weights in ([], [1, 0], [1, -1], [1, float("nan")]):
        with pytest.raises(ValueError):
            weighted_bounded_assign(["a"], weights)
    with pytest.raises(TypeError):
        weighted_bounded_assign(["a"], [1, "x"])
    with pytest.raises(ValueError):
        weighted_bounded_assign(["a"], [1, 1], 0.99)


def test_weighted_bounded_deterministic_and_salted():
    keys = [f"u{i}" for i in range(2_000)]
    first = weighted_bounded_assign(keys, [1, 2, 3])["placements"]
    assert first == weighted_bounded_assign(keys, [1, 2, 3])["placements"]
    assert first != weighted_bounded_assign(keys, [1, 2, 3], 1.0, b"salt")["placements"]


def test_cli_weighted_smoke():
    proc = subprocess.run(
        [sys.executable, "-m", "shardkit", "weighted", "--keys", "12000", "--weights", "1,1,4"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["capacities"] == [2000, 2000, 8000]
    assert payload["bound_holds"] is True


def test_cli_bounded_smoke():
    proc = subprocess.run(
        [sys.executable, "-m", "shardkit", "bounded", "--keys", "20000", "--buckets", "7"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["keys"] == 20000
    assert payload["bound_holds"] is True
    assert payload["capacity"] == math.ceil(20000 / 7)
    assert "placements" not in payload


def test_cli_replicas_smoke():
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "shardkit",
            "replicas",
            "--keys",
            "5000",
            "--buckets",
            "8",
            "--replicas",
            "3",
            "--domains",
            "4",
            "--overload",
            "1.25",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["replicas"] == 3
    assert payload["violations"] == 0
    assert payload["unplaced"] == 0
    assert payload["bound_holds"] is True
