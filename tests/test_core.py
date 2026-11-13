import json
import subprocess
import sys
from pathlib import Path

import pytest

from shardkit import audit, jump_hash, rendezvous, score, weighted_rendezvous

ROOT = Path(__file__).resolve().parents[1]


def test_jump_range_and_determinism():
    for buckets in (1, 2, 7, 64):
        values = [jump_hash(i, buckets) for i in range(2000)]
        assert set(values) <= set(range(buckets))
        assert values == [jump_hash(i, buckets) for i in range(2000)]


def test_jump_single_bucket_is_zero():
    assert all(jump_hash(i, 1) == 0 for i in range(500))


def test_jump_is_balanced():
    n, b = 200_000, 8
    counts = [0] * b
    for i in range(n):
        counts[jump_hash(i, b)] += 1
    mean = n / b
    assert min(counts) > mean * 0.88
    assert max(counts) < mean * 1.12


def test_jump_minimal_disruption_on_growth():
    n, b = 100_000, 8
    moved = sum(jump_hash(i, b) != jump_hash(i, b + 1) for i in range(n))
    # Lamping-Veach moves 1/(b+1) of keys on average when adding one bucket.
    assert 0.07 < moved / n < 0.15


def test_rendezvous_deterministic_and_salted():
    keys = [f"k{i}" for i in range(500)]
    first = [rendezvous(k, 8) for k in keys]
    assert first == [rendezvous(k, 8) for k in keys]
    assert first != [rendezvous(k, 8, b"salt") for k in keys]


def test_rendezvous_key_types():
    assert rendezvous("42", 5) == rendezvous(b"42", 5)
    assert 0 <= rendezvous(42, 5) < 5


def test_weighted_rendezvous_follows_weights():
    weights = [1.0, 1.0, 4.0]
    counts = [0, 0, 0]
    for i in range(20_000):
        counts[weighted_rendezvous(f"u{i}", weights)] += 1
    assert 3.2 < counts[2] / ((counts[0] + counts[1]) / 2) < 4.8


def test_score_monotone_in_weight():
    key, bucket = "abc", 3
    assert score(key, bucket, 4.0) > score(key, bucket, 1.0)


def test_audit_report_shape_and_balance():
    report = audit([f"u{i}" for i in range(50_000)], 8)
    assert report["keys"] == 50_000
    assert len(report["counts"]) == 8
    assert sum(report["counts"]) == 50_000
    assert report["dof"] == 7
    assert report["chi2"] < 50.0
    assert report["max_over_mean"] < 1.15


def test_invalid_inputs():
    with pytest.raises(ValueError):
        jump_hash(1, 0)
    with pytest.raises(ValueError):
        rendezvous("x", 0)
    with pytest.raises(ValueError):
        weighted_rendezvous("x", [])
    with pytest.raises(ValueError):
        weighted_rendezvous("x", [1.0, 0.0])
    with pytest.raises(TypeError):
        rendezvous(1.5, 4)


def test_cli_audit_smoke():
    proc = subprocess.run(
        [sys.executable, "-m", "shardkit", "audit", "--keys", "20000", "--buckets", "4"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["keys"] == 20000
    assert payload["buckets"] == 4
    assert payload["jump_range_ok"] is True
