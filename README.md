# shardkit

Deterministic consistent hashing with auditable placement: jump consistent hash (O(log n), O(1) memory) and weighted rendezvous hashing (O(n), arbitrary weights), plus an `audit` command that measures balance and key movement instead of asserting them.

Upstream: none (original library).

## What you get

| function | cost | weights | when to use |
| --- | --- | --- | --- |
| `jump_hash(int_key, buckets)` | **1,129 ns/lookup**, O(log n) | no | fixed-size key space, minimal memory, fastest placement |
| `rendezvous(key, buckets)` | **13,922 ns/lookup**, O(n) | no | arbitrary keys, stateless, easy to reason about |
| `weighted_rendezvous(key, weights)` | O(n) | yes | heterogeneous bucket capacity |

All three are pure functions of `(key, buckets)` — no state, no RNG, no clock. `audit(keys, buckets)` returns counts, `chi2`, and `max/mean` so a distribution claim can be checked rather than believed.

## Install

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e .
pip install pytest   # only needed to run the test suite
```

No runtime dependencies, Python 3.9+.

## Use

```python
from shardkit import jump_hash, rendezvous, weighted_rendezvous, audit

jump_hash(12345, 8)                  # -> bucket in [0, 8)
rendezvous("user:42", 8)             # -> bucket in [0, 8)
weighted_rendezvous("user:42", [1, 1, 4])   # bucket 2 gets ~4x the keys
audit([f"u{i}" for i in range(200_000)], 8)["max_over_mean"]
```

CLI:

```bash
python -m shardkit audit --keys 200000 --buckets 8
```

## Measured properties (200,000 keys, 8 buckets)

From `bench/bench.py`, output in `bench/results.json`, CPython 3.12.3:

| metric | value |
| --- | --- |
| jump hash | **1,129 ns/lookup** |
| rendezvous (BLAKE2b, 8 buckets) | **13,922 ns/lookup** (12.3x jump) |
| bucket counts | min 24,813 / max 25,346 / mean 25,000 |
| max / mean | **1.0138** |
| chi-square (dof 7) | **10.25** |
| keys moved when growing 8 → 9 buckets | **11,055 / 100,000 = 11.05 %** (ideal 11.11 %) |

Timings on this shared 4-vCPU box vary by several percent between runs (`bench/variance.json` records 8 consecutive runs of the same command; the movement fraction is 0.1105 in every one). The table above is one such run.

The last row is the property people usually assume without checking: adding one bucket moves ~1/(n+1) of keys, and 11.05 % is within 0.06 pp of the ideal. The chi-square of 10.25 with 7 degrees of freedom is well inside the 95 % band (14.07), so the placement is not detectably skewed at this sample size.

## Tests

```bash
python -m pytest tests -q
# 11 passed
```

Covered: range/determinism for 4 bucket counts, single-bucket identity, balance bounds, measured 8→9 disruption, rendezvous determinism and salting, key-type normalization, weight proportionality (4x weight within 3.2–4.8x), score monotonicity, audit report shape/balance, invalid inputs, and the CLI in a subprocess.

## Limits

- `jump_hash` takes a 64-bit integer key only; hash strings first if you need them.
- `rendezvous` is O(buckets) per lookup with a BLAKE2b digest per bucket — fine for tens of buckets, not for thousands.
- No bounded-load variant and no replication/failure-domain awareness; this library is placement only.
- Benchmarks are single-process, single-threaded, on the machine below.

## Reproduce

```bash
python -m pytest tests -q
python bench/bench.py
python -m shardkit audit --keys 200000 --buckets 8
```

## Environment

- CPU: AMD EPYC 7K62 (4 vCPU), RAM 3,723 MB
- CPython **3.12.3** (`/usr/bin/python3.12`), pytest 9.2.0
- Seed: none — the algorithms are deterministic; the 200k keys are `user:0 … user:199999`

## 本轮创新

把两种一致哈希的**代价与迁移边界**放在同一 key 空间上实测并做成可复算的 `audit`：jump 1,129 ns vs rendezvous 13,922 ns（12.3x），8→9 桶迁移 11.05% 对理论 11.11%，chi2 10.25/dof 7 未检出偏斜。与台账里“证书/审计”类仓库不同，这里交付的是可复用库本身，结论是测量出来的而不是引用的。

## License

MIT, original work. See `LICENSE`.
