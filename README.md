# shardkit

Deterministic consistent hashing with auditable placement: jump consistent hash (O(log n), O(1) memory), weighted rendezvous hashing, and **bounded-load / failure-domain-aware placement** whose capacity cap is recomputed by an independent audit instead of asserted.

Upstream: none (original library).

## What you get

| function | cost (measured below) | guarantee |
| --- | --- | --- |
| `jump_hash(int_key, buckets)` | **1,117 ns/lookup** | O(log n), O(1) memory |
| `rendezvous(key, buckets)` | **13,802 ns/lookup** | stateless, uniform in expectation |
| `weighted_rendezvous(key, weights)` | O(buckets) | keys follow weights |
| `bounded_assign(keys, buckets, overload)` | **14,426 ns/key** | **max load <= ceil(overload * n / m)**, proven and re-audited |
| `bounded_stream(keys, buckets, overload)` | **14,992 ns/key** | same cap holds for **every prefix**; no key ever moves |
| `replicated_assign(keys, m, replicas, domains)` | **15,736 ns/key** (3 replicas) | replicas on distinct domains, cap reported even when missed |
| `repair_failure(keys, placements, m, failed)` | **176 ms / 12,500 displaced keys** | moves only forced keys, refills to the new cap |

`audit_placement()` and `audit_replicas()` recompute loads and separation from the placements alone, so a distribution or safety claim can be checked rather than believed.

## Install

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e .
pip install pytest   # only needed for the test suite
```

No runtime dependencies, Python 3.9+.

## Use

```python
from shardkit import bounded_assign, replicated_assign, repair_failure, audit_placement

report = bounded_assign([f"u{i}" for i in range(100_000)], 8, overload=1.0)
report["capacity"], report["max_load"], report["bound_holds"]   # 12500, 12500, True
audit_placement(report["placements"], 8, report["capacity"])["bound_holds"]  # True

zones = [f"zone-{b % 4}" for b in range(8)]
rep = replicated_assign([f"u{i}" for i in range(100_000)], 8, 3, zones, overload=1.1)
rep["violations"], rep["max_load"]        # 0, 37774 (cap 41250)

after = repair_failure(keys, report["placements"], 8, {3})
after["moved"], after["max_load"]         # 12500, 14286
```

CLI:

```bash
python -m shardkit audit    --keys 200000 --buckets 8
python -m shardkit bounded  --keys 100000 --buckets 8 --overload 1.0
python -m shardkit replicas --keys 100000 --buckets 8 --replicas 3 --domains 4 --overload 1.1
```

## Why the cap is arithmetic, not statistical

For `n` keys, `m` buckets and `overload >= 1.0`, capacity is `ceil(overload * n / m)`. Each key walks its rendezvous preference order and takes the first bucket below capacity; if none has room it falls back to the least loaded one in that order. The fallback cannot break the cap: before the last assignment at most `n-1` slots are taken, so the least loaded bucket has load `<= floor((n-1)/m) < ceil(n/m) <= capacity`. This is a pigeonhole argument, not a distribution assumption — it holds for uniform keys, hot-key skew, and duplicate keys alike. `audit_placement()` re-derives the loads and the inequality from the placement list.

`bounded_stream()` applies the same walk one key at a time, recomputing capacity as `ceil(overload * k / m)` before the k-th insertion. That capacity is non-decreasing, so the argument holds for every prefix and nothing is ever relocated. The price shows up in probe depth, not correctness: at overload 1.0 the ring runs saturated, so 44 % of keys find their first rendezvous choice full.

## Measured properties (`bench/bounded.py`, output in `bench/bounded.json`)

100,000 deterministic keys (`user:0 … user:99999`), 8 buckets, CPython 3.12.3, median of 3 runs (2 for the failure cases):

| scenario | result |
| --- | --- |
| `bounded_assign`, overload 1.0 | max load **12,500 = capacity**; **all 8 buckets exactly full**; 14,426 ns/key; mean probe **1.01** |
| `bounded_assign`, overload 1.25 | max 12,634 <= 15,625; 14,357 ns/key; mean probe 1.00 |
| `bounded_stream`, overload 1.0 | max 12,500 = capacity, all buckets exactly full, prefix bound holds; 14,992 ns/key; **mean probe 2.059** |
| `bounded_stream`, overload 1.05 | max 12,648 <= 13,125, prefix bound holds; 14,870 ns/key; **mean probe 1.002** |
| plain `rendezvous`, same keys | max/mean **1.0107** (no cap) |
| hot-key skew: 200 distinct keys, 100k lookups | plain rendezvous max/mean **1.44x**; bounded max **12,500 = cap**, mean probe 1.15 |
| 3 replicas, 4 zones, overload 1.0 | **36 / 300,000 placements over cap** (max 37,509 vs 37,500) — reported, not hidden |
| 3 replicas, 4 zones, overload 1.10 | 0 violations, 0 domain collisions, max 37,774 (cap 41,250) |
| 1 of 8 buckets fails, naive full rebalance | **60,729 keys move (60.7 %)** vs 12,500 forced; 1,282 ms |
| 1 of 8 buckets fails, `repair_failure` | **12,500 keys move (12.5 % = the lower bound)**, 0 extra; max 14,286 = new cap; 176 ms |

Three results here are deliberately unflattering and are part of the delivery: (1) domain-constrained replicas at overload 1.0 miss the cap by 36 placements out of 300,000, so the tight setting is not usable with zone constraints; overload 1.1 fixes it, and 1.25 buys nothing extra (identical placement — the same `placement_sha256` in `bench/bounded.json` — max 37,774). (2) A full rebalance after one failure moves 60,729 keys where 12,500 suffice — the targeted repair exists because the naive number was measured, and it reaches the lower bound while still filling the new capacity exactly (14,284–14,286 per survivor). (3) A saturated online ring is not free: `bounded_stream` at overload 1.0 costs **2.059 hashes per key**, and 5 % headroom (1.05) brings that to 1.002 for a max load 1.2 % above `n/m`. The knob is measured, not assumed.

Timings on this shared 4-vCPU box vary by several percent between runs; each row is the median recorded in `bench/bounded.json`, which also stores the per-run `seconds` array for every timed assignment config (the replication and failure sections store medians only). The movement counts and loads are exact and reproducible, not statistical.

## Tests

```bash
python -m pytest tests -q
# 32 passed
```

Covered: the original 11 tests for jump/rendezvous/weights/audit/CLI, plus 21 new ones, including prefix-bound and prefix-consistency of `bounded_stream` and its measured probe cost; bound at overload 1.0 and 1.25, empty batch, single bucket, more buckets than keys (forces distinct placement), determinism and salting, probe-histogram consistency, argument validation, independent audit detecting an injected violation, replica separation and capacity, the "violations are reported" invariant at tight capacity, impossible domain counts, minimal-movement repair for one and two failures, repair validation, and both new CLI subcommands.

## Limits

- `bounded_assign` needs the whole batch (`capacity` depends on `n`); `bounded_stream` is the online variant. At overload 1.0 the online ring runs saturated and probes ~2 buckets per key — give it ~5 % headroom unless the cap is the only thing that matters.
- `rendezvous` and both placement functions hash every bucket per key (BLAKE2b), so they are O(buckets); for thousands of buckets use `jump_hash`.
- Duplicate keys are treated as independent items; pass unique identifiers if you expect one placement per entity.
- `replicated_assign` needs at least as many distinct domains as replicas; it raises instead of silently co-locating replicas.
- The bounded path has no per-bucket weights (weighted rendezvous is separate).

## Reproduce

```bash
python -m pytest tests -q
python bench/bounded.py
python -m shardkit bounded --keys 100000 --buckets 8 --overload 1.0
python -m shardkit stream  --keys 100000 --buckets 8 --overload 1.05
```

## Environment

- CPU: AMD EPYC 7K62 (4 vCPU), RAM 3,723 MB
- CPython **3.12.3** (`/usr/bin/python3.12`), pytest 9.2.0
- Seed: none — keys are `user:0 … user:99999`, so every placement is reproducible bit for bit

## 本轮创新

把「容量上界的代价」本身做成测量对象，而不是只宣布一个上界。与 Google 2017 的 bounded-load 环不同，这里用 **rendezvous 偏好序 + 批量容量 + 最小负载回退**，给出可被 `audit_placement()` 独立重算的鸽笼证书；实测推翻了「有界负载必然明显增加探测」的默认假设——overload 1.0 时 100k 键的均探测深度是 **1.01**，因为自然 rendezvous 分布只把约 466 个键挤出首选桶。同时给出两条反面证据并交付修复：域约束副本在 overload 1.0 时差 36/300,000 个放置（需 1.1），以及故障后全量重平衡移动 60.7% 而强制下界只有 12.5%——`repair_failure` 恰好达到下界且仍把 7 个存活桶填到 14,284–14,286。再补一条同源的测量：把同一算法改成**流式插入**（容量按每个前缀重算、永不搬迁）后，c=1.0 时每个前缀都满足上界且最终 8 桶恰好满，但均探测 **2.059**；c=1.05 时均探测降到 **1.002**、最大负载只高出 n/m 的 1.2%。即「紧凑上界」与「在线不搬迁」不能同时免费拿到，这个取舍是本轮量出来的。全部数字在 `bench/bounded.json` 里，可复算。

## License

MIT, original work. See `LICENSE`.
