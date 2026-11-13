"""CLI: python -m shardkit {audit,bounded,replicas} ..."""

from __future__ import annotations

import argparse
import json

from .core import audit, jump_hash
from .placement import bounded_assign, replicated_assign


def _summary(report: dict, drop=("placements", "domains")) -> dict:
    return {k: v for k, v in report.items() if k not in drop}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="shardkit")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("audit", help="report rendezvous placement balance")
    p.add_argument("--keys", type=int, default=200_000)
    p.add_argument("--buckets", type=int, default=8)
    p.add_argument("--salt", default="")

    b = sub.add_parser("bounded", help="bounded-load assignment and its certificate")
    b.add_argument("--keys", type=int, default=100_000)
    b.add_argument("--buckets", type=int, default=8)
    b.add_argument("--overload", type=float, default=1.0)
    b.add_argument("--salt", default="")

    r = sub.add_parser("replicas", help="distinct-domain replica placement")
    r.add_argument("--keys", type=int, default=100_000)
    r.add_argument("--buckets", type=int, default=8)
    r.add_argument("--replicas", type=int, default=3)
    r.add_argument("--domains", type=int, default=4)
    r.add_argument("--overload", type=float, default=1.25)
    r.add_argument("--salt", default="")

    args = parser.parse_args(argv)
    keys = [f"user:{i}" for i in range(args.keys)]
    salt = args.salt.encode()

    if args.cmd == "audit":
        report = audit(keys, args.buckets, salt)
        report["salt"] = args.salt
        report["jump_range_ok"] = all(
            0 <= jump_hash(i, args.buckets) < args.buckets for i in range(1000)
        )
        print(json.dumps(report, indent=2))
        return 0

    if args.cmd == "bounded":
        report = bounded_assign(keys, args.buckets, args.overload, salt)
        print(json.dumps(_summary(report), indent=2))
        return 0

    if args.domains < 2:
        parser.error("--domains must be >= 2")
    domain_of = [f"zone-{b % args.domains}" for b in range(args.buckets)]
    report = replicated_assign(
        keys, args.buckets, args.replicas, domain_of, args.overload, salt
    )
    print(json.dumps(_summary(report), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
