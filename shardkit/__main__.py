"""CLI: python -m shardkit audit --keys N --buckets B [--salt S]"""

from __future__ import annotations

import argparse
import json

from .core import audit, jump_hash, rendezvous


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="shardkit")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("audit", help="report rendezvous placement balance")
    p.add_argument("--keys", type=int, default=200_000)
    p.add_argument("--buckets", type=int, default=8)
    p.add_argument("--salt", default="")
    args = parser.parse_args(argv)

    keys = [f"user:{i}" for i in range(args.keys)]
    report = audit(keys, args.buckets, args.salt.encode())
    report["salt"] = args.salt
    report["jump_range_ok"] = all(
        0 <= jump_hash(i, args.buckets) < args.buckets for i in range(1000)
    )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
