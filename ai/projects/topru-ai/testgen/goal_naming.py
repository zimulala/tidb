#!/usr/bin/env python3
"""Shared TopRU goal -> Go test naming rules."""

from __future__ import annotations

import argparse
import re
import sys

GOAL_RE = re.compile(r"^(G[0-9]+)(?:_(.+))?$")


def snake_to_camel(s: str) -> str:
    parts = [p for p in s.split("_") if p]
    return "".join(p[:1].upper() + p[1:] for p in parts)


def goal_id_to_suffix(goal_id: str) -> str:
    m = GOAL_RE.match(goal_id)
    if not m:
        raise ValueError(f"invalid goal id: {goal_id}")
    prefix = m.group(1)
    tail = m.group(2)
    if not tail:
        return prefix
    return prefix + snake_to_camel(tail)


def goal_to_test_name(prefix: str, goal_id: str) -> str:
    return f"{prefix}{goal_id_to_suffix(goal_id)}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--goal")
    ap.add_argument("--goals-stdin", action="store_true")
    args = ap.parse_args()

    if bool(args.goal) == bool(args.goals_stdin):
        ap.error("exactly one of --goal or --goals-stdin is required")

    if args.goal:
        print(goal_to_test_name(args.prefix, args.goal))
        return 0

    for raw in sys.stdin:
        goal = raw.strip()
        if not goal:
            continue
        print(f"{goal}\t{goal_to_test_name(args.prefix, goal)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
