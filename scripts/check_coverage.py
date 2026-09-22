#!/usr/bin/env python3
"""Enforce independent line and branch coverage floors from coverage.py JSON."""

import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--min-line", type=float, required=True)
    parser.add_argument("--min-branch", type=float, required=True)
    args = parser.parse_args()

    totals = json.loads(args.report.read_text(encoding="utf-8"))["totals"]
    line = 100 * totals["covered_lines"] / totals["num_statements"]
    branch = 100 * totals["covered_branches"] / totals["num_branches"]
    print(f"line coverage: {line:.2f}% (minimum {args.min_line:.2f}%)")
    print(f"branch coverage: {branch:.2f}% (minimum {args.min_branch:.2f}%)")
    if line < args.min_line or branch < args.min_branch:
        raise SystemExit("coverage floor not met")


if __name__ == "__main__":
    main()
