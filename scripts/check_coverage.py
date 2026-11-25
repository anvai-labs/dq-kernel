#!/usr/bin/env python3
"""Enforce independent line and branch coverage floors from coverage.py JSON.

The totals describe this repository or they describe nothing. A report that
holds paths outside it is refused before a floor is read: something was
measured that is not this tree, and no percentage from such a report says
what is tested here.
"""

import argparse
import json
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PATHS_LISTED = 3


def inside_repository(path, root=REPOSITORY_ROOT):
    """Return whether a reported path is inside ``root``.

    A relative path is taken from the repository root, which is where
    coverage runs, so the verdict does not depend on the caller's directory.
    """
    reported = Path(path)
    if not reported.is_absolute():
        reported = root / reported
    return reported.resolve().is_relative_to(root)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("report", type=Path)
    parser.add_argument("--min-line", type=float, required=True)
    parser.add_argument("--min-branch", type=float, required=True)
    args = parser.parse_args(argv)

    report = json.loads(args.report.read_text(encoding="utf-8"))
    outside = sorted(path for path in report["files"] if not inside_repository(path))
    if outside:
        listed = "\n  ".join(outside[:PATHS_LISTED])
        unlisted = len(outside) - PATHS_LISTED
        if unlisted > 0:
            listed += f"\n  ... and {unlisted} more"
        count = len(outside)
        plural = "" if count == 1 else "s"
        raise SystemExit(
            f"{count} path{plural} in {args.report} "
            f"{'is' if count == 1 else 'are'} outside the repository:\n"
            f"  {listed}\n"
            "Its totals do not describe this checkout: something was measured that "
            "is not this tree, such as a test running a copy of it."
        )

    totals = report["totals"]
    line = 100 * totals["covered_lines"] / totals["num_statements"]
    branch = 100 * totals["covered_branches"] / totals["num_branches"]
    print(f"line coverage: {line:.2f}% (minimum {args.min_line:.2f}%)")
    print(f"branch coverage: {branch:.2f}% (minimum {args.min_branch:.2f}%)")
    if line < args.min_line or branch < args.min_branch:
        raise SystemExit("coverage floor not met")


if __name__ == "__main__":
    main()
