#!/usr/bin/env python
"""Regenerate and check the spec tracker in docs/roadmap.adoc (SPEC-008).

Usage:
  python scripts/roadmap.py            # print progress and any contract problems
  python scripts/roadmap.py --check    # non-zero exit if the tracker is stale
  python scripts/roadmap.py --write    # rewrite the generated blocks from the specs

The spec files under docs/specs/ are the source of truth; never edit the
generated blocks in docs/roadmap.adoc by hand.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from dq.tests.spec_support import (  # noqa: E402
    ROADMAP_PATH,
    load_specs,
    milestone_progress,
    render_summary,
    render_tracker,
    specs_problems,
    tracker_block,
    tracker_summary_block,
    write_tracker,
)

def print_progress(specs) -> None:
    progress = milestone_progress(specs)
    for milestone in sorted(progress):
        done, total, verified, count = progress[milestone]
        print(f"{milestone}: {verified}/{count} specs verified, {done}/{total} gates")
    for spec in specs:
        marks = "".join("x" if checked else "." for checked, _, _ in spec.progress)
        print(f"  {spec.id}  {spec.status:<11} {spec.milestone:<8} [{marks}] {spec.title}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--check", action="store_true", help="fail if the tracker is stale")
    group.add_argument("--write", action="store_true", help="rewrite the generated blocks")
    args = parser.parse_args(argv)

    specs = load_specs()
    problems = specs_problems(specs)
    text = ROADMAP_PATH.read_text(encoding="utf-8")
    if args.write:
        text = write_tracker(text, specs)
        ROADMAP_PATH.write_text(text, encoding="utf-8")
        print(f"wrote {ROADMAP_PATH.relative_to(REPOSITORY_ROOT)}")

    # Evaluate staleness against what is on disk (re-read after --write).
    text = ROADMAP_PATH.read_text(encoding="utf-8")
    current = (
        tracker_block(text).strip("\n"),
        tracker_summary_block(text).strip("\n"),
    )
    expected = (render_tracker(specs).strip("\n"), render_summary(specs).strip("\n"))
    stale = [
        name
        for name, want, have in zip(("tracker", "summary"), expected, current)
        if want != have
    ]

    print_progress(specs)
    for problem in problems:
        print(f"PROBLEM: {problem}")
    for name in stale:
        print(f"STALE: the {name} block differs; run `python scripts/roadmap.py --write`")
    return 1 if args.check and (problems or stale) else (1 if problems else 0)


if __name__ == "__main__":
    sys.exit(main())
