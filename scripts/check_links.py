#!/usr/bin/env python3
# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Check the documentation's links again while only the remote's failures remain.

Usage: python scripts/check_links.py [--attempts N] [--wait SECONDS] -- LYCHEE [ARGUMENTS ...]

``lychee`` does not retry a status code. A 503 from a host that is well
again a minute later is final to it, and a link check that fails on that
costs a re-run for a link nobody broke: twice in one week for a URL on
github.com that neither pull request had touched.

So the check is run up to ``--attempts`` times, ``--wait`` seconds apart,
and run again only while every failure is one a retry can mend: a 5xx
answer, a connection refused, a timeout. A failure of that kind that
persists still fails the check. A link that is really broken -- a 404, a
missing file, a malformed URL -- fails at once, on the first attempt.

The arguments after ``--`` are lychee's own, after the path of its binary.
Give it no ``--format`` and no ``--output``: the check has lychee write a
report of its own to read, and prints what lychee printed.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Tuple

#: How often to try, and how long to wait between tries.
ATTEMPTS = 3
WAIT = 30

#: Status codes a retry can mend: the remote's own failures.
SERVER_ERROR = range(500, 600)

#: What lychee calls a failure to reach a host at all.
TRANSPORT = ("Network error", "Timeout")

#: Exit codes: a broken link, and a wrong command line.
BROKEN = 1
USAGE = 2

#: Lychee's own report options, which the check sets itself.
OWN = ("--format", "-f", "--output", "-o")


def transient(status: dict) -> bool:
    """Return whether a retry can mend what ``status`` reports."""
    code = status.get("code")
    if isinstance(code, int):
        return code in SERVER_ERROR
    return status.get("text", "").startswith(TRANSPORT)


def describe(failed: dict) -> str:
    """Return one failure as a line: the link, where it was written, and why.

    A link asked about somewhere else than where it was written -- a GitHub
    file page, asked for as the file itself -- is named as it was written,
    with the address it was asked at.
    """
    place = failed.get("span", {})
    asked = failed.get("url")
    written = failed.get("remap", {}).get("original", {}).get("url") or asked
    line = "{} (at {}:{}) | {}".format(
        written,
        place.get("line"),
        place.get("column"),
        failed.get("status", {}).get("text"),
    )
    if written != asked:
        line += f" [asked as {asked}]"
    return line


def findings(report: dict) -> Tuple[List[str], List[str]]:
    """Return ``(mended, final)``: each failure of ``report``, as a line.

    Args:
        report: The document lychee writes with ``--format json``.
    """
    mended, final = [], []
    for statuses in report.get("timeout_map", {}).values():
        mended.extend(describe(failed) for failed in statuses)
    for statuses in report.get("error_map", {}).values():
        for failed in statuses:
            if transient(failed.get("status", {})):
                mended.append(describe(failed))
            else:
                final.append(describe(failed))
    return mended, final


def summary(report: dict) -> str:
    """Return one line for a check that passed: how many links, how many answered.

    Args:
        report: The document lychee writes with ``--format json``.
    """
    total = report.get("total", 0)
    good = report.get("successful", 0)
    return f"{good} of {total} links answered; {total - good} excluded or unsupported"


def run(lychee: str, arguments: List[str], report: Path):
    """Run ``lychee`` once over ``arguments``, its report written to ``report``."""
    return subprocess.run(
        [lychee, "--format", "json", "--output", str(report), *arguments],
        capture_output=True,
        text=True,
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Check the links again while only the remote's failures remain."
    )
    parser.add_argument("--attempts", type=int, default=ATTEMPTS)
    parser.add_argument("--wait", type=int, default=WAIT)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or any(argument in OWN for argument in command[1:]):
        print(
            "usage: check_links.py [--attempts N] [--wait SECONDS] -- LYCHEE [ARGUMENTS ...]",
            file=sys.stderr,
        )
        return USAGE

    mended: List[str] = []
    with tempfile.TemporaryDirectory() as directory:
        report = Path(directory) / "links.json"
        for attempt in range(1, args.attempts + 1):
            done = run(command[0], command[1:], report)
            if done.returncode == 0:
                print(done.stdout, end="")
                if report.is_file():
                    print(summary(json.loads(report.read_text())))
                return 0
            if not report.is_file():
                # No verdict on the links: lychee itself could not run.
                print(done.stdout, end="")
                print(done.stderr, end="", file=sys.stderr)
                # Its own exit code, where there is one to hand on: a
                # process killed by a signal exits below zero.
                return done.returncode if 0 < done.returncode < 256 else BROKEN
            mended, final = findings(json.loads(report.read_text()))
            for line in final:
                print("problem: a link is broken: " + line)
            if final:
                return BROKEN
            for line in mended:
                print("the remote failed: " + line)
            if attempt < args.attempts:
                print(
                    f"{len(mended)} link(s) failed for a reason a retry can mend: "
                    f"trying again in {args.wait}s (attempt {attempt} of {args.attempts})"
                )
                time.sleep(args.wait)
    for line in mended:
        print(f"problem: still failing after {args.attempts} attempts: {line}")
    return BROKEN


if __name__ == "__main__":
    sys.exit(main())
