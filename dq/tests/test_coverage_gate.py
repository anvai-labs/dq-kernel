# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The coverage gate's own contract (SPEC-009: gates that can be trusted).

It is the last check between a coverage report and a green build, so it is
exercised the way CI runs it: the real script, a real report file, and real
exit codes.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY_ROOT / "scripts" / "check_coverage.py"
LINE, BRANCH = 57, 46
# 80% of the lines and 90% of the branches: both floors met.
MET = {
    "covered_lines": 80,
    "num_statements": 100,
    "covered_branches": 45,
    "num_branches": 50,
}


def report(tmp_path, files, totals=MET):
    """Write a coverage JSON report holding ``files`` and return its path."""
    path = tmp_path / "coverage.json"
    path.write_text(json.dumps({"totals": totals, "files": files}), encoding="utf-8")
    return path


def gate(path, cwd=REPOSITORY_ROOT):
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            str(path),
            "--min-line",
            str(LINE),
            "--min-branch",
            str(BRANCH),
        ],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def test_a_report_that_meets_both_floors_passes_and_prints_them(tmp_path):
    result = gate(report(tmp_path, {"dq/sdk/scaffold.py": {}}))
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "line coverage: 80.00% (minimum 57.00%)",
        "branch coverage: 90.00% (minimum 46.00%)",
    ]


@pytest.mark.parametrize(
    "totals, below",
    [
        # 102 of 150 lines-and-branches covered: 68% on the combined figure
        # that pytest-cov's own --cov-fail-under reads (55 in CI).
        (
            {
                "covered_lines": 80,
                "num_statements": 100,
                "covered_branches": 22,
                "num_branches": 50,
            },
            "branch",
        ),
        # 106 of 150: 71% combined.
        (
            {
                "covered_lines": 56,
                "num_statements": 100,
                "covered_branches": 50,
                "num_branches": 50,
            },
            "line",
        ),
    ],
    ids=["branch-floor", "line-floor"],
)
def test_each_floor_is_enforced_on_its_own(tmp_path, totals, below):
    """Both reports clear the combined figure; each must fail one floor."""
    result = gate(report(tmp_path, {"dq/sdk/scaffold.py": {}}, totals))
    assert result.returncode != 0
    assert "coverage floor not met" in result.stderr
    assert f"{below} coverage:" in result.stdout


def test_a_report_holding_paths_outside_the_repository_is_refused(tmp_path):
    """A test that runs a copy of the tree got its copy measured. The totals
    here still meet both floors, and the report is refused all the same."""
    copy = "/private/var/folders/xy/pytest-1/test_x0/repo/dq/engine/soda/soda_engine.py"
    path = report(
        tmp_path,
        {"dq/sdk/scaffold.py": {}, copy: {}, "dq/engine/registry.py": {}},
    )
    result = gate(path)
    assert result.returncode != 0
    assert "1 path in" in result.stderr
    assert copy in result.stderr
    assert "outside the repository" in result.stderr
    assert "line coverage" not in result.stdout


def test_the_refusal_counts_every_path_and_lists_the_first_three(tmp_path):
    files = {f"/elsewhere/copy-{number}/dq/sdk/scaffold.py": {} for number in range(5)}
    result = gate(report(tmp_path, files))
    assert result.returncode != 0
    assert "5 paths in" in result.stderr
    assert [line.strip() for line in result.stderr.splitlines() if "copy-" in line] == [
        "/elsewhere/copy-0/dq/sdk/scaffold.py",
        "/elsewhere/copy-1/dq/sdk/scaffold.py",
        "/elsewhere/copy-2/dq/sdk/scaffold.py",
    ]
    assert "... and 2 more" in result.stderr


def test_both_spellings_of_a_repository_path_are_accepted(tmp_path):
    """Coverage reports a file under its working directory relatively and any
    other file absolutely; the checkout itself is inside either way."""
    absolute = str(REPOSITORY_ROOT / "dq" / "sdk" / "scaffold.py")
    path = report(tmp_path, {"dq/sdk/scaffold.py": {}, absolute: {}})
    assert gate(path).returncode == 0
    # The verdict is the checkout's, not the caller directory's.
    assert gate(path, cwd=tmp_path).returncode == 0


def test_an_absolute_path_that_only_looks_like_the_repository_is_refused(tmp_path):
    look_alike = str(REPOSITORY_ROOT) + "-copy/dq/sdk/scaffold.py"
    result = gate(report(tmp_path, {look_alike: {}}))
    assert result.returncode != 0
    assert look_alike in result.stderr
