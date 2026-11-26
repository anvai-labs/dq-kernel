# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the CI workflow files.

The CI defects these pin were all real: a merge gate that ignored a red
lane, a trigger filter that left stacked pull requests untested, and
unpinned actions. Workflow YAML is code, so it gets contracts too.
"""

import json
import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPOSITORY_ROOT / ".github" / "workflows"
_SHA_PIN = re.compile(r"^[^@]+@[0-9a-f]{40}$")


def _load(name):
    return yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))


def _triggers(workflow):
    """Return the `on:` mapping (YAML 1.1 parses the bare key `on` as True)."""
    return workflow.get(True, workflow.get("on"))


@pytest.mark.parametrize("name", sorted(p.name for p in WORKFLOWS.glob("*.yml")))
def test_every_action_is_pinned_to_a_commit_sha(name):
    unpinned = []

    def walk(node):
        if isinstance(node, dict):
            uses = node.get("uses")
            if isinstance(uses, str) and not _SHA_PIN.match(uses):
                unpinned.append(uses)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(_load(name))
    assert unpinned == [], f"{name} has actions not pinned to a 40-hex SHA: {unpinned}"


def test_pull_requests_are_tested_whatever_their_base_branch():
    """Stacked pull requests (base = a feature branch) get the same gates."""
    triggers = _triggers(_load("ci.yml"))
    assert "pull_request" in triggers, "ci.yml must run on pull_request events"
    # An unfiltered trigger parses to null; anything else is a branch filter.
    branches = (triggers["pull_request"] or {}).get("branches")
    assert branches is None, (
        f"ci.yml filters pull_request to {branches}; pull requests to any "
        "base branch must be tested"
    )


def test_merge_gate_lists_every_other_lane():
    jobs = _load("ci.yml")["jobs"]
    gate = jobs["build"]
    assert set(gate["needs"]) == set(jobs) - {"build"}, (
        "the build gate must depend on every other job, or a new lane can "
        "go red without blocking the merge"
    )


def _gate_script():
    steps = _load("ci.yml")["jobs"]["build"]["steps"]
    verify = next(step for step in steps if step.get("name") == "Verify required jobs")
    assert "toJSON(needs)" in verify["env"].get(
        "NEEDS", ""
    ), "the gate must read the whole needs map, not named single-job results"
    return verify["run"]


def _lanes(**results):
    """Build a `needs` map the way toJSON(needs) renders it."""
    return {name: {"result": result, "outputs": {}} for name, result in results.items()}


def _run_gate(tmp_path, lanes, event="pull_request"):
    """Execute the real gate script from ci.yml against a sample needs map."""
    script = tmp_path / "gate.sh"
    script.write_text(_gate_script(), encoding="utf-8")
    return subprocess.run(
        ["bash", str(script)],
        env={
            "PATH": os.environ["PATH"],
            "EVENT_NAME": event,
            "NEEDS": json.dumps(lanes),
        },
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def test_gate_passes_when_every_lane_succeeds(tmp_path):
    result = _run_gate(
        tmp_path,
        _lanes(lint="success", test="success", **{"dependency-review": "success"}),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "lint: success" in result.stdout


def test_gate_fails_when_any_lane_fails(tmp_path):
    result = _run_gate(
        tmp_path,
        _lanes(lint="success", test="failure", **{"dependency-review": "success"}),
    )
    assert result.returncode == 1
    assert "test: failure" in result.stdout


@pytest.mark.parametrize("result", ["failure", "cancelled", "timed_out"])
def test_gate_fails_on_every_non_success_result(tmp_path, result):
    lanes = _lanes(lint=result, test="success", **{"dependency-review": "success"})
    assert _run_gate(tmp_path, lanes).returncode == 1


def test_gate_passes_when_a_lane_is_legitimately_skipped(tmp_path):
    lanes = _lanes(
        lint="success",
        test="success",
        **{"dependency-review": "success", "docs-only": "skipped"},
    )
    assert _run_gate(tmp_path, lanes).returncode == 0


def test_gate_requires_the_dependency_review_on_pull_requests(tmp_path):
    result = _run_gate(
        tmp_path,
        _lanes(lint="success", test="success", **{"dependency-review": "skipped"}),
    )
    assert result.returncode == 1
    assert "dependency-review" in result.stdout


def test_gate_accepts_a_skipped_dependency_review_on_push(tmp_path):
    lanes = _lanes(lint="success", test="success", **{"dependency-review": "skipped"})
    assert _run_gate(tmp_path, lanes, event="push").returncode == 0


def test_merge_gate_runs_even_when_a_lane_fails():
    gate = _load("ci.yml")["jobs"]["build"]
    assert "always()" in (
        gate.get("if") or ""
    ), "without always() the gate is skipped when a lane fails and reports nothing"


def test_main_test_lane_installs_the_extras_whose_tests_need_them():
    install = next(
        step
        for step in _load("ci.yml")["jobs"]["test"]["steps"]
        if step.get("name") == "Install dependencies"
    )["run"]
    installed = set(re.findall(r"-E\s+([a-z]+)", install))
    # Engines with live test modules that importorskip their vendor package
    # only run in CI if the extra that provides it is installed here.
    assert {
        "spark",
        "deequ",
        "aws",
        "databricks",
        "greatexpectations",
        "dqx",
    } <= installed
    assert install.count("poetry install") == 1


def test_lint_lane_checks_the_lock_matches_pyproject():
    names = [step.get("name") for step in _load("ci.yml")["jobs"]["lint"]["steps"]]
    assert any(
        "lock" in (name or "").lower()
        and "poetry check --lock"
        in next(
            s.get("run", "")
            for s in _load("ci.yml")["jobs"]["lint"]["steps"]
            if s.get("name") == name
        )
        for name in names
    ), "lint must run `poetry check --lock` so pyproject/lock drift cannot merge"


def test_lint_lane_validates_workflow_syntax():
    runs = [step.get("run", "") for step in _load("ci.yml")["jobs"]["lint"]["steps"]]
    actionlint = next((run for run in runs if "actionlint" in run), "")
    assert "actionlint" in actionlint
    assert re.search(
        r"[0-9a-f]{64}\s+actionlint\.tar\.gz", actionlint
    ), "the actionlint download must be verified against a pinned sha256"
