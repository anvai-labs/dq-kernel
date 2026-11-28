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
import shutil
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
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


@pytest.mark.parametrize("schema", ["rule-pack.schema.json", "profile.schema.json"])
def test_build_job_verifies_the_published_schema_is_in_the_wheel(schema):
    """Each published schema is an artifact: the rule pack's (SPEC-010) and
    the profile's (SPEC-019)."""
    runs = [step.get("run", "") for step in _load("ci.yml")["jobs"]["build"]["steps"]]
    check = next((run for run in runs if schema in run), "")
    assert check, f"the build job must verify dq/schemas/{schema} is in the wheel"
    assert "assert" in check and "dist" in check
    assert f'"dq/schemas/{schema}"' in check  # the path asserted, not mentioned


def test_build_jobs_prove_the_wheel_needs_no_extra():
    """The development group carries three extras' packages (datafusion brings
    pyarrow, duckdb-engine brings SQLAlchemy, the OpenTelemetry SDK brings the
    OpenTelemetry API), and every lane has Spark, so no lane that installs it
    can show the package manages without them. Both lanes build the wheel, so
    both prove it."""
    for workflow, job in (("ci.yml", "build"), ("owner-private-ci.yml", "owner-build")):
        runs = [step.get("run", "") for step in _load(workflow)["jobs"][job]["steps"]]
        check = next((run for run in runs if "check_core_install.py" in run), "")
        assert check, f"{workflow}: {job} must run scripts/check_core_install.py"
        assert "python -m venv" in check, "the proof needs an empty environment"
        assert (
            "*.whl" in check and check.count("pip install") == 1
        ), "of the wheel alone"
        built = next(i for i, run in enumerate(runs) if "poetry build" in run)
        assert runs.index(check) > built, "the wheel must exist first"


#: Where each lane's link check begins, once the checker is downloaded.
DOWNLOAD = 'archive="${RUNNER_TEMP}/lychee-x86_64-unknown-linux-musl.tar.gz"'

LANES = ("ci.yml", "owner-private-ci.yml")


def link_checks():
    """Return each lane's link-check step, by workflow."""
    found = {}
    for workflow in LANES:
        steps = _load(workflow)["jobs"]["dependency-review"]["steps"]
        found[workflow] = next(
            (step for step in steps if "check_links.py" in step.get("run", "")), {}
        )
    return found


def test_both_lanes_check_the_links_again_while_only_the_remote_s_failures_remain():
    """The check went red three times in one day on a link no pull request
    had touched. lychee does not retry a status code, and GitHub's file pages
    answer 503 to everybody for half an hour at a time, while the same file
    at its raw address answers steadily."""
    for workflow, step in link_checks().items():
        check = step.get("run", "")
        assert (
            check
        ), f"{workflow}: the dependency review must run scripts/check_links.py"
        assert "check_links.py --attempts" in check, "the check is tried more than once"
        assert (
            "sha256sum --check --strict" in check
        ), "the checker is verified against a pin"
        pin = step.get("env", {}).get("LYCHEE_ARCHIVE_SHA256", "")
        assert re.fullmatch(r"[0-9a-f]{64}", pin), f"{workflow}: the pin is a checksum"
        assert (
            "--remap" in check and "raw.githubusercontent.com" in check
        ), "a GitHub file page is asked for as the file itself"


def test_both_lanes_make_the_same_link_check():
    """The owner lane runs instead of the public one on the owner's own pull
    requests; it must not check less, or differently -- and both must pin the
    checker to one checksum, so neither lane trusts another download."""
    checks = link_checks()
    pins = {
        step.get("env", {}).get("LYCHEE_ARCHIVE_SHA256") for step in checks.values()
    }
    assert len(pins) == 1 and None not in pins, pins
    public, owner = (
        step["run"][step["run"].index(DOWNLOAD) :] for step in checks.values()
    )
    assert DOWNLOAD in public and DOWNLOAD in owner
    assert public == owner


# ------------------------------------------------- the JARs the test lanes need

#: ``(where the step puts it, where it comes from)`` for each JAR.
JARS = (
    (
        "lib/deequ-2.0.21-spark-3.5.jar",
        "https://repo1.maven.org/maven2/com/amazon/deequ/deequ/"
        "2.0.21-spark-3.5/deequ-2.0.21-spark-3.5.jar",
    ),
    (
        "lib/dqdl-1.0.0.jar",
        "https://repo1.maven.org/maven2/software/amazon/glue/dqdl/1.0.0/dqdl-1.0.0.jar",
    ),
)
#: Where the download step's script begins.
JAR_DOWNLOAD = "mkdir -p lib\n"
#: The wait the step allows itself for retries, as the test runs it: the
#: workflow's own cap is two minutes, which is not waited for in a test.
CAP_IN_A_TEST = 3


def jar_downloads():
    """Return each lane's JAR-download step, by workflow."""
    found = {}
    for workflow in LANES:
        steps = [
            step for job in _load(workflow)["jobs"].values() for step in job["steps"]
        ]
        found[workflow] = next(
            (step for step in steps if step.get("name") == "Download Deequ JAR"), {}
        )
    return found


def test_both_lanes_take_the_same_jar_download():
    """The step went red twice on Maven Central's bad minute -- a server error
    wget tried once -- and green on a rerun with nothing changed: wget's exit
    8 on the SPEC-019 Verified run and on a Dependabot run. A run that fails
    on a remote and passes on a rerun says nothing about the commit."""
    scripts = []
    for workflow, step in jar_downloads().items():
        script = step.get("run", "")
        assert JAR_DOWNLOAD in script, f"{workflow}: no step downloads the JARs"
        assert "wget" not in script, f"{workflow}: wget tries once"
        assert "--fail" in script, f"{workflow}: an error page saved as a JAR"
        assert (
            "--retry-all-errors" not in script
        ), f"{workflow}: a wrong address is a 404, and is said at once"
        assert re.search(r"--retry [1-9]", script), f"{workflow}: nothing is retried"
        assert re.search(
            r"--retry-max-time [1-9]", script
        ), f"{workflow}: the retries are not capped in time"
        for _placed, address in JARS:
            assert address in script, f"{workflow}: {address} is not downloaded"
        scripts.append(script[script.index(JAR_DOWNLOAD) :])
    # The owner lane runs instead of the public one on the owner's own pull
    # requests: it must download no differently.
    assert scripts[0] == scripts[1]


class _Remote(BaseHTTPRequestHandler):
    """A remote as Maven Central is on a bad minute. ``/flaky`` answers an
    error twice for each JAR asked for, then well; ``/missing`` is a 404;
    ``/down`` answers an error for good. Every hit is counted."""

    hits: dict = {}

    def do_GET(self):
        _Remote.hits[self.path] = _Remote.hits.get(self.path, 0) + 1
        seen = _Remote.hits[self.path]
        if self.path.startswith("/missing"):
            status = 404
        elif self.path.startswith("/down") or (
            self.path.startswith("/flaky") and seen <= 2
        ):
            status = 503
        else:
            status = 200
        self.send_response(status)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *arguments):
        pass


@pytest.fixture
def remote():
    """Return where a remote with a bad minute is listening, and its hits."""
    _Remote.hits = {}
    server = HTTPServer(("127.0.0.1", 0), _Remote)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", _Remote.hits
    finally:
        server.shutdown()
        server.server_close()


def step_script(workflow):
    """Return a lane's download step as the workflow holds it."""
    step = jar_downloads()[workflow]["run"]
    return step[step.index(JAR_DOWNLOAD) :]


def download_from(workflow, origin, answer, where):
    """Run a lane's download step as written, from ``origin`` instead of Maven
    Central, in ``where``: return its exit code and what it said."""
    script = step_script(workflow)
    for placed, address in JARS:
        script = script.replace(address, f"{origin}/{answer}?jar={placed}")
    # The workflow waits two minutes at most for retries; a test does not.
    script = re.sub(
        r"--retry-max-time \d+", f"--retry-max-time {CAP_IN_A_TEST}", script
    )
    # The shell a workflow's step runs in.
    done = subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
        cwd=where,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return done.returncode, done.stderr


@pytest.mark.skipif(not shutil.which("curl"), reason="the step downloads with curl")
def test_the_jar_download_retries_what_is_transient_and_only_that(remote, tmp_path):
    """The step's own options, run: the words above it are held to what they
    say. Only the addresses and the cap are changed."""
    origin, hits = remote
    files = [tmp_path / placed for placed, _address in JARS]

    # A bad minute, then well: retried, and the JARs are what was answered.
    code, said = download_from("ci.yml", origin, "flaky", tmp_path)
    assert code == 0, said
    assert [path.read_text() for path in files] == ["ok", "ok"]
    assert sorted(hits.values()) == [3, 3], hits  # twice an error, then the JAR
    assert said.count("returned error: 503") == 4, said

    # A wrong address: said at once, retried none, and nothing is saved.
    _Remote.hits.clear()
    for path in files:
        path.unlink()
    code, said = download_from("ci.yml", origin, "missing", tmp_path)
    assert code == 22 and "returned error: 404" in said, said
    assert sum(hits.values()) == 1, hits  # the step stops at its first failure
    assert not any(path.exists() for path in files)

    # Down for good: given up within the cap, and nothing is saved.
    _Remote.hits.clear()
    started = time.monotonic()
    code, said = download_from("ci.yml", origin, "down", tmp_path)
    elapsed = time.monotonic() - started
    assert code != 0 and "returned error: 503" in said, said
    # Tried again -- and stopped by the cap, before the count it was allowed.
    allowed = int(re.search(r"--retry (\d+)", step_script("ci.yml")).group(1))
    assert 3 <= sum(hits.values()) < allowed + 1, (hits, allowed)
    assert elapsed < 2 * (2**allowed), elapsed  # not the backoff of them all
    assert not any(path.exists() for path in files)
