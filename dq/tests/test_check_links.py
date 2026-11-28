# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The link check's retry: what is tried again, and what fails at once.

The checker is stood in for by a runner that answers from a queue of
reports. One test runs the real ``lychee`` against a local server, where a
binary is at hand: name one in ``DQ_TEST_LYCHEE``, or put it on the path.
"""

import importlib.util
import json
import os
import shutil
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY_ROOT / "scripts" / "check_links.py"


def load():
    spec = importlib.util.spec_from_file_location("check_links", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


check = load()

#: What the stand-in answers when every link passed.
PASSED = object()


def answered(code):
    """Return the status lychee writes for an HTTP answer."""
    return {"code": code, "text": f"Rejected status code: {code}"}


REFUSED = {
    "text": "Network error: Connection refused - server may be down or port blocked"
}
MISSING = {"text": "File not found. Check if file exists and path is correct"}
TIMED_OUT = {"text": "Timeout: the request took longer than 30s"}


def entry(url, status, line=3):
    return {"url": url, "status": status, "span": {"line": line, "column": 1}}


def report(errors=(), timeouts=()):
    """Return a lychee report holding the given failures, all from one file."""
    return {
        "error_map": {"docs/page.adoc": list(errors)},
        "timeout_map": {"docs/page.adoc": list(timeouts)} if timeouts else {},
        "errors": len(errors),
        "timeouts": len(timeouts),
    }


def wired(monkeypatch, answers, stderr=""):
    """Stand in for lychee with a queue of ``answers``, one per attempt.

    An answer is a report (written, and lychee exits by what it holds),
    ``PASSED`` (exits 0 and wrote none), or ``None`` (it could not run: no
    report, and ``stderr`` to print).
    """
    state = SimpleNamespace(runs=[], sleeps=[], answers=list(answers), stderr=stderr)

    def run(binary, arguments, target):
        state.runs.append((binary, list(arguments)))
        answer = state.answers[min(len(state.runs) - 1, len(state.answers) - 1)]
        if answer is None:
            return SimpleNamespace(returncode=2, stdout="", stderr=state.stderr)
        if answer is PASSED:
            return SimpleNamespace(returncode=0, stdout="all links passed\n", stderr="")
        target.write_text(json.dumps(answer))
        failed = answer.get("errors", 0) or answer.get("timeouts", 0)
        return SimpleNamespace(returncode=2 if failed else 0, stdout="", stderr="")

    monkeypatch.setattr(check, "run", run)
    monkeypatch.setattr(check.time, "sleep", state.sleeps.append)
    return state


# ------------------------------------------------------- what a retry mends


@pytest.mark.parametrize(
    ("status", "mended"),
    [
        (answered(500), True),
        (answered(502), True),
        (answered(503), True),
        (answered(599), True),
        (REFUSED, True),
        (TIMED_OUT, True),
        (answered(404), False),
        (answered(403), False),
        (answered(429), False),
        (MISSING, False),
        ({"text": "Invalid URL: missing scheme"}, False),
        ({}, False),
    ],
    ids=[
        "500",
        "502",
        "503",
        "599",
        "connection-refused",
        "timeout",
        "404",
        "403",
        "429",
        "missing-file",
        "invalid-url",
        "nothing",
    ],
)
def test_only_the_remote_s_own_failures_are_what_a_retry_can_mend(status, mended):
    assert check.transient(status) is mended


def test_the_report_s_failures_are_sorted_by_what_a_retry_can_mend():
    mended, final = check.findings(
        report(
            errors=[
                entry("https://a.dev/x", answered(503), 3),
                entry("https://a.dev/y", answered(404), 4),
                entry("b.md", MISSING, 5),
            ],
            timeouts=[entry("https://slow.dev/", TIMED_OUT, 6)],
        )
    )
    assert mended == [
        "https://slow.dev/ (at 6:1) | Timeout: the request took longer than 30s",
        "https://a.dev/x (at 3:1) | Rejected status code: 503",
    ]
    assert final == [
        "https://a.dev/y (at 4:1) | Rejected status code: 404",
        "b.md (at 5:1) | File not found. Check if file exists and path is correct",
    ]


def test_a_link_asked_about_somewhere_else_is_named_as_it_was_written():
    """A GitHub file page is asked for as the file itself; the report says so."""
    written = "https://github.com/awslabs/python-deequ/blob/v1.7.0/pydeequ/configs.py"
    asked = "https://raw.githubusercontent.com/awslabs/python-deequ/v1.7.0/pydeequ/configs.py"
    remapped = {
        "url": asked,
        "status": answered(404),
        "span": {"line": 4, "column": 1},
        "remap": {"original": {"url": written}, "new": {"url": asked}},
    }
    _, final = check.findings(report(errors=[remapped]))
    assert final == [
        f"{written} (at 4:1) | Rejected status code: 404 [asked as {asked}]"
    ]


def test_a_link_asked_about_where_it_was_written_names_one_address_only():
    asked = "https://a.dev/y"
    _, final = check.findings(report(errors=[entry(asked, answered(404))]))
    assert final == [f"{asked} (at 3:1) | Rejected status code: 404"]


def test_a_report_with_no_failure_sorts_nothing():
    assert check.findings({"error_map": {}, "timeout_map": {}}) == ([], [])


def test_a_report_that_lacks_the_maps_is_read_as_no_failure():
    assert check.findings({}) == ([], [])


# ---------------------------------------------------------- the whole check

FLAKY = [report(errors=[entry("https://a.dev/x", answered(503))]), PASSED]
DOWN = [report(errors=[entry("https://a.dev/x", answered(503))])]
DEAD = [report(errors=[entry("https://a.dev/y", answered(404))])]
BOTH = [
    report(
        errors=[
            entry("https://a.dev/x", answered(503)),
            entry("https://a.dev/y", answered(404)),
        ]
    )
]


def test_links_that_pass_are_checked_once(monkeypatch, capsys):
    state = wired(monkeypatch, [PASSED])
    assert check.main(["--", "lychee", "docs"]) == 0
    assert len(state.runs) == 1 and state.sleeps == []
    assert capsys.readouterr().out == "all links passed\n"


def test_a_passing_check_says_how_many_links_answered(monkeypatch, capsys):
    passed = dict(report(), total=32, successful=27)
    state = wired(monkeypatch, [passed])
    assert check.main(["--", "lychee", "docs"]) == 0
    assert len(state.runs) == 1
    assert capsys.readouterr().out == (
        "27 of 32 links answered; 5 excluded or unsupported\n"
    )


def test_lychee_is_asked_for_a_report_where_the_check_reads_it(monkeypatch):
    seen = []

    def run(argv, **options):
        seen.append(argv)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(check.subprocess, "run", run)
    check.run("/tools/lychee", ["--no-progress", "docs"], Path("links.json"))
    assert seen == [
        [
            "/tools/lychee",
            "--format",
            "json",
            "--output",
            "links.json",
            "--no-progress",
            "docs",
        ]
    ]


def test_a_remote_that_fails_and_recovers_is_tried_again(monkeypatch, capsys):
    state = wired(monkeypatch, FLAKY)
    assert check.main(["--wait", "5", "--", "lychee", "docs"]) == 0
    assert len(state.runs) == 2 and state.sleeps == [5]
    printed = capsys.readouterr().out
    assert (
        "the remote failed: https://a.dev/x (at 3:1) | Rejected status code: 503"
        in printed
    )
    assert "trying again in 5s (attempt 1 of 3)" in printed
    assert "problem" not in printed


def test_a_remote_that_stays_down_still_fails(monkeypatch, capsys):
    state = wired(monkeypatch, DOWN)
    assert check.main(["--attempts", "3", "--wait", "5", "--", "lychee", "docs"]) == 1
    assert len(state.runs) == 3 and state.sleeps == [5, 5]
    printed = capsys.readouterr().out
    assert "still failing after 3 attempts: https://a.dev/x (at 3:1)" in printed
    assert printed.count("the remote failed:") == 3


def test_a_broken_link_fails_at_once_and_is_not_retried(monkeypatch, capsys):
    state = wired(monkeypatch, DEAD)
    assert check.main(["--attempts", "5", "--", "lychee", "docs"]) == 1
    assert len(state.runs) == 1 and state.sleeps == []
    assert (
        "problem: a link is broken: https://a.dev/y (at 3:1) | "
        "Rejected status code: 404" in capsys.readouterr().out
    )


def test_a_broken_link_fails_at_once_even_beside_a_flaky_one(monkeypatch):
    state = wired(monkeypatch, BOTH)
    assert check.main(["--", "lychee", "docs"]) == 1
    assert len(state.runs) == 1 and state.sleeps == []


def test_a_timeout_is_tried_again(monkeypatch):
    state = wired(
        monkeypatch, [report(timeouts=[entry("https://slow.dev/", TIMED_OUT)]), PASSED]
    )
    assert check.main(["--wait", "0", "--", "lychee", "docs"]) == 0
    assert len(state.runs) == 2


def test_lychee_that_cannot_run_is_reported_in_its_own_words(monkeypatch, capsys):
    state = wired(monkeypatch, [None], stderr="error: unexpected argument '--x'\n")
    assert check.main(["--", "lychee", "--x"]) == 2
    assert len(state.runs) == 1 and state.sleeps == []
    assert capsys.readouterr().err == "error: unexpected argument '--x'\n"


def test_lychee_that_exits_nonzero_with_no_verdict_is_a_failure(monkeypatch):
    def run(binary, arguments, target):
        return SimpleNamespace(returncode=-9, stdout="", stderr="Killed\n")

    monkeypatch.setattr(check, "run", run)
    assert check.main(["--", "lychee", "docs"]) == 1


def test_no_command_is_a_usage_error(capsys):
    assert check.main([]) == 2
    assert check.main(["--"]) == 2
    assert "usage: check_links.py" in capsys.readouterr().err


@pytest.mark.parametrize("own", ["--format", "-f", "--output", "-o"])
def test_the_report_options_are_the_check_s_own(own, monkeypatch):
    state = wired(monkeypatch, [PASSED])
    assert check.main(["--", "lychee", own, "x", "docs"]) == 2
    assert state.runs == []


# ------------------------------------------------- against the real checker


def lychee_binary():
    """Return the path of a real lychee, where one is at hand."""
    found = os.environ.get("DQ_TEST_LYCHEE", "") or shutil.which("lychee")
    if not found:
        pytest.skip(
            "needs lychee: name a binary in DQ_TEST_LYCHEE, or put one on the path"
        )
    return found


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def flaky_server(times):
    """Return a local server that answers 503 ``times`` times, then 200."""

    class Handler(BaseHTTPRequestHandler):
        hits = 0

        def do_GET(self):
            Handler.hits += 1
            self.send_response(503 if Handler.hits <= times else 200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *arguments):
            pass

    server = HTTPServer(("127.0.0.1", free_port()), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


@pytest.mark.parametrize(
    ("times", "expected"), [(1, 0), (50, 1)], ids=["recovers", "stays-down"]
)
def test_against_the_real_checker(tmp_path, capsys, times, expected):
    server = flaky_server(times)
    try:
        page = tmp_path / "page.md"
        page.write_text(f"[flaky](http://127.0.0.1:{server.server_port}/flaky)\n")
        assert (
            check.main(
                ["--attempts", "3", "--wait", "1", "--", lychee_binary(), str(page)]
            )
            == expected
        )
    finally:
        server.shutdown()
        server.server_close()
    assert "Rejected status code: 503" in capsys.readouterr().out
