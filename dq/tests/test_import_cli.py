# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""``dq-import`` (SPEC-013 G4).

A valid ``dq-report/v1`` from each importer -- the very report the canonical
builder writes -- and SPEC-012's exit codes, each for its own reason: a
pipeline must be able to tell a rejected source from a failed dataset.
"""

import hashlib
import json
import logging
import subprocess
import sys
import tomllib
from datetime import datetime
from pathlib import Path

import pytest

from dq.gate import EXIT_CONFIGURATION, EXIT_GATE_FAILED, EXIT_PASSED
from dq.harvest import harvest
from dq.harvest import cli
from dq.report import REPORT_SCHEMA_VERSION, build_report
from dq.tests.harvest_support import (
    COLUMNAR_JSON_EXAMPLE,
    CSV_EXAMPLE,
    DBT_EXAMPLE,
    EVERY_EXAMPLE,
    GX_EXAMPLE,
)
from dq.tests.test_scaffold import unmeasured_environment

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DATASET_SHA = "b" * 64

#: Each example with the identifier of the run that produced it.
RUN_OF = {
    DBT_EXAMPLE: lambda: json.loads(DBT_EXAMPLE.read_text())["metadata"][
        "invocation_id"
    ],
    GX_EXAMPLE: lambda: json.loads(GX_EXAMPLE.read_text())["meta"]["validation_id"],
    CSV_EXAMPLE: lambda: f"import-{hashlib.sha256(CSV_EXAMPLE.read_bytes()).hexdigest()[:16]}",
    COLUMNAR_JSON_EXAMPLE: lambda: (
        f"import-{hashlib.sha256(COLUMNAR_JSON_EXAMPLE.read_bytes()).hexdigest()[:16]}"
    ),
}


def by_name(value):
    return getattr(value, "name", value)


def run(capsys, *arguments):
    code = cli.main([str(argument) for argument in arguments])
    return code, capsys.readouterr()


def written(tmp_path, capsys, source, *more):
    """Run ``dq-import`` on ``source`` for a report; return ``(code, output, report)``."""
    report_path = tmp_path / "evidence" / "report.json"
    code, captured = run(
        capsys,
        source,
        "--dataset-id",
        "orders",
        "--dataset-sha256",
        DATASET_SHA,
        "--report-json",
        report_path,
        *more,
    )
    return code, captured, json.loads(report_path.read_text(encoding="utf-8"))


# ------------------------------------------------------------- the report


@pytest.mark.parametrize("example", EVERY_EXAMPLE, ids=by_name)
def test_each_importer_writes_the_report_the_builder_writes(tmp_path, capsys, example):
    source = example.read_bytes()
    code, captured, report = written(tmp_path, capsys, example)
    assert code == EXIT_GATE_FAILED  # every example holds a failed check

    outcomes = harvest(source, reference=str(example))
    digest = hashlib.sha256(source).hexdigest()
    assert report == build_report(
        results=outcomes,
        config_reference=str(example),
        config_sha256=digest,
        dataset_id="orders",
        dataset_sha256=DATASET_SHA,
        application_id=RUN_OF[example](),
        spark_version=None,
        generated_at=datetime.fromisoformat(
            report["generated_at"].replace("Z", "+00:00")
        ),
    )
    assert report["schema_version"] == REPORT_SCHEMA_VERSION
    assert report["rule_set"] == {"reference": str(example), "sha256": digest}
    assert report["dataset"] == {"id": "orders", "sha256": DATASET_SHA}
    assert report["runtime"]["spark"] is None
    assert report["checks"] == outcomes
    assert (
        report["summary"]["total"]
        == len(outcomes)
        == report["summary"]["passed"] + report["summary"]["failed"]
    )
    printed = captured.out
    assert f"Total checks: {len(outcomes)}" in printed
    assert (
        f"Source: {report['checks'][0]['details']['origin']['system']} {example}"
        in printed
    )


@pytest.mark.parametrize("example", (CSV_EXAMPLE, COLUMNAR_JSON_EXAMPLE), ids=by_name)
def test_a_columnar_source_is_identified_by_its_own_digest(tmp_path, capsys, example):
    _, _, report = written(tmp_path, capsys, example)
    assert report["runtime"]["application_id"] == RUN_OF[example]()


def test_the_reference_is_written_as_it_was_given(tmp_path, capsys):
    source = tmp_path / "checks.json"
    source.write_text('[{"check": "a", "success": true}]', encoding="utf-8")
    _, _, report = written(tmp_path, capsys, f"file://{source}")
    assert report["rule_set"]["reference"] == f"file://{source}"


def test_without_a_report_nothing_is_written_but_the_gate_still_holds(tmp_path, capsys):
    before = sorted(tmp_path.iterdir())
    code, captured = run(capsys, DBT_EXAMPLE)
    assert code == EXIT_GATE_FAILED
    assert sorted(tmp_path.iterdir()) == before
    assert "Total checks: 6" in captured.out and "Failed: 5" in captured.out


# ------------------------------------------------------------ exit codes


def test_a_passing_source_exits_0(tmp_path, capsys):
    source = tmp_path / "ok.json"
    source.write_text('[{"check": "a", "success": true}]', encoding="utf-8")
    code, captured = run(capsys, source)
    assert code == EXIT_PASSED
    assert "Failed: 0 (0 blocking, 0 advisory)" in captured.out


def test_a_failing_source_exits_1_and_names_what_blocks(tmp_path, capsys):
    source = tmp_path / "bad.json"
    source.write_text(
        '[{"check": "a", "success": false},'
        ' {"check": "w", "success": false, "criticality": "warn"}]',
        encoding="utf-8",
    )
    code, captured = run(capsys, source)
    assert code == EXIT_GATE_FAILED
    # Under the default policy every failed check blocks, a warning included.
    assert "Failed: 2 (2 blocking, 0 advisory)" in captured.out
    assert "Blocking failed checks:\n  - a\n  - w" in captured.out
    assert "Advisory" not in captured.out


def test_a_rejected_source_exits_2(tmp_path, capsys, caplog):
    source = tmp_path / "bad.json"
    source.write_text('[{"check": "x", "success": "yes"}]', encoding="utf-8")
    with caplog.at_level(logging.ERROR):
        code, captured = run(capsys, source)
    assert code == EXIT_CONFIGURATION
    assert f"Cannot import {source}: columnar source" in caplog.text
    assert "record 0: 'x' has success 'yes', which is not true or false" in caplog.text
    assert "Total checks" not in captured.out  # nothing was imported


@pytest.mark.parametrize(
    "reference",
    [
        "missing.json",
        "https://example.test/run_results.json",
        "s3://bucket/run_results.json",
    ],
)
def test_a_source_that_cannot_be_read_exits_2(tmp_path, capsys, caplog, reference):
    with caplog.at_level(logging.ERROR):
        code, captured = run(capsys, reference)
    assert code == EXIT_CONFIGURATION
    assert f"Cannot import {reference}" in caplog.text


def test_a_directory_is_not_a_source(tmp_path, capsys, caplog):
    with caplog.at_level(logging.ERROR):
        code, _ = run(capsys, tmp_path)
    assert code == EXIT_CONFIGURATION
    assert f"Cannot import {tmp_path}" in caplog.text


def test_a_malformed_dataset_digest_exits_2(tmp_path, capsys, caplog):
    report_path = tmp_path / "report.json"
    with caplog.at_level(logging.ERROR):
        code, _ = run(
            capsys,
            CSV_EXAMPLE,
            "--dataset-id",
            "d",
            "--dataset-sha256",
            "nope",
            "--report-json",
            report_path,
        )
    assert code == EXIT_CONFIGURATION
    assert "dataset_sha256 must be a 64-character SHA-256 digest" in caplog.text
    assert not report_path.exists()


def test_a_report_needs_the_dataset(tmp_path, capsys):
    with pytest.raises(SystemExit) as stopped:
        cli.main(["x.json", "--report-json", "r.json"])
    assert stopped.value.code == EXIT_CONFIGURATION
    assert (
        "dq-import: error: --report-json requires --dataset-id and --dataset-sha256"
        in capsys.readouterr().err
    )


def test_the_dataset_needs_a_report(capsys):
    with pytest.raises(SystemExit) as stopped:
        cli.main(["x.json", "--dataset-id", "d", "--dataset-sha256", DATASET_SHA])
    assert stopped.value.code == EXIT_CONFIGURATION
    assert (
        "dq-import: error: dataset evidence options require --report-json"
        in capsys.readouterr().err
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["x.json", "--system", "soda"],
        ["x.json", "--fail-on", "sometimes"],
        [],  # no source
    ],
)
def test_an_unknown_option_value_exits_2_with_a_usage_message(capsys, arguments):
    with pytest.raises(SystemExit) as stopped:
        cli.main(arguments)
    assert stopped.value.code == EXIT_CONFIGURATION
    assert "usage: dq-import" in capsys.readouterr().err


# --------------------------------------------------------------- the options


def test_the_gate_policy_is_a_flag(tmp_path, capsys):
    source = tmp_path / "warnings.json"
    source.write_text(
        '[{"check": "w", "success": false, "criticality": "warn"}]', encoding="utf-8"
    )
    assert run(capsys, source)[0] == EXIT_GATE_FAILED
    code, captured = run(capsys, source, "--fail-on", "error")
    assert code == EXIT_PASSED
    assert "Failed: 1 (0 blocking, 1 advisory)" in captured.out
    code, captured = run(capsys, DBT_EXAMPLE, "--fail-on", "never")
    assert code == EXIT_PASSED
    assert "Failed: 5 (0 blocking, 5 advisory)" in captured.out


def test_the_system_can_be_stated(tmp_path, capsys, caplog):
    code, _ = run(capsys, DBT_EXAMPLE, "--system", "dbt", "--fail-on", "never")
    assert code == EXIT_PASSED
    with caplog.at_level(logging.ERROR):
        code, _ = run(capsys, GX_EXAMPLE, "--system", "dbt")
    assert code == EXIT_CONFIGURATION
    assert "is not a dbt run_results.json" in caplog.text


def test_a_stated_system_gets_past_detection(tmp_path, capsys, caplog):
    """A file nothing recognises says why it is refused once its system is stated."""
    source = tmp_path / "checks.csv"
    source.write_text("check,criticality\na,error\n", encoding="utf-8")
    with caplog.at_level(logging.ERROR):
        code, _ = run(capsys, source)
        assert code == EXIT_CONFIGURATION and "not recognised" in caplog.text
        code, _ = run(capsys, source, "--system", "columnar")
    assert code == EXIT_CONFIGURATION
    assert "the header has no 'success' column" in caplog.text


def test_help_lists_the_systems_the_policies_and_the_exit_codes(capsys):
    with pytest.raises(SystemExit) as stopped:
        cli.main(["--help"])
    assert stopped.value.code == EXIT_PASSED
    text = capsys.readouterr().out
    assert "--system {columnar,dbt,greatexpectations}" in text
    assert "--fail-on {any,error,never}" in text
    assert "2  the source was rejected" in text


# --------------------------------------------------------- as a command


def test_the_command_is_registered_as_a_console_script():
    scripts = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text())["tool"][
        "poetry"
    ]["scripts"]
    assert scripts["dq-import"] == "dq.harvest.cli:main"


def test_the_command_runs_as_a_module():
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dq.harvest.cli",
            str(DBT_EXAMPLE),
            "--fail-on",
            "never",
        ],
        cwd=REPOSITORY_ROOT,
        env=unmeasured_environment(),
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert result.returncode == EXIT_PASSED, result.stderr
    assert "Total checks: 6" in result.stdout
