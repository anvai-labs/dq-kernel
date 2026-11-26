# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from dq import cli
from dq.exceptions import ConfigurationError, EngineNotFoundError
from dq.gate import EXIT_CONFIGURATION, EXIT_GATE_FAILED, EXIT_PASSED


def run_cli(arguments, *, valid=True, results=None, policy="any", run_error=None):
    spark = MagicMock()
    spark.sparkContext.applicationId = "local-test"
    spark.version = "3.5.9"
    builder = MagicMock()
    builder.master.return_value.appName.return_value.getOrCreate.return_value = spark

    with (
        patch.object(sys, "argv", ["dq-kernel", *arguments]),
        patch("pyspark.sql.SparkSession") as spark_session,
        patch("dq.validate.validate_config", return_value=valid),
        patch("dq.dq_framework.DQFramework") as framework,
    ):
        spark_session.builder = builder
        framework.return_value.gate_policy = policy
        if run_error is not None:
            framework.return_value.run.side_effect = run_error
        else:
            framework.return_value.run.return_value = results or []
        return cli.main(), builder, framework


def passed(check="unique"):
    return {"check": check, "success": True}


def warn_failure(check="stale_dimension"):
    return {"check": check, "success": False, "details": {"criticality": "warn"}}


def test_cli_rejects_invalid_config_before_starting_spark(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("invalid\n", encoding="utf-8")

    exit_code, builder, framework = run_cli([str(config)], valid=False)

    assert exit_code == EXIT_CONFIGURATION
    builder.master.assert_not_called()
    framework.assert_not_called()


def test_cli_rejects_zero_outcomes(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework {}\n", encoding="utf-8")

    exit_code, _, _ = run_cli([str(config)], results=[])

    assert exit_code == 1


def test_cli_defers_remote_config_loading_to_framework():
    exit_code, builder, framework = run_cli(
        ["s3://bucket/rules.conf"], results=[{"check": "unique", "success": True}]
    )

    assert exit_code == 0
    builder.master.assert_called_once()
    framework.assert_called_once()


def test_cli_writes_failed_evidence_and_returns_failure(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework { dqrules = [1] }\n", encoding="utf-8")
    destination = tmp_path / "report.json"

    exit_code, _, _ = run_cli(
        [
            str(config),
            "--report-json",
            str(destination),
            "--dataset-id",
            "bars:spy:2026q3",
            "--dataset-sha256",
            "a" * 64,
        ],
        results=[{"check": "coverage", "success": False}],
    )

    assert exit_code == 1
    evidence = json.loads(destination.read_text(encoding="utf-8"))
    assert evidence["status"] == "failed"
    assert evidence["dataset"]["id"] == "bars:spy:2026q3"
    assert evidence["summary"]["failed"] == 1


# ------------------------------------------------- gate policy and exit codes


def test_cli_exit_code_is_configuration_error_for_invalid_config(tmp_path):
    """Exit 2 means "fix the configuration", not "look at the data"."""
    config = tmp_path / "rules.conf"
    config.write_text("invalid\n", encoding="utf-8")
    exit_code, _, _ = run_cli([str(config)], valid=False)
    assert exit_code == EXIT_CONFIGURATION


@pytest.mark.parametrize(
    ("policy", "results", "expected"),
    [
        ("any", [passed(), warn_failure()], EXIT_GATE_FAILED),
        ("error", [passed(), warn_failure()], EXIT_PASSED),
        ("any", [passed(), {"check": "broken", "success": False}], EXIT_GATE_FAILED),
        ("error", [passed(), {"check": "broken", "success": False}], EXIT_GATE_FAILED),
        ("never", [{"check": "broken", "success": False}], EXIT_PASSED),
        ("any", [passed()], EXIT_PASSED),
    ],
)
def test_cli_exit_code_follows_the_gate_policy(tmp_path, policy, results, expected):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework {}\n", encoding="utf-8")
    exit_code, _, framework = run_cli([str(config)], results=results, policy=policy)
    assert exit_code == expected
    framework.assert_called_once()


def test_cli_passes_the_report_reference_to_the_framework(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework {}\n", encoding="utf-8")
    destination = tmp_path / "report.json"
    exit_code, _, framework = run_cli(
        [
            str(config),
            "--report-json",
            str(destination),
            "--dataset-id",
            "bars:spy:2026q3",
            "--dataset-sha256",
            "a" * 64,
        ],
        results=[passed()],
    )
    assert exit_code == EXIT_PASSED
    assert framework.call_args.kwargs["report_reference"] == str(destination)


@pytest.mark.parametrize(
    "error",
    [
        ConfigurationError("Rule 0: unknown constraint"),
        EngineNotFoundError("Engine module 'x_engine' not found"),
    ],
    ids=["configuration", "unknown-engine"],
)
def test_cli_configuration_errors_exit_two(tmp_path, error):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework {}\n", encoding="utf-8")
    exit_code, _, _ = run_cli([str(config)], run_error=error)
    assert exit_code == EXIT_CONFIGURATION


def test_cli_unrelated_errors_still_exit_one(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework {}\n", encoding="utf-8")
    exit_code, _, _ = run_cli([str(config)], run_error=RuntimeError("executor lost"))
    assert exit_code == EXIT_GATE_FAILED


def test_cli_requires_complete_evidence_identity(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework {}\n", encoding="utf-8")

    with patch.object(
        sys,
        "argv",
        ["dq-kernel", str(config), "--report-json", str(tmp_path / "out.json")],
    ):
        with pytest.raises(SystemExit) as error:
            cli.main()

    assert error.value.code == 2


def test_cli_rejects_dataset_identity_without_report(tmp_path):
    config = tmp_path / "rules.conf"
    config.write_text("dqframework {}\n", encoding="utf-8")

    with patch.object(
        sys,
        "argv",
        ["dq-kernel", str(config), "--dataset-id", "bars:spy:2026q3"],
    ):
        with pytest.raises(SystemExit) as error:
            cli.main()

    assert error.value.code == 2
