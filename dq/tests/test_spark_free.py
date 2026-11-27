# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""No Spark: what must work without it (SPEC-015, gates G4 and G5).

Checked in child processes, where an import can be made to fail. What runs
in this process would prove nothing: the test session may hold Spark loaded
already, and a module that imports Spark lazily would never notice.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "examples" / "arrow_rule.conf"
REPORT_KEYS = {
    "schema_version",
    "generated_at",
    "status",
    "rule_set",
    "dataset",
    "runtime",
    "summary",
    "checks",
}

#: Every package the kernel treats as optional.
EXTRAS = {
    "pyspark",
    "pydeequ",
    "great_expectations",
    "datafusion",
    "databricks",
    "delta",
    "boto3",
    "pyarrow",
    "sqlalchemy",
    "opentelemetry",
    "polars",
    "pandas",
    "pandera",
}


def child_env(**extra):
    """This process's environment for a child, minus what would measure it."""
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("COV_CORE_", "COVERAGE_"))
    }
    env.update(extra)
    return env


def run_python(arguments, env, cwd=None):
    return subprocess.run(
        [sys.executable, *arguments],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=cwd or str(ROOT),
        env=env,
    )


# --------------------------------------------------------------------- G5


def test_the_arrow_adapter_the_runner_and_the_run_core_import_with_every_extra_blocked():
    code = f"""
import sys

BLOCKED = {sorted(EXTRAS)!r}


class Block:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in BLOCKED:
            raise AssertionError(fullname + " was imported")

sys.meta_path.insert(0, Block())
import dq.arrow_adapter
import dq.compute_support
import dq.runner
import dq.run

loaded = [name for name in sys.modules if name.split(".")[0] in BLOCKED]
assert not loaded, loaded
assert callable(dq.arrow_adapter.execute_plan)
assert callable(dq.run.run_rules)
assert dq.runner.ADAPTER == "arrow" and dq.runner.ENGINE == "portable"
assert sorted(dq.compute_support.ADAPTER_MODULES) == ["arrow", "spark", "sql"]
print("ok")
"""
    result = run_python(["-c", code], child_env())
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_the_blocker_really_blocks_the_control_imports_one():
    """A blocker that blocks nothing would pass the test above."""
    code = f"""
import sys


class Block:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {sorted(EXTRAS)!r}:
            raise AssertionError(fullname + " was imported")

sys.meta_path.insert(0, Block())
try:
    import pyarrow  # noqa: F401
except AssertionError:
    print("blocked")
"""
    result = run_python(["-c", code], child_env())
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "blocked"


# --------------------------------------------------------------------- G4


def spark_cannot_be_imported(directory):
    """A directory to put first on PYTHONPATH where importing an extra fails."""
    for name in ("pyspark", "pydeequ", "great_expectations"):
        package = directory / name
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(
            f'raise ImportError("{name} cannot be imported in this process")\n'
        )
    return str(directory)


def command_for(script):
    """The installed ``dq-run`` command, or the module run as one."""
    found = shutil.which(script)
    return [found] if found else [sys.executable, "-m", "dq.runner"]


def write_orders(path):
    import decimal

    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.table(
            {
                "id": ["a", None, "c", "d"],
                "customer_id": ["c1", "c1", "c2", "c2"],
                "amount": pa.array(
                    [
                        decimal.Decimal("1.25"),
                        None,
                        decimal.Decimal("3.50"),
                        decimal.Decimal("20.00"),
                    ],
                    pa.decimal128(10, 2),
                ),
            }
        ),
        path,
    )


def test_dq_run_completes_a_run_and_writes_a_valid_report_where_spark_cannot_be_imported(
    tmp_path,
):
    pytest.importorskip("pyarrow", reason="the runner needs the arrow extra")
    blocked = spark_cannot_be_imported(tmp_path / "blocked")
    env = child_env(PYTHONPATH=blocked)

    # The control: in this environment, importing Spark fails.
    control = run_python(["-c", "import pyspark"], env)
    assert control.returncode != 0
    assert "pyspark cannot be imported in this process" in control.stderr

    orders = tmp_path / "orders.parquet"
    write_orders(orders)
    report = tmp_path / "evidence" / "report.json"
    result = subprocess.run(
        [
            *command_for("dq-run"),
            str(EXAMPLE),
            "--dataset",
            f"orders={orders}",
            "--report",
            str(report),
        ],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(tmp_path),
        env=env,
    )
    assert result.returncode == 1, (result.stdout, result.stderr)  # one check fails
    assert "Missing dependency" not in result.stderr
    assert "The gate failed." in result.stdout

    document = json.loads(report.read_text())
    assert set(document) == REPORT_KEYS
    assert document["schema_version"] == "dq-report/v1"
    assert document["runtime"]["spark"] is None
    assert document["runtime"]["application_id"] == "dq-run"
    assert document["rule_set"] == {
        "reference": str(EXAMPLE),
        "sha256": hashlib.sha256(EXAMPLE.read_bytes()).hexdigest(),
    }
    checks = document["checks"]
    assert [check["check"] for check in checks] == [
        "order_id_complete",
        "orders_present",
        "orders_per_customer.id.min",
        "amount_range.high",
        "amount_range.low",
    ]
    assert {check["jobid"] for check in checks} == {"dq-run"}
    assert {check["details"]["adapter"]["name"] for check in checks} == {"arrow"}
    summary = document["summary"]
    assert (summary["total"], summary["passed"], summary["failed"]) == (5, 4, 1)
    assert summary["blocked"] + summary["warned"] == summary["failed"]
    assert document["status"] == "failed"


def test_dq_run_passes_a_clean_file_where_spark_cannot_be_imported(tmp_path):
    """Every check passes on a file where each customer has two ids."""
    pytest.importorskip("pyarrow", reason="the runner needs the arrow extra")
    import pyarrow as pa
    import pyarrow.parquet as pq

    env = child_env(PYTHONPATH=spark_cannot_be_imported(tmp_path / "blocked"))
    orders = tmp_path / "orders.parquet"
    pq.write_table(
        pa.table(
            {
                "id": ["a", "b"],
                "customer_id": ["c1", "c1"],
                "amount": pa.array([1, 2], pa.decimal128(10, 2)),
            }
        ),
        orders,
    )
    report = tmp_path / "report.json"
    result = subprocess.run(
        [
            *command_for("dq-run"),
            str(EXAMPLE),
            "--dataset",
            f"orders={orders}",
            "--report",
            str(report),
        ],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(tmp_path),
        env=env,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert "The gate passed." in result.stdout
    assert json.loads(report.read_text())["status"] == "passed"


# ------------------------------------------ the Spark command, without Spark


def dq_kernel_without_spark(arguments, env):
    """Run the ``dq-kernel`` command where Spark cannot be imported."""
    return run_python(
        ["-c", "from dq.cli import main; raise SystemExit(main())", *arguments], env
    )


def test_dq_kernel_names_the_extra_and_exits_two_where_spark_cannot_be_imported(
    tmp_path,
):
    """What its help promises: exit 2 for a missing dependency. The handler
    needs its exit codes while the import it handles is failing, so they
    cannot be bound inside the same ``try``: with none bound it crashed."""
    env = child_env(PYTHONPATH=spark_cannot_be_imported(tmp_path / "blocked"))
    result = dq_kernel_without_spark(
        [str(ROOT / "examples" / "basic_deequ_validation.conf")], env
    )
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert "Traceback" not in result.stderr
    assert "pip install dq-kernel[spark]" in result.stderr


def test_dq_kernel_judges_the_configuration_before_it_looks_for_spark(tmp_path):
    """One verdict with or without Spark installed: an invalid rule pack is
    reported as such -- what dq-validate says too -- not as a missing
    dependency."""
    env = child_env(PYTHONPATH=spark_cannot_be_imported(tmp_path / "blocked"))
    absent = tmp_path / "absent.conf"
    result = dq_kernel_without_spark([str(absent)], env)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert "Configuration validation failed" in result.stderr
    assert "Install Spark support" not in result.stderr
