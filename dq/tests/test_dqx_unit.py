# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""JVM-free, DQX-free contract tests for the dqx engine.

The databricks.labs.dqx modules are faked in sys.modules (test_dqdl-style
vendor mock) so the engine contract -- check building, naming, fail-closed
validation, client-mode wiring, failure-to-metric join -- runs in the fast
unit lane without the optional extra installed. Live DQX round trips live
in test_dqx.py (spark-marked, importorskip).
"""

import sys
import types
from types import SimpleNamespace

import pytest

from pyhocon import ConfigFactory

from dq.engine.dqx.dqx_check import DqxCheck
from dq.engine.dqx.dqx_engine import DqxEngine, _StubWorkspaceClient

# ---------------------------------------------------------------- check builder


def check_config(checks):
    return ConfigFactory.from_dict(
        {"name": "dqx-test", "engine": "dqx", "checks": checks}
    )


def test_build_checks_requires_checks_payload():
    with pytest.raises(ValueError, match="requires a 'checks' list"):
        DqxCheck(ConfigFactory.from_dict({"name": "no-checks"})).build_checks()
    with pytest.raises(ValueError, match="requires a 'checks' list"):
        DqxCheck(check_config([])).build_checks()


def test_build_checks_rejects_missing_check_block():
    with pytest.raises(ValueError, match="'check' block"):
        DqxCheck(check_config([{"criticality": "error"}])).build_checks()


def test_build_checks_rejects_missing_function():
    with pytest.raises(ValueError, match="check.function"):
        DqxCheck(
            check_config([{"check": {"arguments": {"column": "id"}}}])
        ).build_checks()


def test_build_checks_defaults_criticality_and_injects_names():
    checks = DqxCheck(
        check_config(
            [
                {"check": {"function": "is_not_null", "arguments": {"column": "id"}}},
                {
                    "check": {
                        "function": "is_unique",
                        "for_each_column": ["a", "b"],
                    }
                },
            ]
        )
    ).build_checks()
    assert checks[0]["name"] == "is_not_null_0"
    assert checks[0]["criticality"] == "error"
    assert checks[1]["name"] == "is_unique_1"
    # for_each_column passes through untouched for DQX to expand.
    assert checks[1]["check"]["for_each_column"] == ["a", "b"]
    assert "arguments" not in checks[1]["check"]


def test_build_checks_preserves_explicit_names_and_criticality():
    checks = DqxCheck(
        check_config(
            [
                {
                    "name": "emails_present",
                    "criticality": "warn",
                    "check": {
                        "function": "is_not_null",
                        "arguments": {"column": "email"},
                    },
                }
            ]
        )
    ).build_checks()
    assert checks[0]["name"] == "emails_present"
    assert checks[0]["criticality"] == "warn"


def test_build_checks_rejects_invalid_criticality():
    with pytest.raises(ValueError, match="criticality"):
        DqxCheck(
            check_config(
                [
                    {
                        "criticality": "fatal",
                        "check": {
                            "function": "is_not_null",
                            "arguments": {"column": "id"},
                        },
                    }
                ]
            )
        ).build_checks()


def test_build_checks_rejects_duplicate_names():
    with pytest.raises(ValueError, match="Duplicate check label .dup.: checks 0 and 1"):
        DqxCheck(
            check_config(
                [
                    {
                        "name": "dup",
                        "check": {
                            "function": "is_not_null",
                            "arguments": {"column": "id"},
                        },
                    },
                    {
                        "name": "dup",
                        "check": {
                            "function": "is_unique",
                            "arguments": {"columns": ["id"]},
                        },
                    },
                ]
            )
        ).build_checks()


def test_build_checks_converts_hocon_trees():
    checks = DqxCheck(
        check_config(
            [
                {
                    "name": "emails_match",
                    "check": {
                        "function": "regex_match",
                        "arguments": {"column": "email", "regex": ".+@.+"},
                    },
                }
            ]
        )
    ).build_checks()
    assert checks[0]["check"]["arguments"] == {"column": "email", "regex": ".+@.+"}


# ------------------------------------------------------------- failure join (pure)


def make_engine_with_index(checks):
    engine = DqxEngine(check_config(checks))
    engine._checks = checks
    engine._check_index = {
        check["name"]: (
            check["check"]["function"],
            check.get("criticality", "error"),
        )
        for check in checks
    }
    return engine


def test_metrics_report_success_without_failure_rows():
    engine = make_engine_with_index(
        [{"name": "ok_check", "check": {"function": "is_not_null", "arguments": {}}}]
    )
    metrics = engine._metrics_from_failures([])
    assert len(metrics) == 1
    assert metrics[0] == {
        "check": "ok_check",
        "success": True,
        "details": {
            "function": "is_not_null",
            "criticality": "error",
            "failed_rows": 0,
            "sample_messages": [],
        },
    }


def test_metrics_join_failures_by_name_and_criticality():
    engine = make_engine_with_index(
        [
            {"name": "a", "criticality": "error", "check": {"function": "f_a"}},
            {"name": "b", "criticality": "warn", "check": {"function": "f_b"}},
        ]
    )
    failures = [
        {
            "name": "b",
            "criticality": "warn",
            "function": "f_b",
            "failed_rows": 3,
            "sample_messages": ["zeta", "alpha"],
        }
    ]
    metrics = engine._metrics_from_failures(failures)
    assert [metric["check"] for metric in metrics] == ["a", "b"]
    assert metrics[0]["success"] is True
    assert metrics[1]["success"] is False
    assert metrics[1]["details"]["failed_rows"] == 3
    assert metrics[1]["details"]["sample_messages"] == ["alpha", "zeta"]


def test_metrics_truncate_and_type_cast_failure_details():
    engine = make_engine_with_index([{"name": "a", "check": {"function": "f_a"}}])
    failures = [
        {
            "name": "a",
            "criticality": "error",
            "function": None,  # falls back to the configured function
            "failed_rows": "7",  # Spark counts arrive as strings in some paths
            "sample_messages": ["m" * 2048],
        }
    ]
    metric = engine._metrics_from_failures(failures)[0]
    assert metric["details"]["function"] == "f_a"
    assert metric["details"]["failed_rows"] == 7
    assert len(metric["details"]["sample_messages"][0]) == 1024


# ------------------------------------------------------------------ engine wiring


class FakeDQXEngine:
    """Records constructor wiring without importing the real DQX."""

    instances = []

    def __init__(self, workspace_client, spark=None):
        self.workspace_client = workspace_client
        self.spark = spark
        FakeDQXEngine.instances.append(self)

    validate_checks = staticmethod(
        lambda checks: SimpleNamespace(has_errors=False, errors=[])
    )

    apply_checks_by_metadata = staticmethod(lambda df, checks: df)


@pytest.fixture
def fake_dqx(monkeypatch):
    """Install fake databricks.labs.dqx modules and reset the recording class."""
    FakeDQXEngine.instances = []
    pkg = types.ModuleType("databricks.labs.dqx")
    engine_mod = types.ModuleType("databricks.labs.dqx.engine")
    engine_mod.DQEngine = FakeDQXEngine
    version_mod = types.ModuleType("databricks.labs.dqx.__version__")
    version_mod.__version__ = "0.17.2-fake"
    reporting_mod = types.ModuleType("databricks.labs.dqx.reporting_columns")

    reporting_mod.DefaultColumnNames = SimpleNamespace(
        ERRORS=SimpleNamespace(value="_errors"),
        WARNINGS=SimpleNamespace(value="_warnings"),
    )

    monkeypatch.setitem(sys.modules, "databricks.labs.dqx", pkg)
    monkeypatch.setitem(sys.modules, "databricks.labs.dqx.engine", engine_mod)
    monkeypatch.setitem(sys.modules, "databricks.labs.dqx.__version__", version_mod)
    monkeypatch.setitem(
        sys.modules, "databricks.labs.dqx.reporting_columns", reporting_mod
    )
    return FakeDQXEngine


def fresh_frame():
    return SimpleNamespace(sparkSession="fake-session")


def test_import_error_carries_install_hint(monkeypatch):
    # Force the extra to be absent: in CI's main lane DQX is installed, and
    # without this sentinel the engine would go on to real DQX validation.
    monkeypatch.setitem(sys.modules, "databricks.labs.dqx.engine", None)
    engine = DqxEngine(check_config([{"check": {"function": "is_not_null"}}]))
    with pytest.raises(ImportError, match="databricks-labs-dqx"):
        engine.apply(fresh_frame())


def test_stub_client_mode_is_default(fake_dqx):
    engine = DqxEngine(check_config([{"check": {"function": "is_not_null"}}]))
    engine._import_dqx()
    engine._build_dqx_engine(fresh_frame())
    wired = fake_dqx.instances[-1]
    client = wired.workspace_client
    assert isinstance(client, _StubWorkspaceClient)
    # Exactly what DQEngineBase._verify_workspace_client touches:
    assert client.config._product_info == ("dqx", "0.17.2-fake")
    assert client.clusters.select_spark_version() == "kernel-stub-local"
    assert wired.spark == "fake-session"


def test_workspace_client_mode_builds_real_client(fake_dqx, monkeypatch):
    calls = []

    class FakeWorkspaceClient:
        def __init__(self, profile=None):
            calls.append(profile)
            self.config = SimpleNamespace(_product_info=("other", "1.0"))
            self.clusters = SimpleNamespace(select_spark_version=lambda: "15.4")

    monkeypatch.setitem(
        sys.modules,
        "databricks.sdk",
        types.SimpleNamespace(WorkspaceClient=FakeWorkspaceClient),
    )
    engine = DqxEngine(
        ConfigFactory.from_dict(
            {
                "name": "dqx-test",
                "checks": [{"check": {"function": "is_not_null"}}],
                "client.mode": "workspace",
                "client.profile": "staging",
            }
        )
    )
    engine._import_dqx()
    engine._build_dqx_engine(fresh_frame())
    assert calls == ["staging"]


def test_unknown_client_mode_fails_closed(fake_dqx):
    engine = DqxEngine(
        ConfigFactory.from_dict(
            {
                "name": "dqx-test",
                "checks": [{"check": {"function": "is_not_null"}}],
                "client.mode": "bogus",
            }
        )
    )
    engine._import_dqx()
    with pytest.raises(ValueError, match="Unsupported dqx client mode"):
        engine._build_dqx_engine(fresh_frame())


class _ValidatingFake(FakeDQXEngine):
    def __init__(self, *_args, has_errors=False, errors=None):
        self._status = SimpleNamespace(has_errors=has_errors, errors=errors or [])

    def validate_checks(self, checks):
        return self._status


def test_dqx_validation_fail_closed(fake_dqx):
    engine = DqxEngine(check_config([{"check": {"function": "is_not_null"}}]))
    engine._import_dqx()
    engine._dqx_engine = _ValidatingFake(
        has_errors=True, errors=["bad function 'nope'"]
    )
    with pytest.raises(ValueError, match="bad function"):
        engine._build_checks()


def test_dqx_validation_pass_builds_index(fake_dqx):
    engine = DqxEngine(
        check_config(
            [
                {
                    "name": "a",
                    "criticality": "warn",
                    "check": {"function": "f_a", "arguments": {"column": "x"}},
                }
            ]
        )
    )
    engine._import_dqx()
    engine._dqx_engine = _ValidatingFake()
    engine._build_checks()
    assert engine._check_index == {"a": ("f_a", "warn")}


def test_an_explicit_name_equal_to_a_derived_label_is_refused():
    """`is_not_null_1` is what the second check would derive; naming the
    first one that would make the two indistinguishable."""
    config = check_config(
        [
            {"name": "is_not_null_1", "check": {"function": "is_not_null"}},
            {"check": {"function": "is_not_null"}},
        ]
    )
    with pytest.raises(ValueError, match="Duplicate check label 'is_not_null_1'"):
        DqxCheck(config).build_checks()
