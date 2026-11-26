# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest

from dq.dq_framework import DQFramework
from dq.exceptions import ConfigurationError, DataFrameNotFoundError, ValidationError
from dq.notifier import InMemoryNotifier


def framework_with_rules(rules, notifier=None, **extra_config):
    framework = DQFramework.__new__(DQFramework)
    framework._config = {"dqframework.dqrules": rules, **extra_config}
    framework._spark = MagicMock()
    framework._spark.sparkContext.applicationId = "local-test"
    framework.get_dataframe = MagicMock(return_value=MagicMock())
    framework._notifier = notifier
    framework._report_reference = None
    framework.last_gate_decision = None
    return framework


def engine_returning(loader_patch, outcomes):
    """Patch EngineLoader so every rule gets an engine yielding ``outcomes``."""
    engine = MagicMock()
    engine.apply.return_value = outcomes
    loader_patch.return_value.load_engine.return_value = engine
    return engine


def test_library_rejects_empty_rule_set():
    framework = framework_with_rules([])
    with pytest.raises(ConfigurationError, match="No dqrules"):
        framework.run()


def test_library_rejects_rule_without_engine():
    framework = framework_with_rules([{"checks": [{"constraint": "isComplete"}]}])
    with pytest.raises(ConfigurationError, match="missing required key 'engine'"):
        framework.run()


@pytest.mark.parametrize(
    "metrics, message",
    [
        ([], "zero outcomes"),
        ([{"check": "ambiguous"}], "boolean 'success'"),
        ([{"check": "ambiguous", "success": "yes"}], "boolean 'success'"),
    ],
)
def test_library_rejects_ambiguous_engine_outcomes(metrics, message):
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}]
    )
    engine = MagicMock()
    engine.apply.return_value = metrics

    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        with pytest.raises(ValidationError, match=message):
            framework.run()


def test_library_preserves_explicit_failed_outcome():
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}]
    )
    engine = MagicMock()
    engine.apply.return_value = [{"check": "complete", "success": False}]

    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        results = framework.run()

    assert results[0]["success"] is False
    assert results[0]["jobid"] == "local-test"
    assert isinstance(results[0]["ts"], int)


def test_configured_dataframe_load_failure_is_not_skipped():
    framework = DQFramework.__new__(DQFramework)
    framework._config = {"dqframework.dataframes": {"bars": "market.bars"}}
    framework.default_dataframe = None
    framework._resolve_dataframe = MagicMock(side_effect=OSError("catalog unavailable"))

    with pytest.raises(DataFrameNotFoundError, match="configured DataFrame 'bars'"):
        framework._load_dataframes()


def test_injected_default_takes_precedence_over_configured_default_reference():
    framework = DQFramework.__new__(DQFramework)
    injected = MagicMock()
    framework._config = {
        "dqframework.dataframes": {
            "default": "placeholder.table",
            "bars": "market.bars",
        }
    }
    framework.default_dataframe = injected
    bars = MagicMock()
    framework._resolve_dataframe = MagicMock(return_value=bars)

    loaded = framework._load_dataframes()

    assert loaded == {"default": injected, "bars": bars}
    framework._resolve_dataframe.assert_called_once_with("bars", "market.bars")


def test_framework_does_not_mutate_engine_owned_outcomes():
    framework = framework_with_rules([{"engine": "deequ"}])
    original = {"check": "complete", "success": True, "details": {"count": 1}}
    engine = MagicMock()
    engine.apply.return_value = [original]
    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        results = framework.run()
    results[0]["details"]["count"] = 2
    assert original == {"check": "complete", "success": True, "details": {"count": 1}}


def test_framework_rejects_nonfinite_metrics_even_on_success():
    framework = framework_with_rules([{"engine": "deequ"}])
    engine = MagicMock()
    engine.apply.return_value = [{"success": True, "details": float("nan")}]
    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        with pytest.raises(ValidationError, match="finite"):
            framework.run()


@pytest.mark.parametrize("limit", ["MAX_OUTCOMES", "MAX_BATCH_BYTES"])
def test_framework_bounds_cumulative_results_across_rules(monkeypatch, limit):
    framework = framework_with_rules([{"engine": "deequ"}, {"engine": "deequ"}])
    engine = MagicMock()
    engine.apply.return_value = [{"success": True}]
    monkeypatch.setattr("dq.dq_framework." + limit, 1)
    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        with pytest.raises(ValidationError, match="summary limits"):
            framework.run()


def test_run_emits_gate_event_with_severity_split(caplog):
    import logging as logging_module

    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}]
    )
    engine = MagicMock()
    engine.apply.return_value = [
        {
            "check": "warn_only",
            "success": False,
            "details": {"criticality": "warn"},
        }
    ]
    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        with caplog.at_level(logging_module.INFO, logger="dq.gate_events"):
            results = framework.run()
    assert len(results) == 1 and results[0]["success"] is False
    gate_records = [r for r in caplog.records if "gate_event" in r.getMessage()]
    assert gate_records, "framework run must announce a gate event"
    assert "warned=1" in gate_records[0].getMessage()
    assert "blocked=0" in gate_records[0].getMessage()


def test_run_gate_event_absent_criticality_blocks(caplog):
    import logging as logging_module

    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}]
    )
    engine = MagicMock()
    engine.apply.return_value = [{"check": "legacy_fail", "success": False}]
    with patch("dq.dq_framework.EngineLoader") as loader:
        loader.return_value.load_engine.return_value = engine
        with caplog.at_level(logging_module.INFO, logger="dq.gate_events"):
            framework.run()
    gate_records = [r for r in caplog.records if "gate_event" in r.getMessage()]
    assert gate_records and "blocked=1" in gate_records[0].getMessage()


# ---------------------------------------- gate policy and notifier conformance


def _outcome(check, success, criticality=None):
    details = {"criticality": criticality} if criticality else {}
    return {"check": check, "success": success, "details": details}


@pytest.mark.parametrize(
    ("outcomes", "status"),
    [
        ([_outcome("a", True, "error"), _outcome("b", True, "warn")], "passed"),
        ([_outcome("a", True, "error"), _outcome("b", False, "warn")], "failed"),
    ],
    ids=["passing-run", "failing-run"],
)
def test_recording_notifier_receives_exactly_one_event_per_run(outcomes, status):
    recorder = InMemoryNotifier()
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}],
        notifier=recorder,
    )
    with patch("dq.dq_framework.EngineLoader") as loader:
        engine_returning(loader, outcomes)
        results = framework.run()
    assert len(recorder.events) == 1
    event = recorder.events[0]
    assert event.status == status
    assert event.failed == tuple(o["check"] for o in outcomes if not o["success"])
    assert event.total == len(outcomes)
    assert event.report is None
    assert results == results  # the run returns its outcomes regardless of notification


def test_event_carries_the_configured_report_reference():
    recorder = InMemoryNotifier()
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}],
        notifier=recorder,
    )
    framework._report_reference = "s3://evidence/run-7.json"
    with patch("dq.dq_framework.EngineLoader") as loader:
        engine_returning(loader, [_outcome("a", True, "error")])
        framework.run()
    assert recorder.events[0].report == "s3://evidence/run-7.json"
    assert recorder.events[0].to_dict()["report"] == "s3://evidence/run-7.json"


def test_memory_notifier_is_built_from_config_and_reachable():
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}],
        **{"dqframework.notifiers": [{"type": "memory"}]},
    )
    with patch("dq.dq_framework.EngineLoader") as loader:
        engine_returning(loader, [_outcome("a", True, "error")])
        framework.run()
    assert len(framework._notifier.events) == 1


def test_gate_event_is_emitted_after_the_run_scope_exits():
    """ADR-008: notification never joins the durability transaction."""
    events = []
    recorder = InMemoryNotifier()
    original = recorder.notify

    def notifying(event):
        events.append(scope.state)
        original(event)

    recorder.notify = notifying
    sink = MagicMock()

    class Scope:
        state = "open"

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.state = "committed"

    scope = Scope()
    sink.run_scope.return_value = scope
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}],
        notifier=recorder,
    )
    framework._config = {
        "dqframework.dqrules": framework._config["dqframework.dqrules"],
        "dqframework.repository": {"dataset": "orders"},
    }
    with (
        patch("dq.dq_framework.EngineLoader") as loader,
        patch("dq.dq_framework.sink_from_config", return_value=sink),
    ):
        engine_returning(loader, [_outcome("a", False, "error")])
        framework.run()
    assert events == ["committed"]


class ExplodingNotifier:
    def notify(self, event):
        raise RuntimeError("delivery exploded")


def test_a_raising_notifier_never_alters_the_run():
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}],
        notifier=ExplodingNotifier(),
    )
    with patch("dq.dq_framework.EngineLoader") as loader:
        engine_returning(loader, [_outcome("a", False, "error")])
        results = framework.run()
    assert [r["check"] for r in results] == ["a"]
    assert results[0]["success"] is False


@pytest.mark.parametrize(
    ("policy", "passed"),
    [("any", False), ("error", True), ("never", True)],
)
def test_run_records_the_gate_decision_under_the_configured_policy(policy, passed):
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}],
        notifier=InMemoryNotifier(),
        **{"dqframework.gate.fail_on": policy},
    )
    with patch("dq.dq_framework.EngineLoader") as loader:
        engine_returning(loader, [_outcome("warn_only", False, "warn")])
        framework.run()
    assert framework.gate_policy == policy
    assert framework.last_gate_decision.passed is passed


def test_unknown_gate_policy_fails_closed_before_any_engine_runs():
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}],
        notifier=InMemoryNotifier(),
        **{"dqframework.gate.fail_on": "sometimes"},
    )
    with patch("dq.dq_framework.EngineLoader") as loader:
        with pytest.raises(ConfigurationError, match="dqframework.gate.fail_on"):
            framework.run()
    loader.return_value.load_engine.assert_not_called()
