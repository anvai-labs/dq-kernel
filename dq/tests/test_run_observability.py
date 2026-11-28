# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""A run as its observer sees it (SPEC-014).

The run core is driven without Spark: the engine loader is patched, the run
key is frozen, and the fake engine writes through the repository as a real
one does, so that what a run returned and persisted can be compared with
another run's, value for value.
"""

import copy
import dataclasses
import json
import logging
import sys
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dq.dq_framework import DQFramework
from dq.exceptions import ConfigurationError
from dq.notifier import InMemoryNotifier
from dq.observability import LOG_FORMAT_VARIABLE, InMemoryObserver, LoggingObserver
from dq.tests.test_framework_contract import _outcome, framework_with_rules

#: The loader is stood in for throughout: whether an engine's library is
#: installed here is not what these tests measure.
pytestmark = pytest.mark.usefixtures("the_loader_is_stood_in_for")

RUN_KEY = 1_700_000_000_000
RUN_LOGGER = "dq.run_events"
CORE_LOGGER = "dq.dq_framework"
ALL_SEVEN = (
    "run_started",
    "rule_started",
    "rule_finished",
    "rule_started",
    "rule_finished",
    "run_finished",
    "gate_event",
)
ONE_RULE = (
    "run_started",
    "rule_started",
    "rule_finished",
    "run_finished",
    "gate_event",
)


class Engine:
    """Returns its outcomes, and persists them through the repository."""

    def __init__(self, outcomes, error=None):
        self.outcomes = outcomes
        self.error = error
        self.keys = []

    def apply(self, dataframe, repository=None):
        if self.error is not None:
            raise self.error
        if repository is not None:
            repository.save(("metrics", self.outcomes), "metrics", self.keys[-1])
            repository.save(
                ("verifications", self.outcomes), "verifications", self.keys[-1]
            )
        return list(self.outcomes)


class RecordingSink:
    """Keeps a copy of everything written, and whether the scope was open."""

    dataset = "orders"

    def __init__(self, error=None):
        self.writes = []
        self.error = error
        self.state = "unopened"

    @contextmanager
    def run_scope(self, run_key, identity=None):
        if self.error is not None:
            raise self.error
        self.state = "open"
        try:
            yield self
        finally:
            self.state = "closed"

    def save(self, df, metric_type, key=None):
        self.writes.append((copy.deepcopy(df), metric_type, key, self.state))


RULES = [
    {"name": "orders", "engine": "deequ", "checks": [{"constraint": "isComplete"}]},
    {"engine": "dqx", "checks": [{"check": {"function": "is_not_null"}}]},
]


def engines_for_rules():
    """One engine per rule of ``RULES``: the first fails a check."""
    return [
        Engine([_outcome("a", True, "error"), _outcome("b", False, "error")]),
        Engine([_outcome("c", True, "warn")]),
    ]


def one_rule_engine(outcomes=None, error=None):
    return [
        Engine(outcomes if outcomes is not None else [_outcome("a", True)], error=error)
    ]


def driven(rules, engines, *, observer=None, notifier=None, with_sink=False, **config):
    """Return ``(framework, run)``: the run loads ``engines`` in rule order."""
    if with_sink:
        config["dqframework.repository"] = {"dataset": "orders"}
    framework = framework_with_rules(
        rules,
        notifier=notifier if notifier is not None else InMemoryNotifier(),
        **config,
    )
    if observer is not None:
        framework._observer = observer
    pending = iter(engines)

    def load(engine_name, rule_config, run_key):
        engine = next(pending)
        engine.keys.append(run_key)
        return engine

    def run(sink=None):
        frozen = SimpleNamespace(  # the run key, and only the run key, is frozen
            time_ns=lambda: RUN_KEY * 1_000_000, perf_counter=time.perf_counter
        )
        with (
            patch("dq.dq_framework.EngineLoader") as patched,
            patch("dq.dq_framework.sink_from_config", return_value=sink),
            patch("dq.dq_framework.time", frozen),
        ):
            patched.return_value.load_engine.side_effect = load
            return framework.run()

    return framework, run


def events_of(observer, notification):
    return [event for name, event in observer.events if name == notification]


# ------------------------------------------------------------------- G1


def test_g1_the_observer_is_told_a_two_rule_run_in_order():
    observer = InMemoryObserver()
    _framework, run = driven(RULES, engines_for_rules(), observer=observer)
    run()
    assert observer.notifications == ALL_SEVEN
    started, first, done, second, second_done, finished, gate = [
        event for _name, event in observer.events
    ]
    assert (started.run_key, started.job_id, started.dataset, started.rules) == (
        RUN_KEY,
        "local-test",
        "unknown",
        2,
    )
    assert (first.rule, first.engine, first.dataset) == ("orders", "deequ", "default")
    assert (done.rule, done.outcomes, done.failed, done.error) == (
        "orders",
        2,
        ("b",),
        None,
    )
    # A rule with no name is named by its position.
    assert (second.rule, second.engine, second.dataset) == ("rule 1", "dqx", "default")
    assert (second_done.outcomes, second_done.failed) == (1, ())
    assert (finished.rules, finished.outcomes, finished.failed) == (2, 3, 1)
    assert (finished.blocking, finished.advisory, finished.gate_passed) == (1, 0, False)
    assert (gate.status, gate.failed, gate.total, gate.blocked) == (
        "failed",
        ("b",),
        3,
        ("b",),
    )
    # One run, one key, on every event.
    assert {event.run_key for _name, event in observer.events} == {RUN_KEY}


def test_the_gate_event_the_observer_gets_is_the_one_the_notifier_gets():
    observer, notifier = InMemoryObserver(), InMemoryNotifier()
    _framework, run = driven(
        RULES, engines_for_rules(), observer=observer, notifier=notifier
    )
    run()
    (gate,) = events_of(observer, "gate_event")
    # The same record, not a copy: neither can change what the other saw.
    assert len(notifier.events) == 1
    assert notifier.events[0] is gate


def test_a_rule_that_names_two_datasets_is_two_applications():
    observer = InMemoryObserver()
    rule = {
        "engine": "deequ",
        "dataframes": ["orders", "returns"],
        "checks": [{"constraint": "isComplete"}],
    }
    _framework, run = driven([rule], [Engine([_outcome("a", True)])], observer=observer)
    run()
    assert observer.notifications == ALL_SEVEN
    assert [event.dataset for event in events_of(observer, "rule_started")] == [
        "orders",
        "returns",
    ]
    # In the order the rule lists them.
    assert [event.dataset for event in events_of(observer, "rule_finished")] == [
        "orders",
        "returns",
    ]
    assert events_of(observer, "run_started")[0].rules == 2
    assert events_of(observer, "run_finished")[0].rules == 2


def test_the_dataset_the_run_names_comes_from_the_configuration():
    observer = InMemoryObserver()
    _framework, run = driven(
        RULES[:1],
        one_rule_engine(),
        observer=observer,
        **{"dqframework.dataset": "orders"},
    )
    run()
    assert events_of(observer, "run_started")[0].dataset == "orders"
    assert events_of(observer, "run_finished")[0].dataset == "orders"


def test_the_dataset_the_run_names_comes_from_the_sink_when_not_configured():
    observer = InMemoryObserver()
    _framework, run = driven(
        RULES[:1], one_rule_engine(), observer=observer, with_sink=True
    )
    run(RecordingSink())
    assert events_of(observer, "run_started")[0].dataset == "orders"


# ------------------------------------------------------------------- G2


class Exploding(InMemoryObserver):
    def _refuse(self, notification):
        raise RuntimeError(f"cannot {notification}")

    def run_started(self, event):
        self._refuse("run_started")

    def rule_started(self, event):
        self._refuse("rule_started")

    def rule_finished(self, event):
        self._refuse("rule_finished")

    def run_finished(self, event):
        self._refuse("run_finished")

    def gate_event(self, event):
        self._refuse("gate_event")


def a_run(observer):
    """Run the two rules once: what came back, what was written, what was announced."""
    sink, notifier = RecordingSink(), InMemoryNotifier()
    framework, run = driven(
        RULES, engines_for_rules(), observer=observer, notifier=notifier, with_sink=True
    )
    results = run(sink)
    return (
        copy.deepcopy(results),
        sink.writes,
        [event.to_dict() for event in notifier.events],
        framework.last_gate_decision.to_dict(),
    )


def test_g2_an_observer_that_raises_on_every_notification_changes_nothing(caplog):
    quiet = a_run(InMemoryObserver())
    with caplog.at_level(logging.WARNING, logger=CORE_LOGGER):
        loud = a_run(Exploding())
    assert loud == quiet
    # Every notification was attempted, and every failure was logged with it.
    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
        and record.getMessage().startswith("observer ")
    ]
    assert warnings == [
        f"observer Exploding failed on {notification} for run_key={RUN_KEY}: cannot {notification}"
        for notification in ALL_SEVEN
    ]


def test_the_persisted_artifacts_of_the_two_runs_are_written_alike():
    _results, writes, _events, _decision = a_run(Exploding())
    assert [(metric_type, key, state) for _df, metric_type, key, state in writes] == [
        ("metrics", RUN_KEY, "open"),
        ("verifications", RUN_KEY, "open"),
        ("metrics", RUN_KEY, "open"),
        ("verifications", RUN_KEY, "open"),
    ]


def test_no_event_holds_anything_the_run_returns():
    observer = InMemoryObserver()
    _framework, run = driven(RULES, engines_for_rules(), observer=observer)
    results = run()
    returned = [id(result) for result in results]
    for _name, event in observer.events:
        for field in dataclasses.fields(event):
            value = getattr(event, field.name)
            assert id(value) not in returned
            if isinstance(value, tuple):
                assert all(id(item) not in returned for item in value)
    # And the caller's changes to what it was given reach no event.
    results[0]["check"] = "renamed by the caller"
    assert events_of(observer, "rule_finished")[0].failed == ("b",)


def test_a_rule_is_announced_as_finished_only_after_its_engine_has_run():
    """One timeline for the run's work and what it told the observer."""
    timeline = []

    class Timed(Engine):
        def apply(self, dataframe, repository=None):
            timeline.append("engine ran")
            return super().apply(dataframe, repository)

    class Watching(InMemoryObserver):
        """Records each notification on the same timeline as the engine's work."""

        def run_started(self, event):
            timeline.append("run_started")

        def rule_started(self, event):
            timeline.append("rule_started")

        def rule_finished(self, event):
            timeline.append("rule_finished")

        def run_finished(self, event):
            timeline.append("run_finished")

        def gate_event(self, event):
            timeline.append("gate_event")

    engines = [Timed([_outcome("a", True)]), Timed([_outcome("b", False)])]
    observer = Watching()
    _framework, run = driven(RULES, engines, observer=observer)
    run()
    assert timeline == [
        "run_started",
        "rule_started",
        "engine ran",
        "rule_finished",
        "rule_started",
        "engine ran",
        "rule_finished",
        "run_finished",
        "gate_event",
    ]


# ------------------------------------------------------------------- G5


def test_g5_each_rule_has_a_duration_and_none_is_negative():
    observer = InMemoryObserver()
    _framework, run = driven(RULES, engines_for_rules(), observer=observer)
    run()
    rules = events_of(observer, "rule_finished")
    (finished,) = events_of(observer, "run_finished")
    assert [event.duration_ms >= 0 for event in rules] == [True, True]
    assert finished.duration_ms >= 0
    # The rules ran one after the other inside the run.
    assert finished.duration_ms >= sum(event.duration_ms for event in rules) - 1e-6


def test_durations_are_the_time_between_a_rule_starting_and_finishing():
    observer = InMemoryObserver()
    framework, run = driven(RULES, engines_for_rules(), observer=observer)
    readings = iter([0.0, 10.0, 10.5, 11.0, 12.25, 13.0])
    framework._clock = lambda: next(readings)
    run()
    rules = events_of(observer, "rule_finished")
    (finished,) = events_of(observer, "run_finished")
    assert [event.duration_ms for event in rules] == [
        pytest.approx(500.0),
        pytest.approx(1250.0),
    ]
    assert finished.duration_ms == pytest.approx(13000.0)


def test_a_rule_that_takes_time_is_measured_taking_it():
    class Slow(Engine):
        def apply(self, dataframe, repository=None):
            time.sleep(0.02)
            return super().apply(dataframe, repository)

    observer = InMemoryObserver()
    _framework, run = driven(
        RULES[:1], [Slow([_outcome("a", True)])], observer=observer
    )
    run()
    assert events_of(observer, "rule_finished")[0].duration_ms >= 20.0


# ------------------------------------------------------ when a run goes wrong


def test_a_rule_that_raises_is_announced_and_the_exception_still_propagates():
    observer, notifier = InMemoryObserver(), InMemoryNotifier()
    engines = engines_for_rules()
    engines[1] = Engine([], error=RuntimeError("boom"))
    framework, run = driven(RULES, engines, observer=observer, notifier=notifier)
    with pytest.raises(RuntimeError, match="boom"):
        run()
    assert observer.notifications == ALL_SEVEN[:6]
    (failed_rule,) = [
        event
        for event in events_of(observer, "rule_finished")
        if event.error is not None
    ]
    assert (failed_rule.rule, failed_rule.error, failed_rule.outcomes) == (
        "rule 1",
        "RuntimeError: boom",
        0,
    )
    assert failed_rule.duration_ms >= 0
    (finished,) = events_of(observer, "run_finished")
    assert (finished.error, finished.rules, finished.outcomes, finished.failed) == (
        "RuntimeError: boom",
        2,
        2,
        1,
    )
    assert (finished.blocking, finished.advisory, finished.gate_passed) == (
        None,
        None,
        None,
    )
    # No decision was reached, so nothing was announced as one.
    assert notifier.events == []
    assert framework.last_gate_decision is None


def test_a_rule_that_produced_nothing_is_announced_as_having_raised():
    observer = InMemoryObserver()
    _framework, run = driven(RULES[:1], one_rule_engine(outcomes=[]), observer=observer)
    with pytest.raises(Exception, match="zero"):
        run()
    assert observer.notifications == ONE_RULE[:4]
    (rule,) = events_of(observer, "rule_finished")
    (finished,) = events_of(observer, "run_finished")
    assert rule.error.startswith("ValidationError:")
    assert finished.error.startswith("ValidationError:")
    assert (rule.outcomes, finished.outcomes) == (0, 0)


def test_a_run_scope_that_cannot_be_opened_is_announced():
    observer = InMemoryObserver()
    _framework, run = driven(
        RULES, engines_for_rules(), observer=observer, with_sink=True
    )
    with pytest.raises(RuntimeError, match="locked"):
        run(RecordingSink(error=RuntimeError("database is locked")))
    assert observer.notifications == ("run_started", "run_finished")
    (finished,) = events_of(observer, "run_finished")
    assert (finished.error, finished.rules, finished.outcomes) == (
        "RuntimeError: database is locked",
        0,
        0,
    )


@pytest.mark.parametrize(
    ("interruption", "described"),
    [(KeyboardInterrupt(), "KeyboardInterrupt: "), (SystemExit(3), "SystemExit: 3")],
    ids=["KeyboardInterrupt", "SystemExit"],
)
def test_an_interrupted_run_still_announces_its_rule_and_itself(
    interruption, described
):
    """A rule that is interrupted ended too; the observer is told so."""
    observer = InMemoryObserver()
    _framework, run = driven(
        RULES[:1], one_rule_engine(error=interruption), observer=observer
    )
    with pytest.raises(type(interruption)):
        run()
    assert observer.notifications == ONE_RULE[:4]
    (rule,) = events_of(observer, "rule_finished")
    (finished,) = events_of(observer, "run_finished")
    assert (rule.error, finished.error) == (described, described)
    assert rule.outcomes == 0


def test_rule_events_come_from_inside_the_run_scope_and_the_rest_from_after_it():
    observer, sink = InMemoryObserver(), RecordingSink()
    framework, run = driven(
        RULES[:1], one_rule_engine(), observer=observer, with_sink=True
    )
    states = []
    for notification in ("rule_started", "rule_finished", "run_finished", "gate_event"):
        original = getattr(observer, notification)

        def noting(event, notification=notification, original=original):
            states.append((notification, sink.state))
            original(event)

        setattr(observer, notification, noting)
    run(sink)
    assert states == [
        ("rule_started", "open"),
        ("rule_finished", "open"),
        ("run_finished", "closed"),
        ("gate_event", "closed"),
    ]


# ------------------------------------------------------------- configuration


def test_with_nothing_configured_the_run_is_logged(caplog):
    _framework, run = driven(RULES[:1], one_rule_engine())
    with caplog.at_level(logging.INFO, logger=RUN_LOGGER):
        run()
    lines = [
        record.getMessage() for record in caplog.records if record.name == RUN_LOGGER
    ]
    assert [line.split(" ", 1)[0] for line in lines] == list(ONE_RULE)
    assert (
        lines[1]
        == f"rule_started run_key={RUN_KEY} rule=orders engine=deequ dataset=default"
    )
    assert (
        lines[-1]
        == f"gate_event run_key={RUN_KEY} dataset=unknown status=passed failed=0/1 blocking=0 advisory=0"
    )


def test_observers_are_built_from_the_configuration():
    framework, run = driven(
        RULES[:1], one_rule_engine(), **{"dqframework.observers": [{"type": "memory"}]}
    )
    run()
    assert isinstance(framework._observer, InMemoryObserver)
    assert framework._observer.notifications == ONE_RULE


def test_an_injected_observer_is_preferred_to_the_configured_ones():
    injected = InMemoryObserver()
    framework, run = driven(
        RULES[:1],
        one_rule_engine(),
        observer=injected,
        **{"dqframework.observers": [{"type": "memory"}]},
    )
    run()
    assert framework._observer is injected
    assert injected.notifications == ONE_RULE


def test_an_unknown_observer_type_fails_before_any_engine_runs():
    framework, _run = driven(
        RULES[:1], one_rule_engine(), **{"dqframework.observers": [{"type": "sentry"}]}
    )
    with patch("dq.dq_framework.EngineLoader") as patched:
        with pytest.raises(ConfigurationError, match="'sentry'"):
            framework.run()
    patched.return_value.load_engine.assert_not_called()


def test_a_missing_extra_fails_before_any_engine_runs(monkeypatch):
    for name in ("opentelemetry", "opentelemetry.trace", "opentelemetry.metrics"):
        monkeypatch.setitem(sys.modules, name, None)
    framework, _run = driven(
        RULES[:1], one_rule_engine(), **{"dqframework.observers": [{"type": "otel"}]}
    )
    with patch("dq.dq_framework.EngineLoader") as patched:
        with pytest.raises(ImportError, match="dq-kernel\\[otel\\]|-E otel"):
            framework.run()
    patched.return_value.load_engine.assert_not_called()


def test_the_observer_is_built_once_however_often_the_framework_runs():
    framework, run = driven(
        RULES[:1],
        one_rule_engine() + one_rule_engine(),
        **{"dqframework.observers": [{"type": "memory"}]},
    )
    run()
    first = framework._observer
    run()
    assert framework._observer is first
    assert first.notifications == ONE_RULE * 2


def test_json_lines_from_a_real_run_parse_and_name_the_run(monkeypatch, caplog):
    monkeypatch.setenv(LOG_FORMAT_VARIABLE, "json")
    _framework, run = driven(RULES, engines_for_rules())
    with caplog.at_level(logging.INFO, logger=RUN_LOGGER):
        run()
    documents = [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == RUN_LOGGER
    ]
    assert [document["event"] for document in documents] == list(ALL_SEVEN)
    assert {document["run_key"] for document in documents} == {RUN_KEY}
    rule_documents = [
        document for document in documents if document["event"].startswith("rule_")
    ]
    assert [(document["rule"], document["engine"]) for document in rule_documents] == [
        ("orders", "deequ"),
        ("orders", "deequ"),
        ("rule 1", "dqx"),
        ("rule 1", "dqx"),
    ]


def test_the_logging_observer_is_what_a_framework_built_by_hand_gets():
    """Assembled through ``__new__``, with no observer attribute of its own."""
    framework = framework_with_rules(
        [{"engine": "deequ", "checks": [{"constraint": "isComplete"}]}]
    )
    assert "_observer" not in vars(framework)
    assert DQFramework._observer is None
    assert type(framework._observer_for_run()) is LoggingObserver


@pytest.mark.parametrize(
    ("rules", "config", "refusal"),
    [
        ([], {}, "No dqrules"),
        (
            RULES[:1],
            {"dqframework.gate.fail_on": "sometimes"},
            "dqframework.gate.fail_on",
        ),
    ],
    ids=["no-rules", "unknown-gate-policy"],
)
def test_nothing_is_announced_of_a_run_that_never_started(rules, config, refusal):
    """A configuration refused before any rule runs announces nothing, not
    even that a run started: there was no run."""
    observer = InMemoryObserver()
    framework = framework_with_rules(rules, **config)
    framework._observer = observer
    with patch("dq.dq_framework.EngineLoader") as patched:
        with pytest.raises(ConfigurationError, match=refusal):
            framework.run()
    patched.return_value.load_engine.assert_not_called()
    assert observer.events == []


def test_the_run_is_measured_from_before_its_first_rule():
    """A rule's time is its own; the run's includes what surrounds its rules."""
    observer = InMemoryObserver()
    framework, run = driven(RULES[:1], one_rule_engine(), observer=observer)
    readings = iter([0.0, 10.0, 10.5, 11.0])
    framework._clock = lambda: next(readings)
    run()
    assert events_of(observer, "rule_finished")[0].duration_ms == pytest.approx(500.0)
    assert events_of(observer, "run_finished")[0].duration_ms == pytest.approx(11000.0)
