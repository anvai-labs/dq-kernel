# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The observer port and its adapters (SPEC-014), on their own.

The OpenTelemetry adapter is checked twice: against a fake tracer and meter,
which record exactly what they are told, and against the real API through the
SDK's in-memory span exporter and metric reader, which export nothing. The
fake confirms the adapter's intent; the SDK confirms OpenTelemetry accepts it.
"""

import dataclasses
import json
import logging
import sys

import pytest

from dq.exceptions import ConfigurationError
from dq.notifier import GateEvent
from dq.observability import (
    INSTRUMENTATION_NAME,
    JSON_LINES,
    LOG_FORMAT_VARIABLE,
    OUTCOME_COUNTER,
    RUN_SPAN,
    RULE_SPAN,
    CompositeObserver,
    InMemoryObserver,
    LoggingObserver,
    OpenTelemetryObserver,
    RunFinished,
    RunStarted,
    RuleFinished,
    RuleStarted,
    describe,
    elapsed_ms,
    observer_from_config,
)
from dq.tests.test_package_contract import _run_with_blocked_imports
from dq.tests.observability_support import in_memory_observatory

RUN_KEY = 1_700_000_000_000
JOB = "local-abc"
LOGGER = "dq.run_events"


def a_run(observer) -> None:
    """Tell ``observer`` what a two-rule run would have: one check failed."""
    observer.run_started(RunStarted(RUN_KEY, JOB, "orders", 2))
    observer.rule_started(
        RuleStarted(RUN_KEY, "orders_id_complete", "deequ", "default")
    )
    observer.rule_finished(
        RuleFinished(
            RUN_KEY, "orders_id_complete", "deequ", "default", 842.1, 3, ("orders_id",)
        )
    )
    observer.rule_started(RuleStarted(RUN_KEY, "orders_unique", "dqx", "default"))
    observer.rule_finished(
        RuleFinished(RUN_KEY, "orders_unique", "dqx", "default", 12.0, 1)
    )
    observer.run_finished(RunFinished(RUN_KEY, JOB, "orders", 900.0, 2, 4, 1, 1, 0))
    observer.gate_event(
        GateEvent(
            run_key=RUN_KEY,
            dataset="orders",
            status="failed",
            failed=("orders_id",),
            total=4,
            ts=RUN_KEY,
            job_id=JOB,
            blocked=("orders_id",),
        )
    )


# ------------------------------------------------------------------ events


def test_an_event_cannot_be_changed_after_it_is_built():
    event = RunStarted(RUN_KEY, JOB, "orders", 2)
    with pytest.raises(dataclasses.FrozenInstanceError):
        event.rules = 3


def test_a_rule_that_raised_is_described_by_type_and_message():
    event = RuleFinished(
        RUN_KEY, "r", "deequ", "default", 0.5, error=describe(RuntimeError("boom"))
    )
    assert event.error == "RuntimeError: boom"
    assert event.outcomes == 0 and event.failed == ()


def test_the_gate_verdict_of_a_finished_run():
    passed = RunFinished(RUN_KEY, JOB, "orders", 1.0, 1, 3, 0, 0, 0)
    blocked = RunFinished(RUN_KEY, JOB, "orders", 1.0, 1, 3, 1, 1, 0)
    advised = RunFinished(RUN_KEY, JOB, "orders", 1.0, 1, 3, 1, 0, 1)
    raised = RunFinished(
        RUN_KEY, JOB, "orders", 1.0, 1, 3, 1, error="RuntimeError: boom"
    )
    assert (passed.gate_passed, blocked.gate_passed, advised.gate_passed) == (
        True,
        False,
        True,
    )
    assert raised.gate_passed is None  # no decision was reached


def test_a_duration_is_never_negative():
    assert elapsed_ms(5.0, 5.5) == pytest.approx(500.0)
    assert elapsed_ms(5.0, 3.0) == 0.0


# -------------------------------------------------- recording and fan-out


def test_the_recording_observer_keeps_every_notification_in_order():
    observer = InMemoryObserver()
    a_run(observer)
    assert observer.notifications == (
        "run_started",
        "rule_started",
        "rule_finished",
        "rule_started",
        "rule_finished",
        "run_finished",
        "gate_event",
    )
    assert [type(event).__name__ for _name, event in observer.events] == [
        "RunStarted",
        "RuleStarted",
        "RuleFinished",
        "RuleStarted",
        "RuleFinished",
        "RunFinished",
        "GateEvent",
    ]


def test_one_failing_observer_neither_stops_the_others_nor_raises(caplog):
    first, last = InMemoryObserver(), InMemoryObserver()

    class Exploding(InMemoryObserver):
        def run_started(self, event):
            raise RuntimeError("cannot log")

        def gate_event(self, event):
            raise RuntimeError("cannot trace")

    exploding = Exploding()
    composite = CompositeObserver(first, exploding, last)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        a_run(composite)
    everything = (
        "run_started",
        "rule_started",
        "rule_finished",
        "rule_started",
        "rule_finished",
        "run_finished",
        "gate_event",
    )
    # The two that work each received all seven; the one that cannot was
    # asked for all seven too, and what it did with them is its own affair.
    assert first.notifications == everything
    assert last.notifications == everything
    assert exploding.notifications == everything[1:6]
    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
    ]
    assert warnings == [
        f"observer Exploding failed on run_started for run_key={RUN_KEY}: cannot log",
        f"observer Exploding failed on gate_event for run_key={RUN_KEY}: cannot trace",
    ]


# ------------------------------------------------------------- logging


def lines_of(caplog, observer, level=logging.INFO):
    """Log one run through ``observer``; return the lines it wrote, and only those."""
    with caplog.at_level(level, logger=LOGGER):
        before = len(caplog.records)
        a_run(observer)
    return [
        record.getMessage()
        for record in caplog.records[before:]
        if record.name == LOGGER
    ]


def test_the_logging_observer_names_the_run_the_rule_and_the_engine(caplog):
    lines = lines_of(caplog, LoggingObserver(log_format="text"))
    assert lines == [
        f"run_started run_key={RUN_KEY} job_id={JOB} dataset=orders rules=2",
        f"rule_started run_key={RUN_KEY} rule=orders_id_complete engine=deequ dataset=default",
        "rule_finished run_key=1700000000000 rule=orders_id_complete engine=deequ "
        "dataset=default duration_ms=842.1 outcomes=3 failed=1",
        f"rule_started run_key={RUN_KEY} rule=orders_unique engine=dqx dataset=default",
        "rule_finished run_key=1700000000000 rule=orders_unique engine=dqx "
        "dataset=default duration_ms=12.0 outcomes=1 failed=0",
        "run_finished run_key=1700000000000 job_id=local-abc dataset=orders "
        "duration_ms=900.0 rules=2 outcomes=4 failed=1 blocking=1 advisory=0",
        f"gate_event run_key={RUN_KEY} dataset=orders status=failed failed=1/4 blocking=1 advisory=0",
    ]


def test_a_rule_that_raised_and_a_run_that_raised_say_so_in_the_log(caplog):
    observer = LoggingObserver(log_format="text")
    with caplog.at_level(logging.INFO, logger=LOGGER):
        observer.rule_finished(
            RuleFinished(
                RUN_KEY, "rule 1", "deequ", "default", 0.0, error="RuntimeError: boom"
            )
        )
        observer.run_finished(
            RunFinished(
                RUN_KEY, JOB, "orders", 0.0, 1, 0, 0, error="RuntimeError: boom"
            )
        )
    lines = [record.getMessage() for record in caplog.records]
    assert lines[0].endswith("outcomes=0 failed=0 error=RuntimeError: boom")
    # No decision was reached, so there is no blocking or advisory count to print.
    assert lines[1].endswith("rules=1 outcomes=0 failed=0 error=RuntimeError: boom")


def test_json_log_lines_parse_and_carry_the_run_and_the_rule(caplog):
    lines = lines_of(caplog, LoggingObserver(log_format=JSON_LINES))
    documents = [json.loads(line) for line in lines]
    assert [document["event"] for document in documents] == [
        "run_started",
        "rule_started",
        "rule_finished",
        "rule_started",
        "rule_finished",
        "run_finished",
        "gate_event",
    ]
    # Every line carries the run key.
    assert {document["run_key"] for document in documents} == {RUN_KEY}
    rule_lines = [
        document for document in documents if document["event"].startswith("rule_")
    ]
    assert [(document["rule"], document["engine"]) for document in rule_lines] == [
        ("orders_id_complete", "deequ"),
        ("orders_id_complete", "deequ"),
        ("orders_unique", "dqx"),
        ("orders_unique", "dqx"),
    ]
    first, second, last = documents[1], documents[2], documents[-1]
    assert (first["rule"], first["engine"], first["dataset"]) == (
        "orders_id_complete",
        "deequ",
        "default",
    )
    assert (second["duration_ms"], second["outcomes"], second["failed"]) == (
        842.1,
        3,
        ["orders_id"],
    )
    assert (last["status"], last["failed"], last["total"], last["blocked"]) == (
        "failed",
        ["orders_id"],
        4,
        ["orders_id"],
    )


def test_a_json_line_says_when_no_verdict_was_reached(caplog):
    observer = LoggingObserver(log_format=JSON_LINES)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        observer.run_finished(
            RunFinished(RUN_KEY, JOB, "orders", 0.0, 1, 0, 0, error="E: x")
        )
    document = json.loads(caplog.records[-1].getMessage())
    assert (
        document["event"],
        document["blocking"],
        document["advisory"],
        document["error"],
    ) == (
        "run_finished",
        None,
        None,
        "E: x",
    )


def test_the_log_format_comes_from_the_environment(monkeypatch, caplog):
    monkeypatch.setenv(LOG_FORMAT_VARIABLE, "json")
    documents = [json.loads(line) for line in lines_of(caplog, LoggingObserver())]
    assert [document["event"] for document in documents] == [
        "run_started",
        "rule_started",
        "rule_finished",
        "rule_started",
        "rule_finished",
        "run_finished",
        "gate_event",
    ]


def test_the_log_format_is_case_insensitive_and_anything_else_is_text(
    monkeypatch, caplog
):
    monkeypatch.setenv(LOG_FORMAT_VARIABLE, " JSON ")
    assert all(line.startswith("{") for line in lines_of(caplog, LoggingObserver()))
    monkeypatch.setenv(LOG_FORMAT_VARIABLE, "text")
    assert all(not line.startswith("{") for line in lines_of(caplog, LoggingObserver()))
    monkeypatch.delenv(LOG_FORMAT_VARIABLE)
    assert all(not line.startswith("{") for line in lines_of(caplog, LoggingObserver()))


def test_a_format_given_outright_beats_the_environment(monkeypatch, caplog):
    monkeypatch.setenv(LOG_FORMAT_VARIABLE, "json")
    assert all(
        not line.startswith("{")
        for line in lines_of(caplog, LoggingObserver(log_format="text"))
    )
    monkeypatch.delenv(LOG_FORMAT_VARIABLE)
    assert all(
        line.startswith("{")
        for line in lines_of(caplog, LoggingObserver(log_format="json"))
    )


# --------------------------------------------------------- configuration


def test_nothing_configured_is_the_logging_observer():
    assert type(observer_from_config(None)).__name__ == "LoggingObserver"
    assert type(observer_from_config([])).__name__ == "LoggingObserver"


@pytest.mark.parametrize(
    "kind, built", [("logging", LoggingObserver), ("memory", InMemoryObserver)]
)
def test_each_type_builds_its_observer(kind, built):
    assert isinstance(observer_from_config([{"type": kind}]), built)


def test_several_configured_fan_out_through_one_composite():
    composite = observer_from_config([{"type": "logging"}, {"type": "memory"}])
    assert isinstance(composite, CompositeObserver)
    assert [type(each).__name__ for each in composite._observers] == [
        "LoggingObserver",
        "InMemoryObserver",
    ]


@pytest.mark.parametrize(
    "entry", [{"type": "sentry"}, {}, "not even an entry", {"type": None}]
)
def test_an_unknown_type_fails_closed(entry):
    with pytest.raises(
        ConfigurationError, match="is not supported; use 'logging', 'memory' or 'otel'"
    ):
        observer_from_config([entry])


def test_an_unknown_type_is_reported_after_the_ones_before_it_are_accepted():
    with pytest.raises(ConfigurationError, match="'sentry'"):
        observer_from_config([{"type": "memory"}, {"type": "sentry"}])


# ---------------------------------------------------- OpenTelemetry, fakes


class FakeSpan:
    def __init__(self, name, attributes):
        self.name = name
        self.attributes = dict(attributes or {})
        self.status = None
        self.ends = 0

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def set_status(self, status, description=None):
        self.status = (status, description)

    def end(self):
        self.ends += 1


class FakeTracer:
    def __init__(self):
        self.spans = []

    def start_span(self, name, attributes=None):
        span = FakeSpan(name, attributes)
        self.spans.append(span)
        return span


class FakeCounter:
    def __init__(self, name, unit="", description=""):
        self.name, self.unit, self.description = name, unit, description
        self.added = []

    def add(self, amount, attributes=None):
        self.added.append((amount, dict(attributes or {})))


class FakeMeter:
    def __init__(self):
        self.counters = []

    def create_counter(self, name, unit="", description=""):
        counter = FakeCounter(name, unit, description)
        self.counters.append(counter)
        return counter


@pytest.fixture
def fakes():
    tracer, meter = FakeTracer(), FakeMeter()
    return tracer, meter, OpenTelemetryObserver(tracer=tracer, meter=meter)


def spans_named(tracer, name):
    return [span for span in tracer.spans if span.name == name]


def test_one_span_per_run_and_one_per_rule(fakes):
    tracer, meter, observer = fakes
    a_run(observer)
    run = spans_named(tracer, RUN_SPAN)
    rules = spans_named(tracer, RULE_SPAN)
    assert len(run) == 1 and len(rules) == 2
    assert run[0].attributes == {
        "dq.run_key": RUN_KEY,
        "dq.job_id": JOB,
        "dq.dataset": "orders",
        "dq.rules": 2,
        "dq.duration_ms": 900.0,
        "dq.outcomes": 4,
        "dq.failed": 1,
        "dq.gate_passed": False,
        "dq.blocking": 1,
        "dq.advisory": 0,
    }
    assert rules[0].attributes == {
        "dq.run_key": RUN_KEY,
        "dq.rule": "orders_id_complete",
        "dq.engine": "deequ",
        "dq.dataset": "default",
        "dq.duration_ms": 842.1,
        "dq.outcomes": 3,
        "dq.failed": 1,
    }
    assert rules[1].attributes["dq.rule"] == "orders_unique"
    assert [span.ends for span in tracer.spans] == [1, 1, 1]


def test_the_counter_counts_outcomes_by_status_and_criticality(fakes):
    tracer, meter, observer = fakes
    a_run(observer)
    (counter,) = meter.counters
    assert (counter.name, counter.unit) == (OUTCOME_COUNTER, "1")
    assert sorted(counter.added) == [
        (1, {"dq.status": "failed", "dq.criticality": "error"}),
        (3, {"dq.status": "passed", "dq.criticality": "n/a"}),
    ]


def test_advisory_failures_are_counted_as_warnings(fakes):
    _tracer, meter, observer = fakes
    observer.gate_event(
        GateEvent(
            run_key=RUN_KEY,
            dataset="orders",
            status="failed",
            failed=("a", "b", "c"),
            total=3,
            ts=RUN_KEY,
            job_id=JOB,
            blocked=("a",),
            warned=("b", "c"),
        )
    )
    assert sorted(meter.counters[0].added) == [
        (1, {"dq.status": "failed", "dq.criticality": "error"}),
        (2, {"dq.status": "failed", "dq.criticality": "warn"}),
    ]


def test_a_clean_run_adds_only_its_passed_outcomes(fakes):
    _tracer, meter, observer = fakes
    observer.gate_event(
        GateEvent(
            run_key=RUN_KEY,
            dataset="orders",
            status="passed",
            failed=(),
            total=5,
            ts=RUN_KEY,
            job_id=JOB,
        )
    )
    assert meter.counters[0].added == [
        (5, {"dq.status": "passed", "dq.criticality": "n/a"})
    ]


def test_a_rule_that_raised_ends_its_span_in_error(fakes):
    tracer, meter, observer = fakes
    observer.run_started(RunStarted(RUN_KEY, JOB, "orders", 1))
    observer.rule_started(RuleStarted(RUN_KEY, "rule 0", "deequ", "default"))
    observer.rule_finished(
        RuleFinished(
            RUN_KEY, "rule 0", "deequ", "default", 3.25, error="RuntimeError: boom"
        )
    )
    observer.run_finished(
        RunFinished(RUN_KEY, JOB, "orders", 4.0, 1, 0, 0, error="RuntimeError: boom")
    )
    rule, run = spans_named(tracer, RULE_SPAN)[0], spans_named(tracer, RUN_SPAN)[0]
    assert rule.status == (observer._error_status, "RuntimeError: boom")
    assert run.status == (observer._error_status, "RuntimeError: boom")
    assert "dq.gate_passed" not in run.attributes  # no decision was reached
    assert meter.counters[0].added == []  # and there was no gate event to count


def test_a_passing_gate_is_an_attribute_of_the_run_span(fakes):
    tracer, _meter, observer = fakes
    observer.run_started(RunStarted(RUN_KEY, JOB, "orders", 1))
    observer.run_finished(RunFinished(RUN_KEY, JOB, "orders", 1.0, 1, 2, 0, 0, 0))
    assert spans_named(tracer, RUN_SPAN)[0].attributes["dq.gate_passed"] is True
    assert spans_named(tracer, RUN_SPAN)[0].status is None


def test_a_run_that_only_warned_opened_its_gate(fakes):
    tracer, _meter, observer = fakes
    observer.run_started(RunStarted(RUN_KEY, JOB, "orders", 1))
    observer.run_finished(RunFinished(RUN_KEY, JOB, "orders", 1.0, 1, 2, 1, 0, 1))
    run = spans_named(tracer, RUN_SPAN)[0]
    assert (run.attributes["dq.gate_passed"], run.attributes["dq.advisory"]) == (
        True,
        1,
    )


def test_a_notification_that_repeats_never_ends_a_span_twice(fakes):
    """OpenTelemetry only logs a second end; the adapter keeps its own count."""
    tracer, _meter, observer = fakes
    event = RuleFinished(RUN_KEY, "r", "deequ", "default", 1.0, 1)
    observer.run_started(RunStarted(RUN_KEY, JOB, "orders", 1))
    observer.rule_started(RuleStarted(RUN_KEY, "r", "deequ", "default"))
    observer.rule_finished(event)
    observer.rule_finished(event)
    observer.run_finished(RunFinished(RUN_KEY, JOB, "orders", 1.0, 1, 1, 0, 0, 0))
    observer.run_finished(RunFinished(RUN_KEY, JOB, "orders", 1.0, 1, 1, 0, 0, 0))
    assert [span.ends for span in tracer.spans] == [1, 1]
    assert observer._runs == {} and observer._rules == {}


def test_a_finish_nobody_started_is_ignored(fakes):
    tracer, meter, observer = fakes
    observer.rule_finished(RuleFinished(RUN_KEY, "r", "deequ", "default", 1.0, 1))
    observer.run_finished(RunFinished(RUN_KEY, JOB, "orders", 1.0, 1, 1, 0, 0, 0))
    assert tracer.spans == []
    assert meter.counters[0].added == []


def test_the_same_rule_on_two_datasets_gets_two_spans(fakes):
    tracer, _meter, observer = fakes
    observer.rule_started(RuleStarted(RUN_KEY, "r", "deequ", "orders"))
    observer.rule_started(RuleStarted(RUN_KEY, "r", "deequ", "returns"))
    observer.rule_finished(RuleFinished(RUN_KEY, "r", "deequ", "returns", 2.0, 1))
    observer.rule_finished(RuleFinished(RUN_KEY, "r", "deequ", "orders", 1.0, 3))
    orders, returns = spans_named(tracer, RULE_SPAN)
    assert (
        orders.attributes["dq.duration_ms"],
        returns.attributes["dq.duration_ms"],
    ) == (1.0, 2.0)


# ----------------------------------------------- OpenTelemetry, the real API


def test_without_the_extra_the_observer_names_it(monkeypatch):
    for name in ("opentelemetry", "opentelemetry.trace", "opentelemetry.metrics"):
        monkeypatch.setitem(sys.modules, name, None)
    with pytest.raises(ImportError) as refused:
        OpenTelemetryObserver()
    message = str(refused.value)
    assert (
        "The otel observer requires the optional opentelemetry-api package" in message
    )
    assert "pip install -E otel" in message or "poetry install -E otel" in message


def test_with_a_tracer_standing_in_and_no_opentelemetry_the_error_status_is_a_word(
    monkeypatch,
):
    """A stand-in tracer needs no OpenTelemetry, so neither does the adapter.

    CI installs the extra, so the only way here is to take it away: this is
    the adapter built entirely from fakes where the package is absent.
    """
    for name in ("opentelemetry", "opentelemetry.trace", "opentelemetry.metrics"):
        monkeypatch.setitem(sys.modules, name, None)
    tracer, meter = FakeTracer(), FakeMeter()
    observer = OpenTelemetryObserver(tracer=tracer, meter=meter)
    observer.run_started(RunStarted(RUN_KEY, JOB, "orders", 1))
    observer.rule_started(RuleStarted(RUN_KEY, "r", "deequ", "default"))
    observer.rule_finished(
        RuleFinished(RUN_KEY, "r", "deequ", "default", 1.0, error="RuntimeError: boom")
    )
    (rule,) = spans_named(tracer, RULE_SPAN)
    assert rule.status == ("error", "RuntimeError: boom")
    assert rule.ends == 1


def test_with_the_api_and_no_providers_the_observer_is_silent_not_failing():
    """The extra alone: OpenTelemetry's proxies accept every call."""
    pytest.importorskip("opentelemetry.trace")
    observer = OpenTelemetryObserver()
    a_run(observer)  # nothing raises, and nothing is recorded anywhere


def test_the_adapter_through_the_real_api_with_in_memory_providers():
    observatory = in_memory_observatory()
    a_run(observatory.observer)

    assert sorted(span.name for span in observatory.spans) == sorted(
        [RUN_SPAN, RULE_SPAN, RULE_SPAN]
    )
    (run,) = observatory.named(RUN_SPAN)
    assert run.attributes["dq.run_key"] == RUN_KEY
    assert (
        run.attributes["dq.gate_passed"],
        run.attributes["dq.blocking"],
        run.attributes["dq.advisory"],
    ) == (False, 1, 0)
    assert run.status.status_code.name == "UNSET"  # the run did not raise
    rules = sorted(
        observatory.named(RULE_SPAN), key=lambda span: span.attributes["dq.rule"]
    )
    assert [span.attributes["dq.duration_ms"] for span in rules] == [842.1, 12.0]
    assert observatory.outcomes_counted(OUTCOME_COUNTER) == {
        (("dq.criticality", "error"), ("dq.status", "failed")): 1,
        (("dq.criticality", "n/a"), ("dq.status", "passed")): 3,
    }


def test_a_raised_rule_is_an_error_span_through_the_real_api():
    observatory = in_memory_observatory()
    observer = observatory.observer
    observer.run_started(RunStarted(RUN_KEY, JOB, "orders", 1))
    observer.rule_started(RuleStarted(RUN_KEY, "rule 0", "deequ", "default"))
    observer.rule_finished(
        RuleFinished(
            RUN_KEY, "rule 0", "deequ", "default", 3.0, error="RuntimeError: boom"
        )
    )
    observer.run_finished(
        RunFinished(RUN_KEY, JOB, "orders", 4.0, 1, 0, 0, error="RuntimeError: boom")
    )
    # The rule's span ended first, inside the run's.
    assert [span.name for span in observatory.spans] == [RULE_SPAN, RUN_SPAN]
    for span in observatory.spans:
        assert (span.status.status_code.name, span.status.description) == (
            "ERROR",
            "RuntimeError: boom",
        )
    assert observatory.outcomes_counted(OUTCOME_COUNTER) == {}  # no gate event


def test_the_adapter_names_itself_to_opentelemetry(monkeypatch):
    """Built without a tracer handed to it, it asks for one under its own name."""
    trace = pytest.importorskip("opentelemetry.trace")
    metrics = pytest.importorskip("opentelemetry.metrics")
    asked = []

    def tracer(name, version=None, **_options):
        asked.append(("tracer", name))
        return FakeTracer()

    def meter(name, version="", **_options):
        asked.append(("meter", name))
        return FakeMeter()

    monkeypatch.setattr(trace, "get_tracer", tracer)
    monkeypatch.setattr(metrics, "get_meter", meter)
    OpenTelemetryObserver()
    assert asked == [
        ("tracer", INSTRUMENTATION_NAME),
        ("meter", INSTRUMENTATION_NAME),
    ]


# ------------------------------------------------ no optional library at all


def test_the_module_imports_with_every_vendor_library_blocked():
    statement = """
import logging
logging.disable(logging.CRITICAL)
from dq.observability import (
    CompositeObserver, InMemoryObserver, LoggingObserver, RunFinished, RunObserver,
    RunStarted, RuleFinished, RuleStarted, observer_from_config,
)
assert isinstance(observer_from_config(None), LoggingObserver)
recorder = observer_from_config([{"type": "logging"}, {"type": "memory"}])
recorder.run_started(RunStarted(1, "job", "orders", 1))
recorder.gate_event(None)
print("observed")
"""
    result = _run_with_blocked_imports(
        "boto3",
        "databricks",
        "great_expectations",
        "opentelemetry",
        "pandas",
        "pyarrow",
        "pydeequ",
        "pyspark",
        "sqlalchemy",
        statement=statement,
    )
    assert result.returncode == 0, result.stderr
    assert "observed" in result.stdout
