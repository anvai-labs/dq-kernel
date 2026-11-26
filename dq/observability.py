# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Run observability (SPEC-014): one port through which a run explains itself.

The run core tells an observer five things: a run started, a rule started, a
rule finished, the run finished, and what the gate decided. What an observer
does with that is its own affair -- write log lines, open spans -- and none of
it can change the run. Every notification is an immutable record, and an
observer that raises is logged and swallowed, as a notifier is (ADR-008).

Observers are configured like notifiers, under ``dqframework.observers``.

This module imports no optional library. The OpenTelemetry adapter imports
``opentelemetry`` when it is built, never when this module is.
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Optional, Tuple

from dq.exceptions import ConfigurationError
from dq.notifier import GateEvent
from dq.report import framework_version
from dq.utils.imports import require_extra

#: The environment variable that selects JSON log lines from the logging
#: observer; any other value, or none, gives one line of text.
LOG_FORMAT_VARIABLE = "DQ_LOG_FORMAT"
JSON_LINES = "json"

INSTRUMENTATION_NAME = "dq-kernel"
RUN_SPAN = "dq.run"
RULE_SPAN = "dq.rule"
OUTCOME_COUNTER = "dq.outcomes"

#: Attribute names on spans and on the counter.
STATUS = "dq.status"
CRITICALITY = "dq.criticality"
PASSED = "passed"
FAILED = "failed"
ERROR = "error"
WARN = "warn"
#: What a passed outcome carries as its criticality: the gate event does not
#: say what a check's criticality was when it passed, and one attribute set
#: is kept across the counter.
UNREPORTED = "n/a"

OTEL_PACKAGE = "opentelemetry-api"
OTEL_PIN = "opentelemetry-api>=1.25,<2"

logger = logging.getLogger("dq.run_events")


# ------------------------------------------------------------------ events


@dataclass(frozen=True, slots=True)
class RunStarted:
    """A run is about to apply its rules."""

    run_key: int
    job_id: str
    dataset: str
    rules: int


@dataclass(frozen=True, slots=True)
class RuleStarted:
    """A rule is about to be applied to one dataset."""

    run_key: int
    rule: str
    engine: str
    dataset: str


@dataclass(frozen=True, slots=True)
class RuleFinished:
    """A rule was applied to one dataset, or raised.

    Attributes:
        duration_ms: How long the application took, measured by the run core.
        outcomes: How many checks it produced.
        failed: The names of the checks that failed, in order.
        error: The exception's type and message, when the application raised.
    """

    run_key: int
    rule: str
    engine: str
    dataset: str
    duration_ms: float
    outcomes: int = 0
    failed: Tuple[str, ...] = ()
    error: Optional[str] = None


@dataclass(frozen=True, slots=True)
class RunFinished:
    """A run ended, well or badly.

    Attributes:
        duration_ms: How long the run took, measured by the run core.
        rules: How many rule applications it made.
        outcomes: How many checks it produced.
        failed: How many of those failed.
        blocking: How many failures block under the gate policy, or ``None``
            when the run raised and no decision was reached.
        advisory: How many failures are advisory, or ``None`` likewise.
        error: The exception's type and message, when the run raised.
    """

    run_key: int
    job_id: str
    dataset: str
    duration_ms: float
    rules: int
    outcomes: int
    failed: int
    blocking: Optional[int] = None
    advisory: Optional[int] = None
    error: Optional[str] = None

    @property
    def gate_passed(self) -> Optional[bool]:
        """Whether the gate opened, or ``None`` when no decision was reached."""
        if self.blocking is None:
            return None
        return self.blocking == 0 and self.error is None


def describe(error: BaseException) -> str:
    """Return an exception as a line of text: its type and its message."""
    return f"{type(error).__name__}: {error}"


def elapsed_ms(started: float, finished: float) -> float:
    """Return the milliseconds between two readings of one clock, never below 0."""
    return max((finished - started) * 1000.0, 0.0)


# --------------------------------------------------------------- the port


class RunObserver(ABC):
    """What a run tells about itself, after the fact.

    Implementations must not raise. The run core logs and swallows anything
    an observer raises, so observation can never alter a run's outcome or its
    evidence.
    """

    @abstractmethod
    def run_started(self, event: RunStarted) -> None:
        """A run is about to apply its rules."""

    @abstractmethod
    def rule_started(self, event: RuleStarted) -> None:
        """A rule is about to be applied to one dataset."""

    @abstractmethod
    def rule_finished(self, event: RuleFinished) -> None:
        """A rule was applied to one dataset, or raised."""

    @abstractmethod
    def run_finished(self, event: RunFinished) -> None:
        """A run ended, well or badly."""

    @abstractmethod
    def gate_event(self, event: GateEvent) -> None:
        """The gate decided; the same record the notifier receives."""


NOTIFICATIONS = (
    "run_started",
    "rule_started",
    "rule_finished",
    "run_finished",
    "gate_event",
)


class InMemoryObserver(RunObserver):
    """Keeps every notification, in order; never fails.

    For tests, and for a platform that wants the events themselves.
    """

    def __init__(self):
        #: ``(notification, event)`` in the order they arrived.
        self.events: list[tuple[str, object]] = []

    @property
    def notifications(self) -> tuple[str, ...]:
        """The notifications received, in order."""
        return tuple(name for name, _event in self.events)

    def run_started(self, event: RunStarted) -> None:
        self.events.append(("run_started", event))

    def rule_started(self, event: RuleStarted) -> None:
        self.events.append(("rule_started", event))

    def rule_finished(self, event: RuleFinished) -> None:
        self.events.append(("rule_finished", event))

    def run_finished(self, event: RunFinished) -> None:
        self.events.append(("run_finished", event))

    def gate_event(self, event: GateEvent) -> None:
        self.events.append(("gate_event", event))


class CompositeObserver(RunObserver):
    """Fan-out to several observers; each failure is logged and swallowed."""

    def __init__(self, *observers: RunObserver):
        self._observers = observers

    def _tell(self, notification: str, event) -> None:
        for observer in self._observers:
            try:
                getattr(observer, notification)(event)
            except Exception as error:  # noqa: BLE001 - observation is fail-open
                logger.warning(
                    "observer %s failed on %s for run_key=%s: %s",
                    type(observer).__name__,
                    notification,
                    getattr(event, "run_key", None),
                    error,
                )

    def run_started(self, event: RunStarted) -> None:
        self._tell("run_started", event)

    def rule_started(self, event: RuleStarted) -> None:
        self._tell("rule_started", event)

    def rule_finished(self, event: RuleFinished) -> None:
        self._tell("rule_finished", event)

    def run_finished(self, event: RunFinished) -> None:
        self._tell("run_finished", event)

    def gate_event(self, event: GateEvent) -> None:
        self._tell("gate_event", event)


# -------------------------------------------------------------- logging


class LoggingObserver(RunObserver):
    """The default observer: one log line per notification.

    Every line names the run, and the rule and the engine where one applies.
    The lines go to the ``dq.run_events`` logger.

    Args:
        log_format: ``"json"`` for one JSON object per line, ``"text"``
            otherwise; read from :data:`LOG_FORMAT_VARIABLE` when omitted.
    """

    def __init__(self, *, log_format: Optional[str] = None):
        chosen = log_format or os.environ.get(LOG_FORMAT_VARIABLE, "") or ""
        self._json = chosen.strip().lower() == JSON_LINES
        self._logger = logging.getLogger("dq.run_events")

    def _write(self, notification: str, event, text: str) -> None:
        if self._json:
            fields = event.to_dict() if isinstance(event, GateEvent) else asdict(event)
            self._logger.info(
                json.dumps({"event": notification, **fields}, sort_keys=True)
            )
        else:
            self._logger.info(text)

    def run_started(self, event: RunStarted) -> None:
        self._write(
            "run_started",
            event,
            f"run_started run_key={event.run_key} job_id={event.job_id} "
            f"dataset={event.dataset} rules={event.rules}",
        )

    def rule_started(self, event: RuleStarted) -> None:
        self._write(
            "rule_started",
            event,
            f"rule_started run_key={event.run_key} rule={event.rule} "
            f"engine={event.engine} dataset={event.dataset}",
        )

    def rule_finished(self, event: RuleFinished) -> None:
        self._write(
            "rule_finished",
            event,
            f"rule_finished run_key={event.run_key} rule={event.rule} "
            f"engine={event.engine} dataset={event.dataset} "
            f"duration_ms={event.duration_ms:.1f} outcomes={event.outcomes} "
            f"failed={len(event.failed)}"
            + (f" error={event.error}" if event.error else ""),
        )

    def run_finished(self, event: RunFinished) -> None:
        self._write(
            "run_finished",
            event,
            f"run_finished run_key={event.run_key} job_id={event.job_id} "
            f"dataset={event.dataset} duration_ms={event.duration_ms:.1f} "
            f"rules={event.rules} outcomes={event.outcomes} failed={event.failed}"
            + (
                f" blocking={event.blocking} advisory={event.advisory}"
                if event.blocking is not None
                else ""
            )
            + (f" error={event.error}" if event.error else ""),
        )

    def gate_event(self, event: GateEvent) -> None:
        # The notifier announces this event too (ADR-008); this line is the
        # run's own trace of it, in the same words as every other line here.
        self._write(
            "gate_event",
            event,
            f"gate_event run_key={event.run_key} dataset={event.dataset} "
            f"status={event.status} failed={len(event.failed)}/{event.total} "
            f"blocking={len(event.blocked)} advisory={len(event.warned)}",
        )


# --------------------------------------------------------- OpenTelemetry


def _opentelemetry():
    """Return OpenTelemetry's ``(trace, metrics)`` modules, or name the extra."""
    trace = require_extra(
        "opentelemetry.trace",
        OTEL_PACKAGE,
        "otel",
        pin=OTEL_PIN,
        note="the SDK, exporter and providers are the application's",
        kind="observer",
    )
    metrics = require_extra(
        "opentelemetry.metrics", OTEL_PACKAGE, "otel", pin=OTEL_PIN, kind="observer"
    )
    return trace, metrics


def _instrumentation_version() -> Optional[str]:
    version = framework_version()
    return None if version == "source" else version


def _error_status(trace=None):
    """Return OpenTelemetry's error status for :meth:`Span.set_status`.

    With a tracer standing in for a real one and no OpenTelemetry installed,
    the word is returned instead: the stand-in only records what it was asked.
    """
    if trace is None:
        try:
            import opentelemetry.trace as trace  # noqa: PLC0415 - optional
        except ImportError:
            return ERROR
    return trace.StatusCode.ERROR


class OpenTelemetryObserver(RunObserver):
    """Spans and a counter through OpenTelemetry's API (the ``otel`` extra).

    One span per run and one per rule application. The run span carries the
    gate decision; a rule or run that raised ends its span in the error
    status, with the error as its description. One counter, ``dq.outcomes``,
    counts outcomes by ``dq.status`` and ``dq.criticality``.

    The extra provides the API and nothing else. Until the application
    configures an SDK's providers, the API's own proxies accept every call
    and record nothing.

    Args:
        tracer: A tracer, for one already in hand; OpenTelemetry's provider
            is asked for one when omitted.
        meter: Likewise.
    """

    def __init__(self, tracer=None, meter=None):
        trace = None
        version = _instrumentation_version()
        if tracer is None or meter is None:
            trace, metrics = _opentelemetry()
            tracer = tracer or trace.get_tracer(INSTRUMENTATION_NAME, version)
            meter = meter or metrics.get_meter(INSTRUMENTATION_NAME, version)
        self._tracer = tracer
        self._runs: dict = {}
        self._rules: dict = {}
        self._outcomes = meter.create_counter(
            OUTCOME_COUNTER,
            unit="1",
            description="Data-quality outcomes by status and criticality",
        )
        self._error_status = _error_status(trace)

    @staticmethod
    def _rule_key(event) -> tuple:
        return (event.run_key, event.rule, event.dataset)

    def run_started(self, event: RunStarted) -> None:
        self._runs[event.run_key] = self._tracer.start_span(
            RUN_SPAN,
            attributes={
                "dq.run_key": event.run_key,
                "dq.job_id": event.job_id,
                "dq.dataset": event.dataset,
                "dq.rules": event.rules,
            },
        )

    def rule_started(self, event: RuleStarted) -> None:
        self._rules[self._rule_key(event)] = self._tracer.start_span(
            RULE_SPAN,
            attributes={
                "dq.run_key": event.run_key,
                "dq.rule": event.rule,
                "dq.engine": event.engine,
                "dq.dataset": event.dataset,
            },
        )

    def rule_finished(self, event: RuleFinished) -> None:
        # Taken off the open spans, so a notification that repeats can never
        # end a span twice: OpenTelemetry would only log that, not raise.
        span = self._rules.pop(self._rule_key(event), None)
        if span is None:
            return
        span.set_attribute("dq.duration_ms", event.duration_ms)
        span.set_attribute("dq.outcomes", event.outcomes)
        span.set_attribute("dq.failed", len(event.failed))
        if event.error:
            span.set_status(self._error_status, event.error)
        span.end()

    def run_finished(self, event: RunFinished) -> None:
        span = self._runs.pop(event.run_key, None)
        if span is None:
            return
        span.set_attribute("dq.duration_ms", event.duration_ms)
        span.set_attribute("dq.outcomes", event.outcomes)
        span.set_attribute("dq.failed", event.failed)
        if event.error:
            span.set_status(self._error_status, event.error)
        else:
            span.set_attribute("dq.gate_passed", event.gate_passed)
            span.set_attribute("dq.blocking", event.blocking)
            span.set_attribute("dq.advisory", event.advisory)
        span.end()

    def gate_event(self, event: GateEvent) -> None:
        passed = max(event.total - len(event.failed), 0)
        if passed:
            self._outcomes.add(passed, {STATUS: PASSED, CRITICALITY: UNREPORTED})
        if event.blocked:
            self._outcomes.add(len(event.blocked), {STATUS: FAILED, CRITICALITY: ERROR})
        if event.warned:
            self._outcomes.add(len(event.warned), {STATUS: FAILED, CRITICALITY: WARN})


# ---------------------------------------------------------- configuration


def observer_from_config(observer_config) -> RunObserver:
    """Build an observer from configuration (fail closed on unknown types).

    Args:
        observer_config: The ``dqframework.observers`` list; empty means the
            default logging observer.

    Returns:
        One observer: the single one configured, or a composite of several.

    Raises:
        ConfigurationError: If an entry's ``type`` is not ``logging``,
            ``memory`` or ``otel``.
    """
    if not observer_config:
        return LoggingObserver()
    observers: list[RunObserver] = []
    for entry in observer_config:
        kind = entry.get("type", None) if isinstance(entry, dict) else None
        if kind == "logging":
            observers.append(LoggingObserver())
        elif kind == "memory":
            observers.append(InMemoryObserver())
        elif kind == "otel":
            observers.append(OpenTelemetryObserver())
        else:
            raise ConfigurationError(
                f"observer type {kind!r} is not supported; use 'logging', "
                "'memory' or 'otel'"
            )
    if len(observers) == 1:
        return observers[0]
    return CompositeObserver(*observers)
