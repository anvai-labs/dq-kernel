# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The one run core (ADR-014): a rule pack's lifecycle, with no engine in it.

A run is the same wherever it executes. Rules are applied in order, each to
the datasets it names; every outcome crosses the outcome boundary and is
stamped with the run key and the job id; the gate policy decides once, at the
end; and the run tells its observer and its notifier what happened. Keeping
that in one place is what lets a second runner reuse it rather than copy it
-- outcome normalization, the run key, notifiers, the gate policy and
observers kept in two places is the drift the layer redesign removed.

Before the first rule runs, the core asks of each engine the pack names
whether it can run here, and refuses the run when one cannot: what is
installed is judged, nothing is imported for the answer, and the refusal
says what is missing and which extras install it.

A runner binds compute. It supplies two things: how to load an engine for a
rule, and how to resolve a dataset's logical name to a frame. Everything else
-- the sink, the observer, the notifier, the policy, the job id, the
dataset's name, the clock -- the runner hands over as it is. The core imports
no catalog and no DataFrame library, so it runs with every extra blocked.

    from dq.run import run_rules

    result = run_rules(
        pack.rules,
        load_engine=lambda rule, run_key: load(rule.engine, rule.definition, run_key),
        dataset=lambda name: frames[name],
        run_key=run_key,
        job_id="dq-run",
        dataset_name="orders",
        policy=policy,
        observer=observer,
        notifier_factory=lambda: notifier_from_config(config.get("dqframework.notifiers")),
    )
"""

from __future__ import annotations

import logging
import time
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence

from dq.config import RuleRef
from dq.engine.registry import engines_that_cannot_run
from dq.exceptions import EngineNotFoundError, ValidationError
from dq.gate import GateDecision, evaluate_gate
from dq.notifier import GateEvent, GateNotifier
from dq.observability import (
    RunFinished,
    RunObserver,
    RunStarted,
    RuleFinished,
    RuleStarted,
    describe,
    elapsed_ms,
)
from dq.outcomes import MAX_BATCH_BYTES, MAX_OUTCOMES, CheckOutcome, normalize_outcomes

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    """What a run produced.

    Attributes:
        outcomes: The run's legacy outcome dictionaries, in rule order.
        decision: The gate's decision, or ``None`` when the run raised.
        event: The gate event the notifier received, or ``None`` likewise.
        run_key: The key every outcome and artifact of the run carries.
    """

    outcomes: list
    decision: Optional[GateDecision]
    event: Optional[GateEvent]
    run_key: int


def run_rules(
    rules: Sequence[RuleRef],
    *,
    load_engine: Callable[[Any, int], Any],
    dataset: Callable[[str], Any],
    run_key: int,
    job_id: str,
    dataset_name: str,
    policy: str,
    observer: RunObserver,
    notifier_factory: Callable[[], GateNotifier],
    clock: Callable[[], float] = time.perf_counter,
    sink: Any = None,
    report: Optional[str] = None,
    max_outcomes: int = MAX_OUTCOMES,
    max_bytes: int = MAX_BATCH_BYTES,
) -> RunResult:
    """Apply every rule and decide the gate: one run.

    Args:
        rules: A validated rule pack's rules, in order.
        load_engine: ``(rule, run_key) -> engine``: something with
            ``apply(frame, repository=...)``, loaded for the rule's engine.
        dataset: ``logical name -> frame``: the dataset a rule names.
        run_key: The run's key, stamped on every outcome and artifact.
        job_id: What identifies the job that ran, on every outcome.
        dataset_name: The name the run's evidence belongs to.
        policy: The gate policy (``dq.gate.POLICIES``).
        observer: Told what the run did (SPEC-014); never alters it.
        notifier_factory: Builds the notifier when there is something to
            announce; called after the run scope has exited, so a notifier
            that cannot be built costs no run.
        clock: Reads a monotonic clock in seconds, for durations.
        sink: Where engines persist; ``None`` persists nothing.
        report: Reference to the run's ``dq-report/v1`` document, if one is
            written; carried on the gate event.
        max_outcomes: The most outcomes a run may produce before it is
            refused. The kernel's own bound by default; a runner whose module
            publishes the bound as its own passes it, so that turning the
            runner's bound turns the run's.
        max_bytes: The same for the serialized size of the whole batch.

    Returns:
        The :class:`RunResult`.

    Raises:
        EngineNotFoundError: Before the first rule runs, when an engine the
            pack names cannot run here. It says what is not installed and
            the extras that install it, for every such engine at once.
        Exception: Whatever a rule, the sink, or the gate policy raised. The
            run is still announced as finished, with the error, before the
            exception propagates.
    """
    cumulative_metrics: list = []
    cumulative_bytes = 0
    applications = sum(len(rule.dataframes) for rule in rules)
    made = 0

    def observed(notification: str, event) -> None:
        """Tell the observer. Observation never alters the run."""
        try:
            getattr(observer, notification)(event)
        except Exception as error:  # noqa: BLE001 - observation is fail-open
            logger.warning(
                "observer %s failed on %s for run_key=%d: %s",
                type(observer).__name__,
                notification,
                run_key,
                error,
            )

    started = clock()
    observed(
        "run_started",
        RunStarted(
            run_key=run_key,
            job_id=job_id,
            dataset=dataset_name,
            rules=applications,
        ),
    )
    try:
        # What the pack's engines need is judged before the first rule runs,
        # as the pack's shape is (ADR-001): an engine that cannot run here
        # is refused once, for the whole pack, before anything is loaded
        # and no data is read.
        unmet = engines_that_cannot_run(rule.engine for rule in rules)
        if unmet:
            raise EngineNotFoundError("\n".join(unmet))
        scope = sink.run_scope(run_key) if sink is not None else nullcontext()
        with scope:
            for rule in rules:
                engine = load_engine(rule, run_key)
                name = rule.name or f"rule {rule.index}"
                for df_name in rule.dataframes:
                    dataframe = dataset(df_name)
                    observed(
                        "rule_started",
                        RuleStarted(
                            run_key=run_key,
                            rule=name,
                            engine=rule.engine,
                            dataset=df_name,
                        ),
                    )
                    rule_started = clock()
                    try:
                        summary_metrics = engine.apply(dataframe, repository=sink)
                        produced = len(cumulative_metrics)
                        for outcome in normalize_outcomes(summary_metrics):
                            metric = outcome.to_legacy()
                            metric["ts"] = run_key
                            metric["jobid"] = job_id
                            enriched = CheckOutcome.from_legacy(metric)
                            cumulative_bytes += enriched.serialized_size
                            if (
                                len(cumulative_metrics) >= max_outcomes
                                or cumulative_bytes > max_bytes
                            ):
                                raise ValidationError(
                                    "Execution outcomes exceed summary limits"
                                )
                            if not outcome.success:
                                logger.warning(
                                    "Check failed: %s",
                                    metric.get("check", "unnamed"),
                                )
                            cumulative_metrics.append(metric)
                    except BaseException as error:
                        # Anything at all, an interrupt included: the rule
                        # ended, and that is what the observer is told.
                        made += 1
                        observed(
                            "rule_finished",
                            RuleFinished(
                                run_key=run_key,
                                rule=name,
                                engine=rule.engine,
                                dataset=df_name,
                                duration_ms=elapsed_ms(rule_started, clock()),
                                error=describe(error),
                            ),
                        )
                        raise
                    made += 1
                    checks = cumulative_metrics[produced:]
                    observed(
                        "rule_finished",
                        RuleFinished(
                            run_key=run_key,
                            rule=name,
                            engine=rule.engine,
                            dataset=df_name,
                            duration_ms=elapsed_ms(rule_started, clock()),
                            outcomes=len(checks),
                            failed=tuple(
                                check["check"]
                                for check in checks
                                if check.get("success") is not True
                            ),
                        ),
                    )
        decision = evaluate_gate(cumulative_metrics, policy)
    except BaseException as error:
        # The run ended, badly. It is still announced as ended, with the
        # error: a run that died is the one most in need of explaining.
        observed(
            "run_finished",
            RunFinished(
                run_key=run_key,
                job_id=job_id,
                dataset=dataset_name,
                duration_ms=elapsed_ms(started, clock()),
                rules=made,
                outcomes=len(cumulative_metrics),
                failed=_count_failed(cumulative_metrics),
                error=describe(error),
            ),
        )
        raise
    observed(
        "run_finished",
        RunFinished(
            run_key=run_key,
            job_id=job_id,
            dataset=dataset_name,
            duration_ms=elapsed_ms(started, clock()),
            rules=made,
            outcomes=len(cumulative_metrics),
            failed=_count_failed(cumulative_metrics),
            blocking=len(decision.blocking),
            advisory=len(decision.advisory),
        ),
    )
    event = GateEvent.from_outcomes(
        run_key=run_key,
        dataset=dataset_name,
        outcomes=cumulative_metrics,
        ts=run_key,
        job_id=job_id,
        report=report,
    )
    observed("gate_event", event)
    _announce(notifier_factory, event, run_key)
    return RunResult(cumulative_metrics, decision, event, run_key)


def _count_failed(metrics) -> int:
    """Return how many of a run's outcomes failed."""
    return sum(1 for metric in metrics if metric.get("success") is not True)


def _announce(
    notifier_factory: Callable[[], GateNotifier], event: GateEvent, run_key: int
) -> None:
    """Emit the run's gate event; a failure to deliver never raises (ADR-008).

    A notifier that cannot be built is another matter: that is a
    configuration error, and it propagates.
    """
    notifier = notifier_factory()
    try:
        notifier.notify(event)
    except Exception as error:  # noqa: BLE001 - delivery is fail-open
        logger.warning(
            "gate-event notifier %s failed for run_key=%d: %s",
            type(notifier).__name__,
            run_key,
            error,
        )
