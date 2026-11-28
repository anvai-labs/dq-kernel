# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Engine conformance suite (SPEC-011, ADR-012).

ADR-012 defines what an engine must do, and the built-in engines are held
to it by this repository's own tests. This module is the same contract in
a form an engine author outside this repository can run in their test
suite:

    from dq.sdk.conformance import assert_engine_conforms
    from dq.sinks import InMemorySink

    def test_my_engine_conforms(spark):
        frame = spark.createDataFrame([(1, None), (2, "x")], ["id", "name"])
        assert_engine_conforms(
            lambda dqts=None: MyEngine(make_config(), dqts=dqts), frame, InMemorySink
        )

The engine factory builds a fresh engine and accepts a ``dqts`` keyword,
as the framework constructs engines. The frame should make the engine
report at least one passing and one failing check, so there is something
to check. ``sink_factory`` is optional; without it persistence is not
checked.

``reordered_factory`` is optional too; without it attribution under
reordering is not checked. It builds the same engine with the same checks
configured in another order. An engine must then report the same outcome
under each check's name: a vendor's results are matched to checks by what
they carry, never by where they sit, and an engine that reads position as
identity swaps outcomes the moment the vendor returns them in another
order. The checks therefore need labels that mean the same check in
both configurations. When the labels themselves change, the suite says
it could not compare. When a label keeps its text but means another
check -- ``labels_for_checks`` names a second unnamed check of one kind
by its position -- nothing sets that apart from a real swap, and the
suite reports it; give such checks names of their own.

``misnamed_factory`` is optional too; without it column references are
not checked (SPEC-025). It builds the engine for a rule that names a
column by anything but a plain identifier -- ``s.t`` beside a struct
``s``, or ``x-y`` beside columns ``x`` and ``y``. The engine must refuse
it with a ``ConfigurationError`` before anything runs: Spark reads such a
string as an expression, and measured on Spark 3.5 two engines answered
"passed" to a not-null check over data where that column held a null.

Nothing here imports an engine or a vendor library.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple

from dq.engine.dq_engine import DQEngine, capability_problems
from dq.exceptions import ConfigurationError
from dq.outcomes import normalize_outcomes

#: The run key the suite injects to see where artifacts land.
RUN_KEY = 1_700_000_000_000

#: The artifacts a persisting engine writes.
ARTIFACT_KINDS = frozenset({"metrics", "verifications"})

EngineFactory = Callable[..., DQEngine]


@dataclass(frozen=True)
class ConformanceReport:
    """What the suite checked and what it found.

    Attributes:
        checks: Names of the checks that ran.
        problems: Violations, each naming the check that found it. Empty
            when the engine conforms.
        notes: Observations that are not violations, such as a check that
            could not run.
    """

    checks: Tuple[str, ...]
    problems: Tuple[str, ...]
    notes: Tuple[str, ...] = ()

    @property
    def conforms(self) -> bool:
        return not self.problems

    def summary(self) -> str:
        """Return the report as readable text."""
        lines = [f"{len(self.checks)} check(s) run: {', '.join(self.checks)}"]
        lines += [f"  note: {note}" for note in self.notes]
        lines += [f"  PROBLEM: {problem}" for problem in self.problems]
        lines.append("conforms" if self.conforms else "does NOT conform")
        return "\n".join(lines)


def _problem(check: str, detail: str) -> str:
    return f"{check}: {detail}"


def _run(
    engine: DQEngine, frame, repository=None
) -> Tuple[List[dict], Optional[BaseException]]:
    """Apply an engine and return ``(legacy outcomes, error)``.

    Exactly one of the two is non-empty. Any exception is returned rather
    than raised: an engine that fails is a finding, not a crash of the
    suite.
    """
    try:
        metrics = engine.apply(frame, repository=repository)
        return [outcome.to_legacy() for outcome in normalize_outcomes(metrics)], None
    except Exception as error:  # noqa: BLE001 - the failure is the finding
        return [], error


def _why(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}"


def _misnamed_column_problems(misnamed_factory: Callable[[], DQEngine]) -> List[str]:
    """Return what is wrong with an engine's answer to a column it cannot honour.

    The factory builds the engine for a rule that names a column by
    anything but a plain identifier. Refusing with a ``ConfigurationError``
    is the one right answer; building the engine is the finding, since
    Spark would read the name as an expression and the engine would
    measure something else (SPEC-025).
    """
    check = "column_names_are_refused"
    try:
        misnamed_factory()
    except ConfigurationError:
        return []
    except Exception as error:  # noqa: BLE001 - the refusal is the finding
        return [
            _problem(
                check,
                f"refused a rule that names a column by anything but a plain "
                f"identifier with {_why(error)}; refuse with a ConfigurationError, "
                "before anything runs",
            )
        ]
    return [
        _problem(
            check,
            "built an engine for a rule that names a column by anything but a "
            "plain identifier: Spark reads such a string as an expression, and "
            "the engine would measure something else",
        )
    ]


def _by_label(outcomes: List[dict]) -> dict:
    return {outcome.get("check"): outcome for outcome in outcomes}


def _check_attribution_under_reordering(
    reordered_factory: Optional[EngineFactory],
    frame,
    outcomes: List[dict],
    checks: List[str],
    problems: List[str],
    notes: List[str],
) -> None:
    """Check that each check keeps its outcome when the checks are reordered.

    Only a comparison that was actually made is recorded as a check that
    ran; when the two runs cannot be lined up, the report says so instead.
    """
    if reordered_factory is None:
        notes.append(
            "attribution under reordering was not checked: no "
            "reordered_factory was given"
        )
        return
    reordered, error = _run(reordered_factory(), frame)
    if error is not None:
        problems.append(
            _problem(
                "attribution_survives_reordering",
                "the engine built with its checks in another order failed: "
                f"{_why(error)}",
            )
        )
        return
    by_label = _by_label(outcomes)
    in_another_order = _by_label(reordered)
    if set(by_label) != set(in_another_order):
        notes.append(
            "attribution under reordering was not checked: the check names "
            "changed with the order; a check named by its position cannot be "
            "followed from one run to the next"
        )
        return
    checks.append("attribution_survives_reordering")
    moved = sorted(
        label for label in by_label if by_label[label] != in_another_order[label]
    )
    if moved:
        problems.append(
            _problem(
                "attribution_survives_reordering",
                "the same check reported a different outcome when the checks "
                "were configured in another order: "
                + ", ".join(moved)
                + "; match each result to its check by what it carries, "
                "never by its position",
            )
        )


def conformance_problems(
    engine_factory: EngineFactory,
    frame,
    sink_factory: Optional[Callable[[], Any]] = None,
    reordered_factory: Optional[EngineFactory] = None,
    misnamed_factory: Optional[Callable[[], DQEngine]] = None,
) -> ConformanceReport:
    """Run an engine through the conformance suite.

    Args:
        engine_factory: Builds a fresh engine; must accept a ``dqts``
            keyword.
        frame: A frame the engine validates, yielding at least one passing
            and one failing check.
        sink_factory: Builds a fresh sink with a ``writes()`` method
            returning ``{(metric_type, key): rows}`` (``InMemorySink``).
        reordered_factory: Builds a fresh engine with the same checks
            configured in another order. Given it, the suite checks that
            every check keeps its outcome under its own name. The labels
            must mean the same check in both configurations: an explicit
            name, or no two of one kind without a column.
        misnamed_factory: Builds the engine for a rule that names a column
            by anything but a plain identifier. Given it, the suite checks
            that the engine refuses it with a ``ConfigurationError``
            before anything runs (SPEC-025).

    Returns:
        The :class:`ConformanceReport`.
    """
    checks: List[str] = []
    problems: List[str] = []
    notes: List[str] = []
    engine = engine_factory()
    engine_name = type(engine).__name__

    checks.append("manifest_is_valid")
    problems += [
        _problem("manifest_is_valid", found)
        for found in capability_problems(getattr(type(engine), "CAPABILITIES", None))
    ]

    checks.append("uses_the_template")
    if "apply" in type(engine).__dict__:
        problems.append(
            _problem(
                "uses_the_template",
                f"{engine_name} overrides apply(); implement _prepare and "
                "_execute so the lifecycle is the framework's",
            )
        )

    if misnamed_factory is None:
        notes.append(
            "column references were not checked: no misnamed_factory was given"
        )
    else:
        checks.append("column_names_are_refused")
        problems += _misnamed_column_problems(misnamed_factory)

    checks.append("outcomes_pass_the_outcome_gate")
    # An empty batch never gets this far as outcomes: the outcome gate
    # refuses it, and that refusal is reported here as the finding it is.
    outcomes, error = _run(engine, frame)
    if error is not None:
        problems.append(
            _problem("outcomes_pass_the_outcome_gate", f"apply() failed: {_why(error)}")
        )

    if outcomes:
        checks.append("check_names_are_unique")
        names = [outcome.get("check") for outcome in outcomes]
        blank = [
            name for name in names if not isinstance(name, str) or not name.strip()
        ]
        repeated = sorted({str(name) for name in names if names.count(name) > 1})
        if blank:
            problems.append(
                _problem(
                    "check_names_are_unique",
                    f"{len(blank)} outcome(s) have no check name",
                )
            )
        if repeated:
            problems.append(
                _problem(
                    "check_names_are_unique",
                    "check names repeat, so evidence cannot be attributed: "
                    + ", ".join(repeated),
                )
            )

        checks.append("criticality_is_canonical")
        level = engine.CAPABILITIES.get("criticality")
        details = [outcome.get("details") for outcome in outcomes]
        if level in ("native", "derived"):
            silent = [
                outcome.get("check")
                for outcome, payload in zip(outcomes, details)
                if not isinstance(payload, Mapping) or "criticality" not in payload
            ]
            if silent:
                problems.append(
                    _problem(
                        "criticality_is_canonical",
                        f"{len(silent)} of {len(outcomes)} outcome(s) carry no "
                        f"details.criticality although the engine declares "
                        f"criticality={level!r}; first: {silent[0]!r}",
                    )
                )
        foreign = sorted(
            {
                str(payload["criticality"])
                for payload in details
                if isinstance(payload, Mapping)
                and payload.get("criticality") is not None
                and payload["criticality"] not in ("error", "warn")
            }
        )
        if foreign:
            problems.append(
                _problem(
                    "criticality_is_canonical",
                    "criticality must be spelled 'error' or 'warn' (normalize it "
                    f"through dq.engine.checkspec); found {', '.join(foreign)!r}",
                )
            )

        checks.append("outcomes_are_deterministic")
        again, second_error = _run(engine_factory(), frame)
        if second_error is not None:
            problems.append(
                _problem(
                    "outcomes_are_deterministic",
                    f"a second run failed: {_why(second_error)}",
                )
            )
        elif again != outcomes:
            problems.append(
                _problem(
                    "outcomes_are_deterministic",
                    "a second run over the same frame reported different outcomes",
                )
            )
        verdicts = {outcome.get("success") for outcome in outcomes}
        if verdicts != {True, False}:
            notes.append(
                f"every outcome {'passed' if verdicts == {True} else 'failed'}; a frame "
                "that makes the engine report both gives the suite more to check"
            )
        _check_attribution_under_reordering(
            reordered_factory, frame, outcomes, checks, problems, notes
        )

    if not outcomes:
        # Nothing a persistence check could add: every later run would only
        # repeat this failure.
        notes.append(
            "persistence was not checked: the engine produced no valid outcomes"
        )
        if reordered_factory is not None:
            notes.append(
                "attribution under reordering was not checked: the engine "
                "produced no valid outcomes"
            )
        return ConformanceReport(tuple(checks), tuple(problems), tuple(notes))
    if sink_factory is None:
        notes.append("persistence was not checked: no sink_factory was given")
        return ConformanceReport(tuple(checks), tuple(problems), tuple(notes))

    checks.append("persistence_honours_the_capability")
    persists = engine.CAPABILITIES.get("persistence")
    findings_before = len(problems)
    if persists is False:
        _, refusal = _run(engine_factory(), frame, sink_factory())
        if refusal is None:
            problems.append(
                _problem(
                    "persistence_honours_the_capability",
                    "declares persistence=False but accepted a repository without refusing",
                )
            )
        elif (
            not isinstance(refusal, ConfigurationError)
            or "persist" not in str(refusal).lower()
        ):
            problems.append(
                _problem(
                    "persistence_honours_the_capability",
                    "refused a repository with "
                    f"{_why(refusal)}; refuse with a ConfigurationError that says "
                    "the engine cannot persist evidence",
                )
            )
    else:
        sink = sink_factory()
        _, failed = _run(engine_factory(), frame, sink)
        kinds = {kind for kind, _ in sink.writes()}
        if failed is not None:
            problems.append(
                _problem(
                    "persistence_honours_the_capability",
                    f"declares persistence=True but failed with a repository: {_why(failed)}",
                )
            )
        elif kinds != ARTIFACT_KINDS:
            problems.append(
                _problem(
                    "persistence_honours_the_capability",
                    "declares persistence=True but wrote "
                    + (", ".join(sorted(kinds)) if kinds else "nothing")
                    + "; a persisting engine writes both metrics and verifications",
                )
            )

    if persists is not False and len(problems) == findings_before:
        checks.append("artifacts_carry_the_injected_run_key")
        sink = sink_factory()
        _, failed = _run(engine_factory(dqts=RUN_KEY), frame, sink)
        keys = {key for _, key in sink.writes()}
        if failed is not None:
            problems.append(
                _problem(
                    "artifacts_carry_the_injected_run_key",
                    f"the run failed: {_why(failed)}",
                )
            )
        elif keys != {RUN_KEY}:
            problems.append(
                _problem(
                    "artifacts_carry_the_injected_run_key",
                    f"artifacts landed under {sorted(keys) or 'no key'}, not under the "
                    f"run key the framework injected ({RUN_KEY})",
                )
            )

    return ConformanceReport(tuple(checks), tuple(problems), tuple(notes))


def assert_engine_conforms(
    engine_factory: EngineFactory,
    frame,
    sink_factory: Optional[Callable[[], Any]] = None,
    reordered_factory: Optional[EngineFactory] = None,
    misnamed_factory: Optional[Callable[[], DQEngine]] = None,
) -> ConformanceReport:
    """Assert that an engine conforms to the engine contract (ADR-012).

    Raises:
        AssertionError: Listing every violation found, each naming the
            check that found it.
    """
    report = conformance_problems(
        engine_factory, frame, sink_factory, reordered_factory, misnamed_factory
    )
    assert report.conforms, f"does not conform:\n{report.summary()}"
    return report
