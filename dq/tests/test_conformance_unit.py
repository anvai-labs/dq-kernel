# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the engine conformance suite (SPEC-011).

A conformance suite is only worth having if it refuses a non-conforming
engine, so most of these tests are deliberately broken engines. They run
on a fake frame with no engine and no Spark.
"""

import time
from types import SimpleNamespace

import pytest
from pyhocon import ConfigFactory

from dq.engine.checkspec import build_result
from dq.engine.dq_engine import DQEngine
from dq.exceptions import ConfigurationError
from dq.sdk.conformance import (
    RUN_KEY,
    ConformanceReport,
    assert_engine_conforms,
    conformance_problems,
)
from dq.sinks import InMemorySink
from dq.tests.test_package_contract import _run_with_blocked_imports

MIXED = [("steady", True, "error"), ("broken", False, "warn")]


class FakeSpark:
    """Just enough of a Spark session for the shared artifact path."""

    def createDataFrame(self, rows, schema):
        tables = SimpleNamespace(
            collect=lambda: [
                SimpleNamespace(
                    asDict=lambda recursive=True, row=row: dict(zip("abcde", row))
                )
                for row in rows
            ]
        )
        return tables


FRAME = SimpleNamespace(sparkSession=FakeSpark())
CONFIG = ConfigFactory.from_dict({"name": "fake", "engine": "fake"})


def factory(engine_class, outcomes=MIXED, **engine_kwargs):
    return lambda dqts=None: engine_class(
        CONFIG, dqts=dqts, outcomes=outcomes, **engine_kwargs
    )


class ExampleEngine(DQEngine):
    """A conforming engine: hooks, canonical severity, the shared path."""

    CAPABILITIES = {"criticality": "native", "failed_rows": True}

    def __init__(self, config, dqts=None, outcomes=MIXED):
        super().__init__(config, dqts)
        self.outcomes = outcomes

    def _prepare(self, dataframe):
        return self.outcomes

    def _execute(self, dataframe, prepared):
        return [
            build_result(name, ok, {"criticality": criticality})
            for name, ok, criticality in prepared
        ]


def problems_of(engine_class, outcomes=MIXED, sink=InMemorySink, **engine_kwargs):
    return conformance_problems(
        factory(engine_class, outcomes, **engine_kwargs), FRAME, sink
    )


def reordered(engine_class, **engine_kwargs):
    """A factory for the same engine with its checks in another order."""
    return lambda dqts=None: engine_class(
        CONFIG, dqts=dqts, outcomes=list(reversed(MIXED)), **engine_kwargs
    )


# ------------------------------------------------------------- a good engine


NOT_MISNAMED = "column references were not checked: no misnamed_factory was given"


def refusing():
    """What an engine does with a rule that names a column by anything but
    a plain identifier (SPEC-025)."""
    raise ConfigurationError("column name 's.t' must hold no dot: it names one thing")


def test_without_a_misnamed_factory_column_references_are_not_checked():
    report = problems_of(ExampleEngine)
    assert "column_names_are_refused" not in report.checks
    assert NOT_MISNAMED in report.notes
    assert not any(
        problem.startswith("column_names_are_refused") for problem in report.problems
    )


@pytest.mark.parametrize(
    ("outcome", "problem"),
    [
        (
            "builds it",
            "column_names_are_refused: built an engine for a rule that names a "
            "column by anything but a plain identifier: Spark reads such a string as "
            "an expression, and the engine would measure something else",
        ),
        (
            "another way",
            "column_names_are_refused: refused a rule that names a column by anything "
            "but a plain identifier with ValueError: no such column; refuse with a "
            "ConfigurationError, before anything runs",
        ),
    ],
)
def test_an_engine_that_does_not_refuse_a_misnamed_column_does_not_conform(
    outcome, problem
):
    def wrong():
        if outcome == "builds it":
            return ExampleEngine(CONFIG)
        raise ValueError("no such column")

    report = conformance_problems(
        factory(ExampleEngine), FRAME, InMemorySink, None, wrong
    )
    assert "column_names_are_refused" in report.checks
    # Among the problems, not alone: without Spark the persistence check
    # fails for a reason of its own.
    assert problem in report.problems, report.summary()
    with pytest.raises(AssertionError, match="column_names_are_refused"):
        assert_engine_conforms(factory(ExampleEngine), FRAME, InMemorySink, None, wrong)


def test_a_conforming_engine_passes_every_check():
    report = assert_engine_conforms(
        factory(ExampleEngine), FRAME, InMemorySink, reordered(ExampleEngine), refusing
    )
    assert isinstance(report, ConformanceReport) and report.conforms
    assert report.problems == () and report.notes == ()
    assert report.checks == (
        "manifest_is_valid",
        "uses_the_template",
        "column_names_are_refused",
        "outcomes_pass_the_outcome_gate",
        "check_names_are_unique",
        "criticality_is_canonical",
        "outcomes_are_deterministic",
        "attribution_survives_reordering",
        "persistence_honours_the_capability",
        "artifacts_carry_the_injected_run_key",
    )


def test_an_engine_with_no_severity_conforms_when_it_says_so():
    class BareEngine(ExampleEngine):
        CAPABILITIES = {"criticality": "absent"}

        def _execute(self, dataframe, prepared):
            return [build_result(name, ok) for name, ok, _ in prepared]

    assert problems_of(BareEngine).conforms


def test_an_engine_that_cannot_persist_conforms_when_it_refuses():
    """The template refuses for it, before any work, so this conforms."""

    class NonPersisting(ExampleEngine):
        CAPABILITIES = {"persistence": False}

    report = problems_of(NonPersisting)
    assert report.conforms
    assert "artifacts_carry_the_injected_run_key" not in report.checks


def test_persistence_is_not_checked_without_a_sink_factory():
    report = conformance_problems(factory(ExampleEngine), FRAME)
    assert report.conforms
    assert report.notes == (
        NOT_MISNAMED,
        "attribution under reordering was not checked: no reordered_factory was given",
        "persistence was not checked: no sink_factory was given",
    )
    assert "persistence_honours_the_capability" not in report.checks


def test_a_frame_with_one_verdict_only_is_noted_not_rejected():
    report = problems_of(ExampleEngine, outcomes=[("steady", True, "error")])
    assert report.conforms
    assert any("every outcome passed" in note for note in report.notes)


# --------------------------------------- attribution under reordering


class PositionalEngine(DQEngine):
    """An engine that pairs its checks with a vendor's results by position.

    The vendor returns one verdict per check in an order of its own, here
    a fixed one, as GX does when it groups expectations by column. With
    the checks in one order that happens to be the vendor's; in another
    it is not, and every check is reported with another's verdict.
    """

    CAPABILITIES = {"criticality": "native", "failed_rows": True}

    def __init__(
        self, config, dqts=None, outcomes=MIXED, vendor_order=("broken", "steady")
    ):
        super().__init__(config, dqts)
        self.outcomes = outcomes
        self.vendor_order = vendor_order

    def _prepare(self, dataframe):
        return self.outcomes

    def _execute(self, dataframe, prepared):
        verdicts = {name: (ok, criticality) for name, ok, criticality in prepared}
        results = [verdicts[name] for name in self.vendor_order]
        return [
            build_result(name, ok, {"criticality": criticality})
            for (name, _, _), (ok, criticality) in zip(prepared, results)
        ]


def test_an_engine_that_reads_position_as_identity_is_rejected():
    """The suite could not see this before: every other check still passes."""
    assert problems_of(PositionalEngine).conforms
    report = conformance_problems(
        factory(PositionalEngine), FRAME, InMemorySink, reordered(PositionalEngine)
    )
    (problem,) = report.problems
    assert problem.startswith("attribution_survives_reordering:")
    # In one order the checks reached the engine as the vendor returns them.
    assert "broken, steady" in problem
    assert "never by its position" in problem


def test_attribution_is_noted_not_checked_without_a_reordered_factory():
    report = problems_of(ExampleEngine)
    assert report.conforms
    assert "attribution_survives_reordering" not in report.checks
    assert report.notes == (
        NOT_MISNAMED,
        "attribution under reordering was not checked: no reordered_factory was given",
    )


def test_a_check_named_by_its_position_is_reported_rather_than_vouched_for():
    """The one case this check cannot judge.

    ``labels_for_checks`` names a second unnamed check of one kind by its
    position (``hasSize``, ``hasSize_1``). Under another order the same
    name is another check, and a comparison by name means nothing. The
    suite cannot tell that from a real swap -- the names are all still
    there -- so it reports what it saw. An author who gives those checks
    names of their own, which is what the label rule is for, gets a true
    answer.
    """

    class NumberedEngine(ExampleEngine):
        def _execute(self, dataframe, prepared):
            return [
                build_result(f"check_{index}", ok, {"criticality": criticality})
                for index, (_, ok, criticality) in enumerate(prepared)
            ]

    report = conformance_problems(
        factory(NumberedEngine), FRAME, InMemorySink, reordered(NumberedEngine)
    )
    assert "attribution_survives_reordering" in report.checks
    (problem,) = report.problems
    assert problem.startswith("attribution_survives_reordering:")
    assert "check_0, check_1" in problem


def test_attribution_is_noted_not_checked_when_the_names_change_with_the_order():
    class NamedAfterItsPosition(ExampleEngine):
        def _execute(self, dataframe, prepared):
            return [
                build_result(f"{name}_{index}", ok, {"criticality": criticality})
                for index, (name, ok, criticality) in enumerate(prepared)
            ]

    report = conformance_problems(
        factory(NamedAfterItsPosition),
        FRAME,
        InMemorySink,
        reordered(NamedAfterItsPosition),
    )
    assert report.problems == ()
    assert "attribution_survives_reordering" not in report.checks
    assert report.notes == (
        NOT_MISNAMED,
        "attribution under reordering was not checked: the check names changed "
        "with the order; a check named by its position cannot be followed from "
        "one run to the next",
    )


def test_a_reordered_engine_that_fails_to_run_is_rejected():
    class Exploding(ExampleEngine):
        def _execute(self, dataframe, prepared):
            raise RuntimeError("boom")

    report = conformance_problems(
        factory(ExampleEngine), FRAME, InMemorySink, factory(Exploding)
    )
    (problem,) = report.problems
    assert problem == (
        "attribution_survives_reordering: the engine built with its checks in "
        "another order failed: RuntimeError: boom"
    )


def test_attribution_is_noted_not_checked_when_the_engine_produces_nothing():
    report = conformance_problems(
        factory(ExampleEngine, outcomes=[]),
        FRAME,
        InMemorySink,
        reordered(ExampleEngine),
    )
    assert "attribution_survives_reordering" not in report.checks
    assert report.notes == (
        NOT_MISNAMED,
        "persistence was not checked: the engine produced no valid outcomes",
        "attribution under reordering was not checked: the engine produced no "
        "valid outcomes",
    )


def test_assert_engine_conforms_names_a_positional_engine():
    with pytest.raises(AssertionError, match="attribution_survives_reordering"):
        assert_engine_conforms(
            factory(PositionalEngine), FRAME, InMemorySink, reordered(PositionalEngine)
        )


# ----------------------------------------------- each violation is named


def test_a_wrong_result_shape_is_rejected():
    class BadShape(ExampleEngine):
        def _execute(self, dataframe, prepared):
            return [{"check": "no success key"}, {"check": "x", "success": "yes"}]

    report = problems_of(BadShape)
    assert [problem.split(":")[0] for problem in report.problems] == [
        "outcomes_pass_the_outcome_gate"
    ]


def test_no_outcomes_at_all_is_rejected():
    report = problems_of(ExampleEngine, outcomes=[])
    (problem,) = report.problems
    assert problem.startswith("outcomes_pass_the_outcome_gate:")
    assert "zero" in problem
    assert report.notes == (
        NOT_MISNAMED,
        "persistence was not checked: the engine produced no valid outcomes",
    )


def test_an_engine_that_raises_on_a_valid_frame_is_rejected():
    class Exploding(ExampleEngine):
        def _execute(self, dataframe, prepared):
            raise RuntimeError("boom")

    report = problems_of(Exploding)
    assert any(
        "apply() failed: RuntimeError: boom" in problem for problem in report.problems
    )


@pytest.mark.parametrize(
    "outcomes",
    [
        [("dup", True, "error"), ("dup", False, "error")],
        [("", True, "error"), ("named", False, "error")],
        [(None, True, "error")],
    ],
    ids=["repeated", "blank", "not-a-string"],
)
def test_ambiguous_check_names_are_rejected(outcomes):
    report = problems_of(ExampleEngine, outcomes=outcomes)
    assert any(
        problem.startswith("check_names_are_unique:") for problem in report.problems
    )


@pytest.mark.parametrize("level", ["native", "derived"])
def test_a_missing_criticality_is_rejected_when_the_engine_claims_one(level):
    class Silent(ExampleEngine):
        CAPABILITIES = {"criticality": level}

        def _execute(self, dataframe, prepared):
            return [build_result(name, ok) for name, ok, _ in prepared]

    report = problems_of(Silent)
    (problem,) = report.problems
    assert problem.startswith("criticality_is_canonical:")
    assert (
        "2 of 2 outcome(s) carry no details.criticality" in problem
        and repr(level) in problem
    )


def test_an_unnormalized_criticality_is_rejected():
    class WarningSpelledOut(ExampleEngine):
        def _execute(self, dataframe, prepared):
            return [
                build_result(name, ok, {"criticality": "Warning"})
                for name, ok, _ in prepared
            ]

    (problem,) = problems_of(WarningSpelledOut).problems
    assert problem.startswith("criticality_is_canonical:") and "'Warning'" in problem


def test_nondeterministic_outcomes_are_rejected():
    class CoinFlip(ExampleEngine):
        # The suite builds a fresh engine for its second run; alternate.
        flips = 0

        def __init__(self, config, dqts=None, outcomes=MIXED):
            super().__init__(config, dqts, outcomes)
            CoinFlip.flips += 1

        def _execute(self, dataframe, prepared):
            side = CoinFlip.flips % 2 == 1
            return [
                build_result(name, side, {"criticality": "error"})
                for name, _, _ in prepared
            ]

    report = problems_of(CoinFlip)
    assert any(
        problem.endswith("a second run over the same frame reported different outcomes")
        for problem in report.problems
    )


def test_an_engine_that_fails_on_its_second_run_is_rejected():
    """The suite builds a fresh engine for every run. One that works only
    once is neither deterministic nor something to persist from, so the same
    defect is reported under both checks and nothing is run against a key."""

    runs = []

    class WorksOnce(ExampleEngine):
        def _execute(self, dataframe, prepared):
            runs.append(1)
            if len(runs) > 1:
                raise RuntimeError("connection pool exhausted")
            return super()._execute(dataframe, prepared)

    report = problems_of(WorksOnce)
    assert report.problems == (
        "outcomes_are_deterministic: a second run failed: "
        "RuntimeError: connection pool exhausted",
        "persistence_honours_the_capability: declares persistence=True but failed "
        "with a repository: RuntimeError: connection pool exhausted",
    )
    assert "artifacts_carry_the_injected_run_key" not in report.checks


def test_overriding_apply_is_rejected():
    class Legacy(ExampleEngine):
        def apply(self, dataframe, repository=None):
            return self._execute(dataframe, None)

    report = problems_of(Legacy)
    assert any(
        problem.startswith("uses_the_template:") and "overrides apply()" in problem
        for problem in report.problems
    )


# ------------------------------------------------------ persistence honesty


class IgnoresRefusal(ExampleEngine):
    """Declares it cannot persist, yet accepts a repository."""

    CAPABILITIES = {"persistence": False}

    def apply(self, dataframe, repository=None):
        # Everything right except the refusal.
        return self._execute(dataframe, self._prepare(dataframe))


def test_an_engine_that_ignores_its_declared_persistence_capability_is_rejected():
    report = problems_of(IgnoresRefusal)
    assert any(
        problem
        == "persistence_honours_the_capability: declares persistence=False but accepted a repository without refusing"
        for problem in report.problems
    )
    assert "artifacts_carry_the_injected_run_key" not in report.checks


@pytest.mark.parametrize(
    "error",
    [RuntimeError("nope"), ConfigurationError("wrong reason")],
    ids=["wrong-type", "wrong-reason"],
)
def test_refusing_a_repository_for_another_reason_is_rejected(error):
    class RefusesBadly(IgnoresRefusal):
        def apply(self, dataframe, repository=None):
            if repository:
                raise error
            return self._execute(dataframe, self._prepare(dataframe))

    report = problems_of(RefusesBadly)
    assert any(
        problem.startswith(
            "persistence_honours_the_capability: refused a repository with"
        )
        for problem in report.problems
    )


class WritesNothing(ExampleEngine):
    def _persist_outcomes(self, repository, metrics, dataframe):
        return None


def test_an_engine_that_declares_persistence_but_writes_nothing_is_rejected():
    report = problems_of(WritesNothing)
    assert any(
        problem
        == "persistence_honours_the_capability: declares persistence=True but wrote nothing; a persisting engine writes both metrics and verifications"
        for problem in report.problems
    )
    # Nothing to locate once nothing was written: the run-key check is skipped.
    assert "artifacts_carry_the_injected_run_key" not in report.checks


def test_an_engine_that_writes_only_one_artifact_is_rejected():
    class HalfWriter(ExampleEngine):
        def _persist_outcomes(self, repository, metrics, dataframe):
            if not repository:
                return
            metrics_df, _ = self._artifact_frames(metrics, dataframe)
            repository.save(metrics_df, "metrics", self._dqts or 0)

    report = problems_of(HalfWriter)
    assert any(
        "but wrote metrics; a persisting engine writes both" in problem
        for problem in report.problems
    )


class CannotWrite(ExampleEngine):
    """Validates without a repository, and fails as soon as it is given one."""

    def _persist_outcomes(self, repository, metrics, dataframe):
        if repository:
            raise OSError("disk full")


def test_an_engine_that_fails_with_a_repository_is_rejected():
    report = problems_of(CannotWrite)
    assert report.problems == (
        "persistence_honours_the_capability: declares persistence=True but failed "
        "with a repository: OSError: disk full",
    )
    assert "artifacts_carry_the_injected_run_key" not in report.checks


class UsesItsOwnClock(ExampleEngine):
    def _persist_outcomes(self, repository, metrics, dataframe):
        if not repository:
            return
        metrics_df, verifications_df = self._artifact_frames(metrics, dataframe)
        key = int(time.time() * 1000)
        repository.save(metrics_df, "metrics", key)
        repository.save(verifications_df, "verifications", key)


def test_an_engine_that_ignores_the_injected_run_key_is_rejected():
    report = problems_of(UsesItsOwnClock)
    (problem,) = report.problems
    assert problem.startswith(
        "artifacts_carry_the_injected_run_key: artifacts landed under"
    )
    assert f"not under the run key the framework injected ({RUN_KEY})" in problem


class RefusesAStaleKey(ExampleEngine):
    """Writes under any key it is given, except one it deems too old to be real."""

    def _persist_outcomes(self, repository, metrics, dataframe):
        if self._dqts == RUN_KEY:
            raise ValueError(f"run key {RUN_KEY} is stale")
        return super()._persist_outcomes(repository, metrics, dataframe)


def test_an_engine_that_fails_on_the_injected_run_key_is_rejected():
    """The keyed run is the suite's own; its failure is a finding about the
    key, not a crash of the suite."""
    report = problems_of(RefusesAStaleKey)
    assert report.problems == (
        f"artifacts_carry_the_injected_run_key: the run failed: "
        f"ValueError: run key {RUN_KEY} is stale",
    )
    assert "persistence_honours_the_capability" in report.checks


# ------------------------------------------------------------ the assertion


def test_assert_engine_conforms_names_each_violation():
    with pytest.raises(AssertionError) as caught:
        assert_engine_conforms(factory(IgnoresRefusal), FRAME, InMemorySink)
    message = str(caught.value)
    assert "does not conform" in message
    assert "PROBLEM: persistence_honours_the_capability:" in message
    assert "PROBLEM: uses_the_template:" in message  # it also overrides apply
    assert "check(s) run:" in message


def test_summary_reads_well():
    report = ConformanceReport(("a_check",), ("a_check: went wrong",), ("a note",))
    assert report.summary().splitlines() == [
        "1 check(s) run: a_check",
        "  note: a note",
        "  PROBLEM: a_check: went wrong",
        "does NOT conform",
    ]
    assert ConformanceReport(("a_check",), ()).summary().splitlines()[-1] == "conforms"


# ------------------------------------------------------------- light core


def test_the_conformance_suite_imports_with_every_optional_dependency_blocked():
    """An engine author runs this in their own suite; it must not need extras.

    The default persistence path builds Spark frames, so an engine running
    where Spark is absent does what the contract asks of one that cannot
    persist: it says so, and refuses a repository.
    """
    script = """
from types import SimpleNamespace

from pyhocon import ConfigFactory

from dq.engine.checkspec import build_result
from dq.engine.dq_engine import DQEngine
from dq.sdk.conformance import assert_engine_conforms, conformance_problems
from dq.exceptions import ConfigurationError
from dq.sinks import InMemorySink


class EngineWithoutSpark(DQEngine):
    CAPABILITIES = {"criticality": "native", "persistence": False}

    def _execute(self, dataframe, prepared):
        return [
            build_result("steady", True, {"criticality": "error"}),
            build_result("broken", False, {"criticality": "warn"}),
        ]


frame = SimpleNamespace()
config = ConfigFactory.from_dict({"name": "engine-without-spark"})
factory = lambda dqts=None: EngineWithoutSpark(config, dqts=dqts)

report = assert_engine_conforms(factory, frame, InMemorySink)
assert len(report.checks) == 7, report.summary()
try:
    factory().apply(frame, repository=InMemorySink())
except ConfigurationError as error:
    assert "persistence=False" in str(error)
else:
    raise AssertionError("an engine that cannot persist must refuse a repository")
print("conforms without any engine or vendor library")
"""
    result = _run_with_blocked_imports(
        "boto3",
        "databricks",
        "pydeequ",
        "pyspark",
        "great_expectations",
        statement=script,
    )
    assert result.returncode == 0, result.stderr + result.stdout
