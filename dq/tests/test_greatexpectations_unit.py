# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""JVM-free, GE-free contract tests for the greatexpectations engine.

The gx module is faked in sys.modules (test_dqdl-style vendor mock) so
the engine contract -- strict-JSON sanitizer, bool coercion, fail-closed
type resolution, single-key persistence, batch-definition caching, and
attribution of each result to its own expectation -- runs in the fast
unit lane without the optional extra installed.

The fake keeps to what GX does (read from its source and observed on a
Spark frame, GX 1.24.1 and 1.25.0): an expectation's configuration is
handed back with its result, a ``batch_id`` is added to the result's
arguments, and results come back grouped by column, not in suite order.
"""

import math
import sys
import types
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from pyhocon import ConfigFactory

from dq.engine.greatexpectations.greatexpectations_engine import (
    GreatexpectationsEngine,
    _to_strict_json,
)
from dq.exceptions import ConfigurationError
from dq.gate import evaluate_gate

# ---------------------------------------------------------------- sanitizer


def test_sanitizer_normalizes_adversarial_payloads():
    class NumpyBool:
        def item(self):
            return True

    class NumpyFloat:
        def item(self):
            return float("nan")

    payload = {
        42: "int-key",
        "tuple": (1, 2),
        "nan": float("nan"),
        "inf": float("inf"),
        "np_bool": NumpyBool(),
        "np_nan": NumpyFloat(),
        "bytes": b"raw",
        "date": date(2026, 10, 22),
        "exotic": object(),
    }
    out = _to_strict_json(payload)
    import json

    assert out == json.loads(json.dumps(out, allow_nan=False))
    assert list(out) == [
        "42",
        "tuple",
        "nan",
        "inf",
        "np_bool",
        "np_nan",
        "bytes",
        "date",
        "exotic",
    ]
    assert out["tuple"] == [1, 2]
    assert out["nan"] is None and out["inf"] is None
    assert out["np_bool"] is True and out["np_nan"] is None
    assert out["bytes"] == "raw"
    assert out["date"] == "2026-10-22"
    assert isinstance(out["exotic"], str)


def test_sanitizer_falls_through_when_item_unwrap_raises():
    class BrokenScalar:
        def item(self):
            raise RuntimeError("not a numpy scalar")

    out = _to_strict_json({"broken": BrokenScalar()})
    assert isinstance(out["broken"], str)


def test_engine_import_error_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "great_expectations", None)
    engine = GreatexpectationsEngine(
        ConfigFactory.from_dict(
            {"name": "gx", "engine": "greatexpectations", "expectations": []}
        )
    )
    with pytest.raises(ImportError, match="greatexpectations engine requires"):
        engine.apply(_FakeFrame())


def test_sanitizer_bounds_recursion_depth():
    deep = current = {}
    for _ in range(30):
        current["child"] = {}
        current = current["child"]
    current["leaf"] = "value"
    out = _to_strict_json(deep)
    assert out == _to_strict_json(out)  # stable, no recursion error


def test_sanitized_payloads_always_clear_the_outcome_gate():
    # The engine-level contract: for ANY nesting depth, a sanitized
    # payload must pass CheckOutcome.from_legacy without raising.
    from dq.outcomes import CheckOutcome

    def nest(n):
        value = {"leaf": "value"}
        for _ in range(n):
            value = {"child": value}
        return value

    for depth in (0, 5, 14, 15, 20, 60):
        payload = {
            "check": "t",
            "success": True,
            "details": _to_strict_json(nest(depth)),
        }
        CheckOutcome.from_legacy(payload)  # must not raise


def test_sanitizer_truncates_oversized_strings_and_lists():
    huge_string = "x" * 200_000
    huge_list = list(range(5_000))
    out = _to_strict_json({"s": huge_string, "l": huge_list})
    assert len(out["s"]) < 100_000 and out["s"].endswith("...[truncated]")
    assert len(out["l"]) <= 2_001 and out["l"][-1] == "...[truncated]"


def test_sanitizer_survives_cyclic_item_unwrapping():
    class CyclicScalar:
        def item(self):
            return CyclicScalar()

    out = _to_strict_json({"c": CyclicScalar()})
    assert isinstance(out["c"], str)  # depth bound forces the str() fallback


# ------------------------------------------------------------- vendor fake

#: GX's default severity; a warning expectation carries ``"warning"``.
CRITICAL = "critical"

#: GX adds a ``batch_id`` of its own to every result's configuration.
BATCH_ID = "dq_kernel_gx-dq_kernel_frames"

#: What the rule pack may say of an expectation that GX files as a field of
#: its configuration rather than as an argument of the expectation.
_FIELDS = ("severity", "description")


class _Configuration:
    """What GX keeps for an expectation, and hands back with its result."""

    def __init__(self, type, kwargs, severity=CRITICAL, description=None):
        self.type = type
        self.kwargs = kwargs
        self.severity = severity
        self.description = description


class FakeExpectation:
    """An ``gx.expectations.Expect*`` class as the engine sees it.

    ``severity`` and ``description`` are fields of the configuration, not
    arguments of the expectation, which is how GX files them.

    GX has one class per expectation type, each knowing its own
    snake-case name; :func:`_expectation` builds one for a type.
    """

    gx_type = None

    def __init__(self, **kwargs):
        fields = {name: kwargs.pop(name, None) for name in _FIELDS}
        fields["severity"] = fields["severity"] or CRITICAL
        self.configuration = _Configuration(self.gx_type, kwargs, **fields)
        self.id = None

    @property
    def kwargs(self):
        return self.configuration.kwargs


def _expectation(gx_type):
    return type(gx_type, (FakeExpectation,), {"gx_type": gx_type})


class _Suite:
    def __init__(self, name):
        self.name = name
        self.expectations = []
        self.added = []

    def add_expectation(self, expectation):
        """Keep the expectation and return it, as GX does.

        Unless GX already holds one that is the same to it -- same type,
        arguments, severity and description; ``meta`` and ``notes`` do not
        count. It keeps one copy and drops the rest without an error.
        """
        self.added.append(expectation)
        for kept in self.expectations:
            mine, theirs = expectation.configuration, kept.configuration
            if (mine.type, mine.kwargs, mine.severity, mine.description) == (
                theirs.type,
                theirs.kwargs,
                theirs.severity,
                theirs.description,
            ):
                return expectation
        self.expectations.append(expectation)
        return expectation


def _make_fake_gx(validation=None):
    """Build a great_expectations module double; ``fake.validation`` is what
    ``batch.validate`` returns."""
    fake = types.ModuleType("great_expectations")
    fake.expectations = SimpleNamespace(
        **{
            "".join(part.title() for part in gx_type.split("_")): _expectation(gx_type)
            for gx_type in (
                "expect_column_values_to_not_be_null",
                "expect_column_values_to_be_in_set",
                "expect_column_values_to_be_between",
                "expect_column_values_to_be_unique",
                "expect_table_row_count_to_be_between",
            )
        }
    )
    fake.validation = validation
    batch = SimpleNamespace(validate=lambda suite: fake.validation)
    batch_def = SimpleNamespace(get_batch=lambda batch_parameters: batch)
    fake.get_context = lambda: SimpleNamespace(
        data_sources=SimpleNamespace(
            add_spark=lambda name: SimpleNamespace(
                add_dataframe_asset=lambda name: SimpleNamespace(
                    add_batch_definition_whole_dataframe=lambda name: batch_def
                )
            )
        )
    )
    fake._batch_def = batch_def
    fake._suite = _Suite(None)
    fake.ExpectationSuite = lambda name=None: fake._suite
    return fake


def _engine_with(fake_gx, monkeypatch, expectations, **config):
    monkeypatch.setitem(sys.modules, "great_expectations", fake_gx)
    config.setdefault("name", "gx-unit")
    config["engine"] = "greatexpectations"
    config["expectations"] = expectations
    return GreatexpectationsEngine(ConfigFactory.from_dict(config))


def _built_suite(engine):
    """Build the engine's suite through the fake and return it."""
    engine._prepare(_FakeFrame())
    return engine._gx_suite


def _gx_returns(suite, verdicts, order=None):
    """What ``batch.validate`` returns for a built suite.

    GX hands every expectation its configuration back with its result,
    adds a ``batch_id`` of its own, and groups the results by column
    rather than returning them in suite order. ``verdicts`` maps a suite
    position to its ``success``, or to ``(success, result)``; ``order``
    lists the suite positions as GX sends them, suite order by default.
    """
    order = range(len(suite.expectations)) if order is None else order
    results = []
    for position in order:
        configuration = suite.expectations[position].configuration
        verdict = verdicts[position]
        success, result = verdict if isinstance(verdict, tuple) else (verdict, {})
        results.append(
            SimpleNamespace(
                expectation_config=_Configuration(
                    configuration.type,
                    {**configuration.kwargs, "batch_id": BATCH_ID},
                    configuration.severity,
                    configuration.description,
                ),
                success=success,
                result=result,
            )
        )
    return SimpleNamespace(results=results)


class _FakeSpark:
    def createDataFrame(self, rows, schema):
        return SimpleNamespace(rows=rows, schema=schema)


class _FakeFrame:
    sparkSession = _FakeSpark()


# ----------------------------------------------------------------- engine


def test_engine_coerces_results_through_the_sanitizer(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(
        fake,
        monkeypatch,
        [
            {"type": "expect_column_values_to_not_be_null", "column": "id"},
            {"type": "expect_column_values_to_be_in_set", "column": "id"},
        ],
    )
    fake.validation = _gx_returns(
        _built_suite(engine),
        {
            0: (True, {"unexpected_count": 0}),
            1: ("yes", {"partial_unexpected_list": (1, 2), "ratio": float("nan")}),
        },
    )
    metrics = engine.apply(_FakeFrame())
    assert metrics[0]["success"] is True
    assert metrics[1]["success"] is True  # coerced to bool("yes")
    assert metrics[1]["details"] == {
        "criticality": "error",
        "partial_unexpected_list": [1, 2],
        "ratio": None,
    }


def test_engine_fails_closed_on_unknown_type_without_gx_classes(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(
        fake, monkeypatch, [{"type": "expect_never_exists", "column": "id"}]
    )
    with pytest.raises(ValueError, match="Unsupported expectation type"):
        engine.apply(_FakeFrame())


def test_engine_fails_closed_on_missing_type(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(fake, monkeypatch, [{"column": "id"}])
    with pytest.raises(ValueError, match="requires a 'type'"):
        engine.apply(_FakeFrame())


def test_engine_passes_column_only_when_present_and_merges_kwargs(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(
        fake,
        monkeypatch,
        [
            {"type": "expect_table_row_count_to_be_between", "min_value": 1},
            {
                "type": "expect_column_values_to_not_be_null",
                "column": "id",
                "kwargs": {"mostly": 0.9},
            },
        ],
    )
    _built_suite(engine)
    added = fake._suite.added
    assert added[0].kwargs == {"min_value": 1}
    assert added[1].kwargs == {"column": "id", "mostly": 0.9}


def test_engine_persists_both_artifacts_under_one_key(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(
        fake,
        monkeypatch,
        [{"type": "expect_column_values_to_not_be_null", "column": "id"}],
    )
    fake.validation = _gx_returns(_built_suite(engine), {0: True})
    saved = []

    class FakeSink:
        def save(self, df, metric_type, key):
            saved.append((metric_type, key, df))

    engine.apply(_FakeFrame(), repository=FakeSink())
    assert [kind for kind, _, _ in saved] == ["metrics", "verifications"]
    assert len({key for _, key, _ in saved}) == 1
    # The artifact carries the check's label: its type and column.
    assert saved[0][2].rows == [("expect_column_values_to_not_be_null_id", True)]


def test_engine_caches_batch_definition_across_applies(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(
        fake,
        monkeypatch,
        [{"type": "expect_column_values_to_not_be_null", "column": "id"}],
    )
    fake.validation = _gx_returns(
        _built_suite(engine), {0: (True, {"unexpected_count": 0})}
    )
    engine.apply(_FakeFrame())
    first = engine._gx_batch_definition
    first_suite = engine._gx_suite
    engine.apply(_FakeFrame())
    assert engine._gx_batch_definition is first
    assert engine._gx_suite is first_suite


# ------------------------------------------------- attribution (SPEC-011 amendment)


#: A table-level expectation defined between two on one column. GX groups
#: expectations by column, so it sends these back in the order 0, 2, 1.
INTERLEAVED = [
    {"type": "expect_column_values_to_not_be_null", "column": "id"},
    {"type": "expect_table_row_count_to_be_between", "min_value": 1, "max_value": 2},
    {"type": "expect_column_values_to_be_unique", "column": "id"},
]


def test_results_are_attributed_by_what_they_carry_not_by_position(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(fake, monkeypatch, INTERLEAVED)
    suite = _built_suite(engine)
    # GX's order: the two on ``id``, then the row count defined second.
    fake.validation = _gx_returns(
        suite,
        {
            0: (True, {"unexpected_count": 0}),
            1: (False, {"observed_value": 3}),
            2: (True, {"unexpected_count": 0}),
        },
        order=[0, 2, 1],
    )
    metrics = engine.apply(_FakeFrame())
    # One outcome per expectation, in configuration order, each with the
    # verdict and the observations of its own.
    assert [
        (m["check"], m["success"], m["details"].get("observed_value")) for m in metrics
    ] == [
        ("expect_column_values_to_not_be_null_id", True, None),
        ("expect_table_row_count_to_be_between", False, 3),
        ("expect_column_values_to_be_unique_id", True, None),
    ]


def test_a_failed_check_cannot_borrow_a_passing_checks_verdict(monkeypatch):
    """The gate must block on a failed error-level check.

    Read by position, the row-count result lands on the uniqueness check
    and the other way round: the check that failed is reported as passed
    and the one that passed as failed at ``warn``, and a gate that ignores
    warnings opens.
    """
    rules = [
        dict(entry, criticality=criticality)
        for entry, criticality in zip(INTERLEAVED, ["error", "error", "warn"])
    ]
    fake = _make_fake_gx()
    engine = _engine_with(fake, monkeypatch, rules)
    fake.validation = _gx_returns(
        _built_suite(engine), {0: True, 1: False, 2: True}, order=[0, 2, 1]
    )
    metrics = engine.apply(_FakeFrame())
    decision = evaluate_gate(metrics, "error")
    assert decision.passed is False
    assert decision.blocking == ("expect_table_row_count_to_be_between",)


def test_an_explicit_name_travels_with_its_own_result(monkeypatch):
    rules = [
        dict(entry, name=name)
        for entry, name in zip(INTERLEAVED, ["ids_present", "row_count", "ids_unique"])
    ]
    fake = _make_fake_gx()
    engine = _engine_with(fake, monkeypatch, rules)
    fake.validation = _gx_returns(
        _built_suite(engine), {0: True, 1: False, 2: True}, order=[2, 1, 0]
    )
    metrics = engine.apply(_FakeFrame())
    assert {m["check"]: m["success"] for m in metrics} == {
        "ids_present": True,
        "row_count": False,
        "ids_unique": True,
    }


def test_a_result_that_matches_no_expectation_refuses_the_run(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(
        fake,
        monkeypatch,
        [{"type": "expect_column_values_to_not_be_null", "column": "id"}],
    )
    fake.validation = _gx_returns(_built_suite(engine), {0: True}, order=[0])
    # A result for an expectation nobody configured.
    fake.validation.results.append(
        SimpleNamespace(
            expectation_config=_Configuration(
                "expect_column_values_to_be_in_set", {"batch_id": BATCH_ID}
            ),
            success=True,
            result={},
        )
    )
    with pytest.raises(
        ConfigurationError, match=r"GX result 1 .*matches none of the 1"
    ):
        engine.apply(_FakeFrame())


def test_an_expectation_without_a_result_refuses_the_run(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(fake, monkeypatch, INTERLEAVED)
    suite = _built_suite(engine)
    # GX sent two results for three expectations.
    fake.validation = _gx_returns(suite, {0: True, 1: False}, order=[1, 0])
    with pytest.raises(
        ConfigurationError,
        match=r"2 result\(s\) for 3 .*expect_column_values_to_be_unique_id produced none",
    ):
        engine.apply(_FakeFrame())


def test_two_results_for_one_expectation_refuse_the_run(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(fake, monkeypatch, INTERLEAVED)
    fake.validation = _gx_returns(
        _built_suite(engine), {0: True, 1: False, 2: True}, order=[0, 2, 1]
    )
    # The uniqueness result twice: once as itself, once in place of another.
    fake.validation.results[2] = fake.validation.results[1]
    with pytest.raises(ConfigurationError, match=r"GX result 2 .*matches none"):
        engine.apply(_FakeFrame())


def test_a_result_two_expectations_match_equally_refuses_the_run(monkeypatch):
    fake = _make_fake_gx()
    engine = _engine_with(
        fake,
        monkeypatch,
        [
            {
                "name": "floored",
                "type": "expect_column_values_to_be_between",
                "column": "amount",
                "min_value": 1,
            },
            {
                "name": "roofed",
                "type": "expect_column_values_to_be_between",
                "column": "amount",
                "max_value": 5,
            },
        ],
    )
    # No configured expectation has both bounds; a result that does belongs
    # to neither more than the other.
    fake.validation = _gx_returns(
        _built_suite(engine),
        {0: (True, {}), 1: (True, {})},
    )
    for result in fake.validation.results:
        result.expectation_config.kwargs.update({"min_value": 1, "max_value": 5})
    with pytest.raises(
        ConfigurationError, match="GX result 0 matches checks 0, 1 equally"
    ):
        engine.apply(_FakeFrame())


def test_one_expectations_arguments_held_by_anothers_are_told_apart(monkeypatch):
    """A result belongs to the expectation it describes in full."""
    fake = _make_fake_gx()
    engine = _engine_with(
        fake,
        monkeypatch,
        [
            {
                "name": "floored",
                "type": "expect_column_values_to_be_between",
                "column": "amount",
                "min_value": 0,
            },
            {
                "name": "bounded",
                "type": "expect_column_values_to_be_between",
                "column": "amount",
                "min_value": 0,
                "max_value": 100,
            },
        ],
    )
    fake.validation = _gx_returns(
        _built_suite(engine),
        {0: (False, {"observed": "floored"}), 1: (True, {"observed": "bounded"})},
    )
    metrics = engine.apply(_FakeFrame())
    assert {m["check"]: m["details"]["observed"] for m in metrics} == {
        "floored": "floored",
        "bounded": "bounded",
    }
    assert {m["check"]: m["success"] for m in metrics} == {
        "floored": False,
        "bounded": True,
    }


def test_the_same_expectation_under_two_names_is_refused(monkeypatch):
    """GX would run one of them and report it once."""
    fake = _make_fake_gx()
    engine = _engine_with(
        fake,
        monkeypatch,
        [
            {
                "name": "a",
                "type": "expect_column_values_to_not_be_null",
                "column": "id",
            },
            {
                "name": "b",
                "type": "expect_column_values_to_not_be_null",
                "column": "id",
            },
        ],
    )
    with pytest.raises(
        ValueError, match="Checks 0 and 1 are the same expectation to GX"
    ):
        engine.apply(_FakeFrame())
    assert len(fake._suite.expectations) == 1  # GX kept one copy


def test_expectations_differing_only_in_prose_are_refused(monkeypatch):
    """GX runs both and reports two results nothing could tell apart."""
    fake = _make_fake_gx()
    engine = _engine_with(
        fake,
        monkeypatch,
        [
            {
                "name": "a",
                "description": "first",
                "type": "expect_column_values_to_not_be_null",
                "column": "id",
            },
            {
                "name": "b",
                "description": "second",
                "type": "expect_column_values_to_not_be_null",
                "column": "id",
            },
        ],
    )
    with pytest.raises(
        ValueError, match="Checks 0 and 1 are the same expectation to GX"
    ):
        engine.apply(_FakeFrame())
    assert len(fake._suite.expectations) == 2  # GX kept both


def test_expectations_differing_only_in_severity_are_told_apart(monkeypatch):
    """Severity is part of what a result carries."""
    fake = _make_fake_gx()
    engine = _engine_with(
        fake,
        monkeypatch,
        [
            {
                "name": "strict",
                "type": "expect_column_values_to_not_be_null",
                "column": "id",
            },
            {
                "name": "advisory",
                "type": "expect_column_values_to_not_be_null",
                "column": "id",
                "severity": "warning",
            },
        ],
    )
    suite = _built_suite(engine)
    assert [kept.configuration.severity for kept in suite.expectations] == [
        CRITICAL,
        "warning",
    ]
    fake.validation = _gx_returns(suite, {0: False, 1: True}, order=[1, 0])
    metrics = engine.apply(_FakeFrame())
    assert {m["check"]: m["success"] for m in metrics} == {
        "strict": False,
        "advisory": True,
    }
