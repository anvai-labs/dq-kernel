# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the shared semantic corpus (SPEC-011).

Every compute adapter is measured against this one corpus, so the corpus is
held to its own standard: it is well formed, each case's decisions are the
consequence of its documented metrics through the kernel's own evaluation,
and the checkers report a mismatch when there is one.
"""

import copy
import json
from decimal import Decimal
from fractions import Fraction
from types import SimpleNamespace

import pytest

from dq.exceptions import ConfigurationError
from dq.plan import (
    MetricKind,
    SEMANTIC_GROUPS_VERSION,
    SEMANTIC_RANGES_VERSION,
    SEMANTICS_VERSION,
)
from dq.sdk import fixtures
from dq.sdk.fixtures import (
    DOUBLE_MARKERS,
    build_plan,
    check_case,
    check_metrics,
    decode_cell,
    decisions,
    documented_decisions,
    documented_metrics,
    kernel_capabilities,
    load_cases,
    load_document,
    metric_label,
    python_rows,
    validate_corpus,
)
from dq.tests.test_package_contract import _run_with_blocked_imports

DECIDABLE = [case for case in load_cases() if not case.expect_error]
EVERY_CASE = load_cases()
BY_ID = {case.id: case for case in EVERY_CASE}


# ------------------------------------------------------------- the corpus


def test_the_corpus_is_well_formed():
    assert validate_corpus(load_document()) == []


@pytest.mark.parametrize("case", EVERY_CASE, ids=lambda case: case.id)
def test_each_case_builds_a_plan_of_its_own_semantic_version(case):
    plan = build_plan(case)
    assert plan.semantic_version == case.semantics
    assert [rule.rule_id for rule in plan.rules] == sorted(
        case.expected or {r["rule_id"] for r in case.rules}
    )


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_each_case_s_decisions_follow_from_its_documented_metrics(case):
    """No adapter involved: the kernel turns the metrics into the decisions."""
    plan = build_plan(case)
    assert documented_decisions(plan, case) == dict(case.expected)


def test_every_semantic_version_is_covered_in_depth():
    by_version = {}
    for case in EVERY_CASE:
        by_version.setdefault(case.semantics, []).append(case)
    assert set(by_version) == {
        SEMANTICS_VERSION,
        SEMANTIC_GROUPS_VERSION,
        SEMANTIC_RANGES_VERSION,
    }
    for semantics, cases in by_version.items():
        assert len(cases) >= 6, f"{semantics} has only {len(cases)} cases"
    # The cases that matter most are each pinned by a dedicated case.
    assert (
        "counts-empty-input" in BY_ID and "groups-empty-input-passes-vacuously" in BY_ID
    )
    assert "ranges-no-values-passes-vacuously" in BY_ID
    assert (
        "counts-exact-two-thirds-boundary" in BY_ID
        and "ranges-decimal-bounds-are-exact" in BY_ID
    )
    mixed = BY_ID["counts-mixed-null-nan-infinity-decimal"]
    assert mixed.carries_nan("score") and not mixed.carries_nan("id")
    # The text "NaN" is a present value, not a NaN: only a double column can carry one.
    text_case = BY_ID["counts-strings-are-present-even-when-they-read-nan"]
    assert "NaN" in {row[0] for row in text_case.rows}
    assert not text_case.carries_nan("s")


def test_fail_closed_cases_document_the_error_they_expect():
    error_cases = [case for case in EVERY_CASE if case.expect_error]
    assert error_cases
    for case in error_cases:
        assert case.expect_error and case.expected == {}


def test_a_plan_s_fingerprint_is_stable_and_bound_to_the_data():
    case = BY_ID["ranges-integer-bounds"]
    assert build_plan(case).fingerprint == build_plan(case).fingerprint
    altered = copy.deepcopy(load_document())
    entry = next(item for item in altered["cases"] if item["id"] == case.id)
    entry["rows"][0][0] = 6
    rebuilt = fixtures._case_from(entry)
    assert (
        rebuilt.digest() != case.digest()
    ), "a fixture's identity must change with its rows"
    assert build_plan(rebuilt).fingerprint != build_plan(case).fingerprint


# ---------------------------------------------------------------- decoding


def test_cells_decode_to_the_values_an_adapter_should_see():
    assert decode_cell("1.25", "decimal(10,2)") == Decimal("1.25")
    assert decode_cell(None, "decimal(10,2)") is None
    assert decode_cell("NaN", "double") != decode_cell(
        "NaN", "double"
    )  # NaN is not itself
    assert decode_cell("Infinity", "double") == float("inf")
    assert decode_cell("-Infinity", "double") == -float("inf")
    assert decode_cell(1.5, "double") == 1.5
    assert decode_cell("NaN", "string") == "NaN"
    assert decode_cell(7, "long") == 7 and decode_cell(True, "boolean") is True
    assert set(DOUBLE_MARKERS) == {"NaN", "Infinity", "-Infinity"}


def test_rows_are_decoded_column_by_column():
    case = BY_ID["counts-mixed-null-nan-infinity-decimal"]
    first, second = python_rows(case)[:2]
    assert first == ("a", 1.5, Decimal("1.25"))
    assert second[0] == "" and second[2] is None
    assert second[1] != second[1]  # NaN


# --------------------------------------------------------- metric labels


def test_metric_labels_are_the_corpus_vocabulary():
    case = BY_ID["groups-two-column-key"]
    plan = build_plan(case)
    assert sorted(metric_label(metric) for metric in plan.metrics) == [
        "group_count:region+day",
        "group_max_distinct:id:region+day",
        "group_min_distinct:id:region+day",
    ]
    assert set(case.metrics) == {metric_label(metric) for metric in plan.metrics}


def test_documented_metrics_supply_exact_fractions_for_bounds():
    case = BY_ID["ranges-decimal-bounds-are-exact"]
    plan = build_plan(case)
    values = documented_metrics(plan, case)
    bounds = {metric.kind: value for metric, value in values.items()}
    assert (
        bounds[MetricKind.COLUMN_COUNT] == 2
        and type(bounds[MetricKind.COLUMN_COUNT]) is int
    )
    assert bounds[MetricKind.COLUMN_MAX] == Fraction(Decimal("0.10000000000000000001"))
    assert (
        bounds[MetricKind.COLUMN_MAX] > bounds[MetricKind.COLUMN_MIN] == Fraction(1, 10)
    )


def test_kernel_capabilities_declare_exactly_what_a_plan_needs():
    for case in DECIDABLE:
        plan = build_plan(case)
        capabilities = kernel_capabilities(plan)
        assert capabilities.semantic_version == case.semantics
        assert capabilities.metrics == {metric.kind for metric in plan.metrics}
        plan.validate_for(capabilities)


# ------------------------------------------------- the checkers can fail


def test_check_case_reports_each_wrong_decision_by_rule():
    case = BY_ID["ranges-integer-bounds"]
    plan = build_plan(case)
    wrong = documented_decisions(plan, case)
    wrong["max_exactly_nine"] = False
    wrong.pop("min_above_minus_three")

    def execute(given_plan, frames):
        return [
            SimpleNamespace(to_legacy=lambda r=r: {"check": r, "success": wrong[r]})
            for r in wrong
        ]

    problems = check_case(case, execute, {"ds": object()})
    assert sorted(problems) == [
        "ranges-integer-bounds.max_exactly_nine: expected True, got False",
        "ranges-integer-bounds.min_above_minus_three: expected False, got no outcome",
    ]


def test_check_case_accepts_a_matching_error_and_rejects_a_wrong_one():
    case = BY_ID["ranges-infinite-bound-fails-closed"]

    def refusing(plan, frames):
        raise ConfigurationError(
            "column 'v' in dataset 'ds' has an infinite bound; ranges/v1 bounds must be finite"
        )

    def other_error(plan, frames):
        raise ConfigurationError("something else went wrong")

    def deciding(plan, frames):
        return []

    frames = {"ds": object()}
    assert check_case(case, refusing, frames) == []
    assert check_case(case, other_error, frames) == [
        "ranges-infinite-bound-fails-closed: raised instead of deciding: something else went wrong"
    ]
    assert check_case(case, deciding, frames) == [
        "ranges-infinite-bound-fails-closed: expected an error matching 'infinite bound', got outcomes"
    ]


def test_check_metrics_names_the_metric_an_adapter_computes_differently():
    case = BY_ID["counts-mixed-null-nan-infinity-decimal"]
    plan = build_plan(case)
    right = documented_metrics(plan, case)

    def counting_nan_as_present(plan, frames):
        values = dict(right)
        score = next(
            metric for metric in values if metric_label(metric) == "present_count:score"
        )
        values[score] = 4
        return values

    assert check_metrics(case, lambda p, f: right, {"ds": object()}) == []
    assert check_metrics(case, counting_nan_as_present, {"ds": object()}) == [
        "counts-mixed-null-nan-infinity-decimal: present_count:score expected 3, got 4"
    ]
    assert check_metrics(case, lambda p, f: {}, {"ds": object()}) == [
        f"counts-mixed-null-nan-infinity-decimal: {metric_label(metric)} was not computed"
        for metric in plan.metrics
    ]


# ------------------------------------------------------ corpus integrity


def _broken(mutate):
    document = copy.deepcopy(load_document())
    mutate(document)
    return validate_corpus(document)


def test_a_sound_document_has_no_problems():
    assert _broken(lambda document: None) == []


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda d: d.__setitem__("version", 2), "corpus version must be 1"),
        (
            lambda d: d["cases"].append(copy.deepcopy(d["cases"][0])),
            "duplicate case id",
        ),
        (
            lambda d: d["cases"][0].__setitem__("semantics", "counts/v2"),
            "semantics must be one of",
        ),
        (
            lambda d: d["cases"][0]["columns"].__setitem__(1, ["score", "float"]),
            "unsupported type 'float'",
        ),
        (
            lambda d: d["cases"][0].__setitem__(
                "columns", [["id", "string"], ["id", "long"]]
            ),
            "unique names",
        ),
        (
            lambda d: d["cases"][0]["rows"].__setitem__(0, ["a", 1.5]),
            "row 0 has 2 cells, expected 3",
        ),
        (
            lambda d: d["cases"][0]["rows"][0].__setitem__(0, 5),
            "column 'id': expected a string",
        ),
        (
            lambda d: d["cases"][0]["rows"][0].__setitem__(1, "nan"),
            "expected a number or one of",
        ),
        (
            lambda d: d["cases"][0]["rows"][0].__setitem__(2, 1.25),
            "expected a decimal string",
        ),
        (lambda d: d["cases"][0].__setitem__("rules", []), "at least one rule"),
        (
            lambda d: d["cases"][0]["rules"][0].__setitem__("kind", "wizardry"),
            "unknown kind 'wizardry'",
        ),
        (
            lambda d: d["cases"][0]["rules"][0].__setitem__("operator", "approx"),
            "operator must be one of",
        ),
        (
            lambda d: d["cases"][0]["rules"][3].__setitem__("column", "missing"),
            "needs a column that exists",
        ),
        (
            lambda d: d["cases"][0]["rules"][3].__setitem__("kind", "value_range"),
            "does not belong to counts/v1",
        ),
        (
            lambda d: d["cases"][0]["expected"].pop("id_complete"),
            "expected must decide exactly every rule",
        ),
        (
            lambda d: d["cases"][0]["expected"].__setitem__("id_complete", "no"),
            "expected must decide exactly every rule",
        ),
        (
            lambda d: d["cases"][0]["metrics"].pop("row_count"),
            "metrics must be exactly",
        ),
        (
            lambda d: d["cases"][0].__setitem__("deequ_diverges", {"nope": "x"}),
            "deequ_diverges names 'nope'",
        ),
        (
            lambda d: d["cases"][0]["rules"][0].__setitem__("rule_id", "Not An Id"),
            "a rule needs a valid rule_id",
        ),
        (
            lambda d: d["cases"][0]["rules"][0].__setitem__("threshold", "abc"),
            "threshold must be a decimal string",
        ),
        (
            lambda d: d["cases"][0]["rules"][0].__setitem__("column", "id"),
            "kind 'size' takes no column",
        ),
        # A decimal the kernel refuses, although it parses: the plan cannot
        # be built, and the validator says so instead of letting an adapter
        # find out.
        (
            lambda d: d["cases"][0]["rules"][0].__setitem__("threshold", "NaN"),
            "its plan cannot be built: threshold requires a bounded finite Decimal",
        ),
        (
            lambda d: d["cases"][0]["rows"][0].__setitem__(1, True),
            "expected a number or one of",
        ),
        (
            lambda d: d["cases"][0]["rows"][0].__setitem__(2, "1.2.5"),
            "expected a decimal string",
        ),
        (
            lambda d: d["cases"][0]["rows"][0].__setitem__(1, [1.5]),
            "expected a number or one of",
        ),
    ],
    ids=[
        "version",
        "duplicate-id",
        "semantics",
        "column-type",
        "duplicate-column",
        "row-width",
        "cell-type",
        "cell-marker",
        "cell-decimal",
        "no-rules",
        "rule-kind",
        "operator",
        "missing-column",
        "kind-semantics",
        "missing-expectation",
        "non-bool-expectation",
        "missing-metric",
        "unknown-divergence",
        "rule-id",
        "threshold-text",
        "size-with-column",
        "threshold-nan",
        "cell-boolean-in-double",
        "cell-decimal-text",
        "cell-list",
    ],
)
def test_a_malformed_document_is_rejected(mutate, message):
    assert message in " ".join(_broken(mutate))


FIRST = "counts-mixed-null-nan-infinity-decimal"
INTEGERS = "ranges-integer-bounds"


def _cell_as(case_id, column, cell):
    """Return a mutation storing ``cell`` in row 0 of a case's ``column``."""

    def mutate(document):
        entry = next(entry for entry in document["cases"] if entry["id"] == case_id)
        position = [name for name, _ in entry["columns"]].index(column)
        entry["rows"][0][position] = cell

    return mutate


@pytest.mark.parametrize(
    ("case_id", "column", "cell", "message"),
    [
        (FIRST, "amount", "NaN", "expected a decimal string"),
        (FIRST, "amount", "sNaN", "expected a decimal string"),
        (FIRST, "amount", "Infinity", "expected a decimal string"),
        (FIRST, "amount", "1.234", "expected a decimal that fits decimal(10,2)"),
        (FIRST, "amount", "0.001", "expected a decimal that fits decimal(10,2)"),
        (FIRST, "amount", "123456789.01", "expected a decimal that fits decimal(10,2)"),
        (INTEGERS, "amount", 2**63, "expected a 64-bit integer"),
        (INTEGERS, "amount", -(2**63) - 1, "expected a 64-bit integer"),
        (INTEGERS, "amount", 1.5, "expected an integer"),
        (INTEGERS, "amount", True, "expected an integer"),
    ],
    ids=[
        "decimal-nan",
        "decimal-signalling-nan",
        "decimal-infinite",
        "decimal-one-digit-too-precise",
        "decimal-beyond-scale",
        "decimal-too-many-integer-digits",
        "long-above-max",
        "long-below-min",
        "long-fraction",
        "long-boolean",
    ],
)
def test_a_cell_no_engine_can_store_as_written_is_rejected(
    case_id, column, cell, message
):
    """Spark stores a decimal ``"NaN"`` as null and ``"1.234"`` as
    ``decimal(10,2)`` as 1.23, without a word. A case carrying such a cell
    would run on data other than the one it documents."""
    assert message in " ".join(_broken(_cell_as(case_id, column, cell)))


@pytest.mark.parametrize(
    "cell", ["0.00", "99999999.99", "-99999999.99", "1.2", "5", "1e2", " 1.25 "]
)
def test_a_decimal_cell_that_fits_its_column_exactly_is_sound(cell):
    assert _broken(_cell_as(FIRST, "amount", cell)) == []


@pytest.mark.parametrize("value", [2**63 - 1, -(2**63), 0])
def test_every_64_bit_integer_fits_a_long_column(value):
    assert _broken(_cell_as(INTEGERS, "amount", value)) == []


def test_cases_can_be_loaded_for_one_semantic_version():
    every = load_cases()
    loaded = {version: load_cases(version) for version in fixtures.SEMANTICS}
    assert {case.semantics for case in loaded["groups/v1"]} == {"groups/v1"}
    assert sum(map(len, loaded.values())) == len(every)
    assert sorted(case.id for part in loaded.values() for case in part) == sorted(
        case.id for case in every
    )


def test_a_grouped_rule_needs_its_grouping_and_target():
    document = copy.deepcopy(load_document())
    case = next(
        entry for entry in document["cases"] if entry["id"] == "groups-single-group"
    )
    del case["rules"][0]["group_by"]
    del case["rules"][1]["target"]
    case["rules"][2].__setitem__("group_by", ["region", "region"])
    problems = " ".join(validate_corpus(document))
    assert "needs distinct grouping columns that exist" in problems
    assert (
        "target must be one of ('group_min_distinct', 'group_max_distinct')" in problems
    )


def test_an_error_case_may_not_carry_decisions():
    document = copy.deepcopy(load_document())
    case = next(
        entry
        for entry in document["cases"]
        if entry["id"] == "ranges-infinite-bound-fails-closed"
    )
    case["expected"] = {"max_at_most_two": False}
    assert "a case that documents an error carries no decisions" in " ".join(
        validate_corpus(document)
    )


def test_loading_a_malformed_corpus_raises(tmp_path):
    broken = tmp_path / "broken.json"
    broken.write_text(
        json.dumps({"version": 1, "cases": [{"id": "x"}]}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="the semantic corpus is malformed"):
        fixtures.load_cases(path=broken)


# ------------------------------------------------------------- light core


def test_the_fixture_module_imports_with_every_optional_dependency_blocked():
    result = _run_with_blocked_imports(
        "boto3",
        "databricks",
        "pydeequ",
        "pyspark",
        "great_expectations",
        statement=(
            "import logging; logging.disable(logging.CRITICAL); "
            "from dq.sdk import fixtures; "
            "cases = fixtures.load_cases(); "
            "plan = fixtures.build_plan(cases[0]); "
            "assert fixtures.documented_decisions(plan, cases[0]) == dict(cases[0].expected); "
            "print('corpus ok without any engine')"
        ),
    )
    assert result.returncode == 0, result.stderr


def test_an_uppercase_or_punctuated_case_id_is_rejected():
    document = copy.deepcopy(load_document())
    document["cases"][0]["id"] = "Counts_Mixed!"
    problems = " ".join(validate_corpus(document))
    assert "a case id is lowercase words joined by hyphens" in problems
