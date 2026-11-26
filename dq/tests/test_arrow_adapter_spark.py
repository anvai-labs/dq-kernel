# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Arrow adapter against the Spark adapter, on real Spark (SPEC-015, G2).

Two adapters that claim one semantics must agree on one set of data. The
corpus is run through both over the same Spark DataFrame -- the Arrow adapter
reads it through Spark's own Arrow export, which is also how it is used
inside the Spark facade -- and their metric values, decisions and refusals
are compared.

Beyond the corpus, tables are generated with signed zeros, NaN, nulls and
decimals in them and measured three ways: by Spark, by Arrow, and by a
reference written here in plain Python from the rows. Two adapters can agree
on a shared mistake; a third opinion that imports neither cannot. The rules
are generated too, with every threshold on a boundary the data makes -- a
ratio equal to the threshold, a count equal to it, a bound equal to it --
because that is where an inexact adapter is wrong.

The reference's semantics are the kernel's, stated plainly: a value is
present when it is not null and, for a float, not NaN; a distinct count
leaves nulls out, counts NaN once and a signed zero as zero; a null key is
a group; a bound is the shortest decimal of a float, or the value itself.
"""

import decimal
import math
import random
from decimal import Decimal
from fractions import Fraction

import pytest

from dq import arrow_adapter, spark_adapter
from dq.arrow_adapter import table_of
from dq.exceptions import ConfigurationError
from dq.plan import ExecutionPlan
from dq.sdk.fixtures import (
    DATASET_NAME,
    build_plan,
    check_metrics,
    decisions,
    kernel_capabilities,
    load_cases,
    metric_label,
    rule_spec,
)
from dq.tests.fixture_frames import spark_frame

pytestmark = pytest.mark.spark

pytest.importorskip("pyarrow", reason="the Arrow adapter needs the arrow extra")

EVERY_CASE = load_cases()
DECIDABLE = [case for case in EVERY_CASE if not case.expect_error]
FAIL_CLOSED = [case for case in EVERY_CASE if case.expect_error]


def spark_values(plan, bindings):
    return spark_adapter.compute_plan_values(
        plan, spark_adapter._validated_bindings(plan, bindings)
    )


def arrow_values(plan, bindings):
    return arrow_adapter.compute_plan_values(
        plan, arrow_adapter.validated_bindings(plan, bindings)
    )


# ---------------------------------------------------------------- corpus


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_both_adapters_compute_the_corpus_metrics_over_one_spark_dataframe(spark, case):
    bindings = {DATASET_NAME: spark_frame(spark, case)}
    plan = build_plan(case)
    assert spark_values(plan, bindings) == arrow_values(plan, bindings)
    assert (
        check_metrics(
            case,
            lambda plan, frames: arrow_adapter.compute_plan_values(
                plan, arrow_adapter.validated_bindings(plan, frames)
            ),
            bindings,
        )
        == []
    )


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_both_adapters_decide_the_corpus_the_same_way(spark, case):
    bindings = {DATASET_NAME: spark_frame(spark, case)}
    plan = build_plan(case)
    by_spark = spark_adapter.execute_plan(plan, bindings)
    by_arrow = arrow_adapter.execute_plan(plan, bindings)
    assert decisions(by_arrow) == decisions(by_spark) == dict(case.expected)
    # Everything recorded about a check is the same, but which adapter ran it.
    for left, right in zip(by_spark, by_arrow):
        details = left.to_legacy()["details"]
        details["adapter"] = None
        other = right.to_legacy()["details"]
        other["adapter"] = None
        assert details == other
    assert by_spark[0].to_legacy()["details"]["adapter"] == {
        "name": "spark",
        "version": "1",
    }
    assert by_arrow[0].to_legacy()["details"]["adapter"] == {
        "name": "arrow",
        "version": "1",
    }


@pytest.mark.parametrize("case", FAIL_CLOSED, ids=lambda case: case.id)
def test_both_adapters_refuse_in_the_same_words(spark, case):
    bindings = {DATASET_NAME: spark_frame(spark, case)}
    plan = build_plan(case)
    with pytest.raises(ConfigurationError) as by_spark:
        spark_adapter.execute_plan(plan, bindings)
    with pytest.raises(ConfigurationError) as by_arrow:
        arrow_adapter.execute_plan(plan, bindings)
    assert str(by_arrow.value) == str(by_spark.value)


def test_a_plan_naming_two_absent_columns_is_refused_on_the_same_one(spark):
    """Whichever column is reported first must not depend on the adapter."""
    frame = spark.createDataFrame([("a", 1)], "id string, n long")
    bindings = {DATASET_NAME: frame}
    specs = {
        "counts": (
            ("z_first", "zzz", "completeness"),
            ("a_second", "aaa", "completeness"),
        ),
        "ranges": (
            ("z_first", "zzz", "value_range"),
            ("a_second", "aaa", "value_range"),
        ),
    }
    for first, second in specs.values():
        rules = []
        for identifier, column, kind in (first, second):
            rules.append(
                rule_spec(
                    {
                        "rule_id": identifier,
                        "kind": kind,
                        "operator": "ge",
                        "threshold": "0",
                        "column": column,
                        **({"target": "column_min"} if kind == "value_range" else {}),
                    },
                    build_plan(DECIDABLE[0]).rules[0].dataset,
                )
            )
        plan = ExecutionPlan(tuple(rules))
        with pytest.raises(ConfigurationError) as by_spark:
            spark_adapter.execute_plan(plan, bindings)
        with pytest.raises(ConfigurationError) as by_arrow:
            arrow_adapter.execute_plan(plan, bindings)
        assert str(by_arrow.value) == str(by_spark.value)


# --------------------------------------------------- the export PySpark has


def test_a_classic_pyspark_frame_is_read_through_the_export_it_has(spark):
    frame = spark.createDataFrame(
        [
            ("a", 1.5, Decimal("1.25")),
            (None, float("nan"), None),
            ("c", -0.0, Decimal("3.50")),
        ],
        "id string, v double, amount decimal(10,2)",
    )
    offered = [
        name
        for name in ("toArrow", "_collect_as_arrow", "__arrow_c_stream__")
        if hasattr(frame, name)
    ]
    assert "_collect_as_arrow" in offered, f"this PySpark offers {offered}"
    table = table_of(frame)
    assert table.schema.names == ["id", "v", "amount"]
    values = table.column("v").to_pylist()
    assert values[1] != values[1]  # a NaN is still a NaN
    assert math.copysign(1, values[2]) == -1.0  # and a negative zero kept its sign


def test_an_empty_pyspark_frame_is_a_table_of_its_columns_and_no_row(spark):
    frame = spark.createDataFrame([], "id string, score double")
    assert frame._collect_as_arrow() == []
    table = table_of(frame)
    assert table.schema.names == ["id", "score"]
    assert table.num_rows == 0
    by_rule = {
        outcome.to_legacy()["check"]: outcome.to_legacy()
        for outcome in arrow_adapter.execute_plan(
            build_plan(next(c for c in DECIDABLE if c.id == "counts-empty-input")),
            {DATASET_NAME: frame},
        )
    }
    assert by_rule["size_at_least_one"]["details"]["observed"] == {
        "numerator": 0,
        "denominator": 1,
    }
    assert by_rule["score_complete"]["details"]["reason"] == "empty_dataset"


def test_repartitioning_a_frame_changes_nothing_the_arrow_adapter_computes(spark):
    case = next(
        c for c in DECIDABLE if c.id == "counts-mixed-null-nan-infinity-decimal"
    )
    plain = spark_frame(spark, case)
    shuffled = plain.repartition(5).orderBy("id")
    plan = build_plan(case)
    assert arrow_values(plan, {DATASET_NAME: shuffled}) == arrow_values(
        plan, {DATASET_NAME: plain}
    )


# ------------------------------------------------------ generated tables


SCHEMA = "s string, n long, v double, flag boolean, amount decimal(10,2)"
COLUMNS = ("s", "n", "v", "flag", "amount")
#: Few values, repeated often: duplicates, nulls and every awkward float.
CELLS = {
    "s": [None, "", "a", "b", "c"],
    "n": [None, -3, 0, 7, 2**40],
    "v": [None, 0.0, -0.0, 1.5, -2.5, 0.1, float("nan"), float("inf"), float("-inf")],
    "flag": [None, True, False],
    "amount": [
        None,
        Decimal("0.00"),
        Decimal("1.25"),
        Decimal("-3.50"),
        Decimal("99999999.99"),
    ],
}
NUMERIC = ("n", "v", "flag", "amount")
#: (grouping columns, measured columns) -- a float key included, for its
#: signed zeros and its NaN.
GROUPINGS = {
    ("s",): ("n", "v", "flag", "amount", "s"),
    ("flag", "n"): ("s", "amount"),
    ("v",): ("s", "n"),
}
SEEDS = list(range(8))


def generated(seed):
    """A table of up to 24 rows, drawn from the few values in ``CELLS``."""
    rng = random.Random(seed)
    return [
        tuple(rng.choice(CELLS[column]) for column in COLUMNS)
        for _ in range(rng.randint(0, 24))
    ]


def is_nan(value):
    return isinstance(value, float) and math.isnan(value)


def present(values):
    return sum(1 for value in values if value is not None and not is_nan(value))


def distinct(values):
    """A distinct count as the kernel defines it."""
    seen = set()
    for value in values:
        if value is None:
            continue
        if is_nan(value):
            seen.add("NaN")
        elif isinstance(value, float) and value == 0:
            seen.add(0.0)
        else:
            seen.add(value)
    return len(seen)


def group_key(value):
    """A grouping key as the kernel defines it."""
    if is_nan(value):
        return "NaN"
    if isinstance(value, float) and value == 0:
        return 0.0
    return value


def exact(value):
    """The fraction an adapter must report for a bound -- or why it cannot."""
    if isinstance(value, bool):
        return Fraction(int(value))
    if isinstance(value, int):
        return Fraction(value)
    if isinstance(value, Decimal):
        return Fraction(value)
    if math.isinf(value):
        return None
    return Fraction(Decimal(str(value)))


def reference(rows):
    """Every metric of the generated plans, computed from the rows alone."""
    by_column = {
        column: [row[index] for row in rows] for index, column in enumerate(COLUMNS)
    }
    values = {
        column: [
            value
            for value in by_column[column]
            if value is not None and not is_nan(value)
        ]
        for column in COLUMNS
    }
    found = {"row_count": len(rows)}
    for column in COLUMNS:
        found[f"present_count:{column}"] = present(by_column[column])
    for column in NUMERIC:
        found[f"column_count:{column}"] = len(values[column])
        for bound, pick in (("min", min), ("max", max)):
            found[f"column_{bound}:{column}"] = (
                exact(pick(values[column])) if values[column] else Fraction(0)
            )
    for keys, measured in GROUPINGS.items():
        groups = {}
        for row in rows:
            key = tuple(group_key(row[COLUMNS.index(name)]) for name in keys)
            groups.setdefault(key, []).append(row)
        label = "+".join(keys)
        found[f"group_count:{label}"] = len(groups)
        for column in measured:
            index = COLUMNS.index(column)
            counts = [
                distinct([row[index] for row in group]) for group in groups.values()
            ]
            found[f"group_min_distinct:{column}:{label}"] = min(counts, default=0)
            found[f"group_max_distinct:{column}:{label}"] = max(counts, default=0)
    return found


def threshold(value):
    """A rule threshold as a decimal string: exactly ``value`` when it can be."""
    with decimal.localcontext() as context:
        context.prec = 34
        return str(Decimal(value.numerator) / Decimal(value.denominator))


def documents(rows):
    """The generated rules of each semantic version, thresholds on boundaries."""
    found = reference(rows)
    counts, groups, ranges = [], [], []

    def rule(identifier, kind, operator, limit, **extra):
        return {
            "rule_id": identifier,
            "kind": kind,
            "operator": operator,
            "threshold": str(limit),
            **extra,
        }

    rows_total = found["row_count"]
    for operator in ("ge", "gt", "eq"):
        counts.append(rule(f"size_{operator}", "size", operator, rows_total))
    for column in COLUMNS:
        numerator = found[f"present_count:{column}"]
        at = threshold(Fraction(numerator, rows_total)) if rows_total else "0"
        for operator in ("ge", "gt", "eq"):
            counts.append(
                rule(
                    f"{column}_present_{operator}",
                    "completeness",
                    operator,
                    at,
                    column=column,
                )
            )
    for keys, measured in GROUPINGS.items():
        label = "+".join(keys)
        for column in measured:
            for target in ("group_min_distinct", "group_max_distinct"):
                at = found[f"{target}:{column}:{label}"]
                for operator in ("ge", "gt", "eq"):
                    groups.append(
                        rule(
                            f"{column}_by_{'_'.join(keys)}_{target}_{operator}",
                            "grouped_distinct",
                            operator,
                            at,
                            column=column,
                            group_by=list(keys),
                            target=target,
                        )
                    )
    for column in NUMERIC:
        for target in ("column_min", "column_max"):
            at = found[f"{target}:{column}"]
            if at is None:
                continue  # an infinite bound: the adapters must refuse it
            for operator in ("ge", "gt", "eq"):
                ranges.append(
                    rule(
                        f"{column}_{target}_{operator}",
                        "value_range",
                        operator,
                        threshold(at),
                        column=column,
                        target=target,
                    )
                )
    return {"counts/v1": counts, "groups/v1": groups, "ranges/v1": ranges}


def plan_of(rules):
    return ExecutionPlan(
        tuple(
            rule_spec(rule, build_plan(DECIDABLE[0]).rules[0].dataset) for rule in rules
        )
    )


def by_label(values):
    return {metric_label(metric): value for metric, value in values.items()}


TABLES = [()] + [tuple(generated(seed)) for seed in SEEDS]


@pytest.mark.parametrize(
    "rows", TABLES, ids=["empty"] + [f"seed-{seed}" for seed in SEEDS]
)
def test_spark_arrow_and_a_plain_reference_agree_on_a_generated_table(spark, rows):
    frame = spark.createDataFrame(list(rows), SCHEMA)
    bindings = {DATASET_NAME: frame}
    for semantics, rules in documents(rows).items():
        plan = plan_of(rules)
        want = {
            metric: reference(rows)[metric_label(metric)] for metric in plan.metrics
        }
        if None in want.values():
            # An infinite value is the bound: nothing exact can be said of it.
            with pytest.raises(ConfigurationError, match="infinite bound"):
                spark_values(plan, bindings)
            with pytest.raises(ConfigurationError, match="infinite bound"):
                arrow_values(plan, bindings)
            continue
        where = f"\n{semantics} over {rows}"
        assert spark_values(plan, bindings) == want, (
            "Spark differs from the reference" + where
        )
        assert arrow_values(plan, bindings) == want, (
            "Arrow differs from the reference" + where
        )
        expected = decisions(plan.evaluate(want, kernel_capabilities(plan)))
        assert decisions(spark_adapter.execute_plan(plan, bindings)) == expected, where
        assert decisions(arrow_adapter.execute_plan(plan, bindings)) == expected, where


def test_the_generated_tables_reach_every_awkward_value():
    """A generator that never draws a value proves nothing about it."""
    seen = {column: set() for column in COLUMNS}
    for seed in SEEDS:
        for row in generated(seed):
            for column, value in zip(COLUMNS, row):
                seen[column].add(value)
    assert seen["v"] >= {0.0, -0.0, float("inf"), float("-inf"), None}
    assert any(is_nan(value) for value in seen["v"]), "no NaN was drawn"
    assert seen["amount"] >= {Decimal("-3.50"), Decimal("99999999.99"), None}
    assert seen["flag"] >= {True, False, None}
    assert seen["n"] >= {2**40, None}


def expected_decisions(rows):
    """What the kernel itself decides from the reference's values."""
    verdicts = {}
    for semantics, rules in documents(rows).items():
        plan = plan_of(rules)
        want = {
            metric: reference(rows)[metric_label(metric)] for metric in plan.metrics
        }
        if None not in want.values():
            verdicts[semantics] = decisions(
                plan.evaluate(want, kernel_capabilities(plan))
            )
    return verdicts


@pytest.mark.parametrize("semantics", ["counts/v1", "groups/v1", "ranges/v1"])
def test_the_generated_rules_are_decided_both_ways(semantics):
    """Every threshold equals what the data makes, so ``ge`` passes where
    ``gt`` fails on the same rule. Were every rule to come out one way, the
    tables would be proving nothing."""
    verdicts = [
        verdict
        for seed in SEEDS
        for verdict in expected_decisions(generated(seed))[semantics].values()
    ]
    passed = sum(verdicts)
    failed = len(verdicts) - passed
    assert passed, f"none of {len(verdicts)} rules passes"
    assert failed >= len(verdicts) * 0.2, f"only {failed} of {len(verdicts)} rules fail"
