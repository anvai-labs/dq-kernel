# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared plan builders and reducers for portable-plan tests.

Pure-python factories mirroring the schemavalidation_support pattern:
ExecutionPlan builders parameterized by rule id and dataset, the
column-decision reducer used by the differential suites, and the
grouped fixture rows shared across engine families. Spark-dependent
factories stay in their own ``*_support`` modules.
"""

from decimal import Decimal

from dq.plan import (
    ColumnRef,
    Comparison,
    ExecutionPlan,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
)

# Distinct counts per group: east has 3 distinct ids across 3 rows,
# west repeats (a, a) for 2 distinct across 3 rows, north repeats
# (a, a, a) for 1 distinct across 3 rows.
GROUPS_ROWS = [
    ("east", "a", 1.0),
    ("east", "b", 2.0),
    ("east", "c", 3.0),
    ("west", "a", 9.0),
    ("west", "a", 9.0),
    ("west", "b", 5.0),
    ("north", "a", 7.0),
    ("north", "a", 7.0),
    ("north", "a", 8.0),
]


def groups_plan(rule_id, dataset):
    """Build a one-rule grouped-distinct plan on id grouped by region.

    Args:
        rule_id: Rule identity in the returned plan.
        dataset: DatasetRef the plan binds to.

    Returns:
        ExecutionPlan with a GROUPED_DISTINCT rule over the minimum
        per-group distinct count.
    """
    return ExecutionPlan(
        (
            RuleSpec(
                rule_id,
                dataset,
                RuleKind.GROUPED_DISTINCT,
                Predicate(Comparison.GE, Decimal(1)),
                ColumnRef("id"),
                group_by=(ColumnRef("region"),),
                target=MetricKind.GROUP_MIN_DISTINCT,
            ),
        )
    )


def ranges_plan(rule_id, dataset):
    """Build a one-rule value-range plan on amount.

    Args:
        rule_id: Rule identity in the returned plan.
        dataset: DatasetRef the plan binds to.

    Returns:
        ExecutionPlan with a VALUE_RANGE rule over the column minimum.
    """
    return ExecutionPlan(
        (
            RuleSpec(
                rule_id,
                dataset,
                RuleKind.VALUE_RANGE,
                Predicate(Comparison.GE, Decimal(0)),
                ColumnRef("amount"),
                target=MetricKind.COLUMN_MIN,
            ),
        )
    )


def column_decisions(outcomes, columns):
    """Reduce portable outcomes to a per-column all-success decision.

    Args:
        outcomes: CheckOutcome sequence from ``execute_plan``; every
            outcome must cover a column named in ``columns`` (an
            unexpected column raises KeyError by design).
        columns: Column names to seed the decision map with.

    Returns:
        Dict mapping each column to True only when every outcome
        covering it succeeded.
    """
    decisions = {column: True for column in columns}
    for outcome in outcomes:
        legacy = outcome.to_legacy()
        column = legacy["details"]["column"]
        decisions[column] = decisions[column] and legacy["success"]
    return decisions
