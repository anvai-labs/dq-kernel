# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-end envelope dispatch: mixed-semantics rulesets on real Spark.

The pure contracts in ``test_envelope.py`` pin composition and dispatch on
hand-built values. These tests pin the production path:
``dq.spark_adapter.execute_envelope`` computes every subplan's metrics with
its own semantics (counts/v1, groups/v1, ranges/v1) and returns the
envelope's one aggregate verdict.
"""

from decimal import Decimal

import pytest

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from dq.exceptions import ConfigurationError
from dq.envelope import RulesetEnvelope
from dq.plan import (
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
)
from dq.spark_adapter import execute_envelope

pytestmark = pytest.mark.spark

DATASET = DatasetRef("orders", "d" * 64)

_ROWS = [
    (1, "east", 10.0),
    (2, "west", 0.0),
    (3, "east", 5.5),
    (None, "west", 7.25),
]

_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("region", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)


def counts_plan(completeness_threshold=Decimal("0.5")):
    return ExecutionPlan(
        (
            RuleSpec(
                "orders_size",
                DATASET,
                RuleKind.SIZE,
                Predicate(Comparison.GE, Decimal(1)),
            ),
            RuleSpec(
                "id_complete",
                DATASET,
                RuleKind.COMPLETENESS,
                Predicate(Comparison.GE, completeness_threshold),
                ColumnRef("id"),
            ),
        )
    )


def groups_plan():
    return ExecutionPlan(
        (
            RuleSpec(
                "region_group_min_distinct",
                DATASET,
                RuleKind.GROUPED_DISTINCT,
                Predicate(Comparison.GE, Decimal(1)),
                ColumnRef("id"),
                group_by=(ColumnRef("region"),),
                target=MetricKind.GROUP_MIN_DISTINCT,
            ),
        )
    )


def ranges_plan():
    return ExecutionPlan(
        (
            RuleSpec(
                "amount_min",
                DATASET,
                RuleKind.VALUE_RANGE,
                Predicate(Comparison.GE, Decimal(0)),
                ColumnRef("amount"),
                target=MetricKind.COLUMN_MIN,
            ),
        )
    )


def orders_dataframe(spark):
    return spark.createDataFrame(_ROWS, _SCHEMA)


def outcome_by_rule(outcomes):
    return {outcome.to_legacy()["check"]: outcome for outcome in outcomes}


def test_execute_envelope_returns_aggregate_outcomes(spark):
    envelope = RulesetEnvelope((counts_plan(), groups_plan(), ranges_plan()))
    outcomes = execute_envelope(envelope, {"orders": orders_dataframe(spark)})
    by_rule = outcome_by_rule(outcomes)
    assert set(by_rule) == {
        "orders_size",
        "id_complete",
        "region_group_min_distinct",
        "amount_min",
    }
    assert all(outcome.success is True for outcome in by_rule.values())


def test_execute_envelope_mixed_verdict_keeps_per_rule_results(spark):
    envelope = RulesetEnvelope(
        (counts_plan(completeness_threshold=Decimal("0.9")), groups_plan())
    )
    outcomes = execute_envelope(envelope, {"orders": orders_dataframe(spark)})
    by_rule = outcome_by_rule(outcomes)
    assert by_rule["id_complete"].success is False
    assert by_rule["orders_size"].success is True
    assert by_rule["region_group_min_distinct"].success is True


def test_execute_envelope_single_plan_envelope_matches_execute_plan(spark):
    envelope = RulesetEnvelope((ranges_plan(),))
    outcomes = execute_envelope(envelope, {"orders": orders_dataframe(spark)})
    by_rule = outcome_by_rule(outcomes)
    assert by_rule["amount_min"].success is True


def test_execute_envelope_rejects_bare_plan(spark):
    with pytest.raises(ConfigurationError, match="RulesetEnvelope"):
        execute_envelope(counts_plan(), {"orders": orders_dataframe(spark)})


def test_execute_envelope_reports_missing_bindings():
    envelope = RulesetEnvelope((counts_plan(),))
    with pytest.raises(ConfigurationError, match="orders"):
        execute_envelope(envelope, {})
