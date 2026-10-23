# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the Delta Change Data Feed adapter (ADR-011 role B).

Metric computation runs against plain Spark fixtures (kernel parity
with dq.spark_adapter over identical frames); the CDF reader itself is
pinned with a fake SparkSession, so no Delta table or delta-spark
classpath is required.
"""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from dq.cdf_adapter import (
    ADAPTER_NAME,
    ADAPTER_VERSION,
    CAPABILITIES,
    GROUP_CAPABILITIES,
    RANGES_CAPABILITIES,
    execute_cdf_plan,
    read_changes,
)
from dq.exceptions import ConfigurationError
from dq.plan import (
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
    SEMANTIC_GROUPS_VERSION,
    SEMANTIC_RANGES_VERSION,
    SEMANTICS_VERSION,
)
from dq.spark_adapter import execute_plan
from dq.tests.test_package_contract import _run_with_blocked_imports
from dq.tests.helpers import legacy_by_rule
from dq.tests.plan_support import groups_plan, ranges_plan

DATASET = DatasetRef("orders", "c" * 64)


def counts_plan():
    return ExecutionPlan(
        (
            RuleSpec(
                "row_floor",
                DATASET,
                RuleKind.SIZE,
                Predicate(Comparison.GE, Decimal(1)),
            ),
            RuleSpec(
                "id_complete",
                DATASET,
                RuleKind.COMPLETENESS,
                Predicate(Comparison.GE, Decimal("0.5")),
                ColumnRef("id"),
            ),
        )
    )


@pytest.fixture
def mixed_changes(spark):
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("amount", DoubleType(), True),
        ]
    )
    return spark.createDataFrame(
        [
            ("o1", "east", 10.0),
            ("o2", "west", None),
            (None, "east", -1.5),
            ("o3", "west", float("nan")),
        ],
        schema,
    )


# ---------------------------------------------------------- capabilities


def test_capabilities_mirror_the_spark_adapter_under_a_cdf_name():
    assert ADAPTER_NAME == "spark-cdf" and ADAPTER_VERSION == "1"
    assert CAPABILITIES.semantic_version == SEMANTICS_VERSION
    assert CAPABILITIES.metrics == frozenset(
        {MetricKind.ROW_COUNT, MetricKind.PRESENT_COUNT}
    )
    assert GROUP_CAPABILITIES.semantic_version == SEMANTIC_GROUPS_VERSION
    assert GROUP_CAPABILITIES.metrics == frozenset(
        {
            MetricKind.GROUP_COUNT,
            MetricKind.GROUP_MIN_DISTINCT,
            MetricKind.GROUP_MAX_DISTINCT,
        }
    )
    assert RANGES_CAPABILITIES.semantic_version == SEMANTIC_RANGES_VERSION
    assert RANGES_CAPABILITIES.metrics == frozenset(
        {MetricKind.COLUMN_COUNT, MetricKind.COLUMN_MIN, MetricKind.COLUMN_MAX}
    )


# -------------------------------------------------------- kernel parity


def test_cdf_counts_match_full_snapshot_execution(spark, mixed_changes):
    cdf = execute_cdf_plan(counts_plan(), mixed_changes)
    full = execute_plan(counts_plan(), {"orders": mixed_changes})
    by_rule_cdf = legacy_by_rule(cdf)
    by_rule_full = legacy_by_rule(full)
    assert set(by_rule_cdf) == set(by_rule_full)
    for rule_id, outcome in by_rule_cdf.items():
        assert outcome["success"] == by_rule_full[rule_id]["success"]
        assert (
            outcome["details"]["observed"]
            == by_rule_full[rule_id]["details"]["observed"]
        )
        assert outcome["details"]["adapter"]["name"] == "spark-cdf"
        assert by_rule_full[rule_id]["details"]["adapter"]["name"] == "spark"


def test_cdf_groups_and_ranges_match_full_snapshot(spark, mixed_changes):
    from dq.tests.plan_support import GROUPS_ROWS
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("region", StringType(), True),
            StructField("id", StringType(), True),
            StructField("amount", DoubleType(), True),
        ]
    )
    changes = spark.createDataFrame(GROUPS_ROWS, schema)

    groups = groups_plan("cdf_grp_min", DATASET)
    cdf_g = legacy_by_rule(execute_cdf_plan(groups, changes))
    full_g = legacy_by_rule(execute_plan(groups, {"orders": changes}))
    assert cdf_g["cdf_grp_min"]["success"] == full_g["cdf_grp_min"]["success"]
    assert (
        cdf_g["cdf_grp_min"]["details"]["observed"]
        == full_g["cdf_grp_min"]["details"]["observed"]
    )

    ranges = ranges_plan("cdf_rng_min", DATASET)
    cdf_r = legacy_by_rule(execute_cdf_plan(ranges, mixed_changes))
    full_r = legacy_by_rule(execute_plan(ranges, {"orders": mixed_changes}))
    assert cdf_r["cdf_rng_min"]["success"] == full_r["cdf_rng_min"]["success"]
    assert (
        cdf_r["cdf_rng_min"]["details"]["observed"]
        == full_r["cdf_rng_min"]["details"]["observed"]
    )


def test_empty_change_batch_pins_delta_scoped_semantics(spark):
    from pyspark.sql.types import StringType, StructField, StructType

    empty = spark.createDataFrame(
        [], StructType([StructField("id", StringType(), True)])
    )
    plan = ExecutionPlan(
        (
            RuleSpec(
                "id_complete",
                DATASET,
                RuleKind.COMPLETENESS,
                Predicate(Comparison.GE, Decimal("0.5")),
                ColumnRef("id"),
            ),
        )
    )
    (outcome,) = execute_cdf_plan(plan, empty)
    legacy = outcome.to_legacy()
    # kernel contract: completeness fails on empty input
    assert legacy["success"] is False
    assert legacy["details"]["reason"] == "empty_dataset"

    empty_grouped = spark.createDataFrame(
        [],
        StructType(
            [StructField("region", StringType()), StructField("id", StringType())]
        ),
    )
    grouped = groups_plan("g", DATASET)
    (vacuous,) = execute_cdf_plan(grouped, empty_grouped)
    assert vacuous.to_legacy()["success"] is True
    assert vacuous.to_legacy()["details"]["reason"] == "no_groups"


def test_cdf_plan_must_target_one_dataset(spark, mixed_changes):
    other = DatasetRef("other", "d" * 64)
    plan = ExecutionPlan(
        (
            RuleSpec("a", DATASET, RuleKind.SIZE, Predicate(Comparison.GE, Decimal(1))),
            RuleSpec("b", other, RuleKind.SIZE, Predicate(Comparison.GE, Decimal(1))),
        )
    )
    with pytest.raises(ConfigurationError, match="exactly one dataset"):
        execute_cdf_plan(plan, mixed_changes)


# ------------------------------------------------------------- reader


class _FakeChanges:
    # stands in for the _change_type Column the adapter resolves on the frame
    _change_type = SimpleNamespace(isin=lambda types: ("isin", types))

    def __getitem__(self, key):
        return getattr(self, key)

    def filter(self, condition):
        self.filter_condition = str(condition)
        return self

    def drop(self, *columns):
        self.dropped = columns
        return self


def test_read_changes_pins_the_cdf_window_and_table():
    from types import SimpleNamespace

    changes = _FakeChanges()
    captured = {}

    class Reader:
        def option(self, key, value):
            captured.setdefault("options", []).append((key, value))
            return self

        def table(self, ref):
            captured["table"] = ref
            return changes

    spark = SimpleNamespace(read=Reader())
    result = read_changes(spark, "main.dq.orders", 5, 9)
    assert result is changes
    assert captured["options"] == [
        ("readChangeFeed", "true"),
        ("startingVersion", "5"),
        ("endingVersion", "9"),
    ]
    assert captured["table"] == "main.dq.orders"
    assert (
        "insert" in changes.filter_condition
        and "update_postimage" in changes.filter_condition
    )
    assert changes.dropped == ("_change_type", "_commit_version", "_commit_timestamp")


def test_read_changes_omits_ending_version_when_unset():
    from types import SimpleNamespace

    changes = _FakeChanges()
    captured = {}

    class Reader:
        def option(self, key, value):
            captured.setdefault("options", []).append((key, value))
            return self

        def table(self, ref):
            return changes

    read_changes(SimpleNamespace(read=Reader()), "t", 3)
    assert [k for k, _ in captured["options"]] == ["readChangeFeed", "startingVersion"]


def test_read_changes_rejects_bad_versions_and_blank_table():
    from types import SimpleNamespace

    spark = SimpleNamespace(read=None)
    with pytest.raises(ConfigurationError, match="non-negative integer"):
        read_changes(spark, "t", -1)
    with pytest.raises(ConfigurationError, match="non-negative integer"):
        read_changes(spark, "t", True)
    with pytest.raises(ConfigurationError, match=">= starting_version"):
        read_changes(spark, "t", 5, 4)
    with pytest.raises(ConfigurationError, match="blank"):
        read_changes(spark, "  ", 0)


def test_read_changes_translates_missing_feed_into_enablement_hint():
    from types import SimpleNamespace

    from pyspark.sql.utils import AnalysisException

    def make_exc(message):
        try:
            raise AnalysisException(message)
        except AnalysisException as exc:
            return exc

    analysis_error = make_exc(
        "Table is missing change data feed support; enable delta.enableChangeDataFeed"
    )

    class Reader:
        def option(self, key, value):
            return self

        def table(self, ref):
            raise analysis_error

    with pytest.raises(ConfigurationError, match="delta.enableChangeDataFeed"):
        read_changes(SimpleNamespace(read=Reader()), "t", 0)


def test_read_changes_filter_runs_against_a_real_dataframe(spark):
    # A struct fake let the isin-tuple crash ship once; this test runs
    # the real filter/drop pipeline on a real session DataFrame, with
    # only spark.read stubbed to hand back a change-feed-like frame.
    from types import SimpleNamespace

    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("_change_type", StringType(), True),
            StructField("_commit_version", LongType(), True),
            StructField("_commit_timestamp", TimestampType(), True),
        ]
    )
    import datetime as dt

    frame = spark.createDataFrame(
        [
            ("a", "insert", 5, dt.datetime(2026, 10, 23)),
            ("b", "delete", 5, dt.datetime(2026, 10, 23)),
            ("c", "update_preimage", 6, dt.datetime(2026, 10, 23)),
            ("d", "update_postimage", 6, dt.datetime(2026, 10, 23)),
        ],
        schema,
    )

    class Reader:
        def option(self, key, value):
            return self

        def table(self, ref):
            return frame

    # SparkSession.read is a read-only property; pass a shimming session
    shim = SimpleNamespace(read=Reader())
    changes = read_changes(shim, "main.dq.orders", 5, 6)
    rows = sorted(row["id"] for row in changes.collect())
    assert rows == ["a", "d"]
    assert set(changes.schema.fieldNames()) == {"id"}


# ------------------------------------------------------ import hygiene


def test_cdf_adapter_imports_without_pyspark():
    result = _run_with_blocked_imports(
        "pyspark", "pydeequ", statement="import dq.cdf_adapter; print('ok')"
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


pytestmark = pytest.mark.spark
