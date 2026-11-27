# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The SQL adapter against the Spark adapter, on real Spark (SPEC-016).

Two adapters that claim one semantics must agree on one set of data. Every
corpus case is loaded twice from the same rows -- into a Spark frame and
into a DuckDB table, exactly -- and the two adapters are compared on their
metric values, their decisions, and their refusals.
"""

from decimal import Decimal
from fractions import Fraction

import pytest
import sqlalchemy as sa

from dq import spark_adapter, sql_adapter
from dq.exceptions import ConfigurationError
from dq.sdk.fixtures import DATASET_NAME, build_plan, decisions, load_cases, python_rows
from dq.sql_adapter import SqlDataset, execute_plan, validated_bindings
from dq.tests.fixture_frames import spark_frame
from dq.tests.sql_support import corpus_table, insert

pytestmark = pytest.mark.spark

pytest.importorskip("pyarrow", reason="the comparison needs the arrow extra")
pytest.importorskip("duckdb", reason="the certified lane needs duckdb")

EVERY_CASE = load_cases()
DECIDABLE = [case for case in EVERY_CASE if not case.expect_error]
FAIL_CLOSED = [case for case in EVERY_CASE if case.expect_error]


def both(case, spark):
    """The same rows as a Spark frame and as a DuckDB table, exactly loaded."""
    engine = sa.create_engine("duckdb:///:memory:")
    table = corpus_table(case, sa.MetaData())
    table.metadata.create_all(engine)
    insert(
        table, [dict(zip(case.column_names, row)) for row in python_rows(case)], engine
    )
    return {DATASET_NAME: spark_frame(spark, case)}, {
        DATASET_NAME: SqlDataset(table, engine)
    }


def spark_values(plan, datasets):
    return spark_adapter.compute_plan_values(
        plan, spark_adapter._validated_bindings(plan, datasets)
    )


def sql_values(plan, datasets):
    return sql_adapter.compute_plan_values(plan, validated_bindings(plan, datasets))


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_both_adapters_compute_the_corpus_metrics_over_the_same_rows(spark, case):
    on_spark, on_sql = both(case, spark)
    assert sql_values(build_plan(case), on_sql) == spark_values(
        build_plan(case), on_spark
    )


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_both_adapters_decide_the_corpus_the_same_way(spark, case):
    on_spark, on_sql = both(case, spark)
    plan = build_plan(case)
    by_spark = spark_adapter.execute_plan(plan, on_spark)
    by_sql = execute_plan(plan, on_sql)
    assert decisions(by_sql) == decisions(by_spark) == dict(case.expected)
    for left, right in zip(by_spark, by_sql):
        details, other = left.to_legacy()["details"], right.to_legacy()["details"]
        details["adapter"] = other["adapter"] = None
        assert details == other
    assert by_sql[0].to_legacy()["details"]["adapter"] == {
        "name": "sql",
        "version": "1",
    }


@pytest.mark.parametrize("case", FAIL_CLOSED, ids=lambda case: case.id)
def test_both_adapters_refuse_in_the_same_words(spark, case):
    on_spark, on_sql = both(case, spark)
    plan = build_plan(case)
    with pytest.raises(ConfigurationError) as by_spark:
        spark_adapter.execute_plan(plan, on_spark)
    with pytest.raises(ConfigurationError) as by_sql:
        execute_plan(plan, on_sql)
    if case.id == "ranges-text-bounds-fail-closed":
        # Spark computes the least string and then refuses its type; SQL
        # knows the column's type beforehand and refuses before any query.
        assert "unsupported type" in str(by_sql.value)
        assert "unsupported type" in str(by_spark.value)
    else:
        assert str(by_sql.value) == str(by_spark.value)


def test_a_decimal_no_double_holds_is_kept_exact_by_both(spark):
    """The case two adapters could both get wrong: a decimal a double cannot
    tell from 0.1, loaded two ways and compared with what was written."""
    case = next(c for c in DECIDABLE if c.id == "ranges-decimal-bounds-are-exact")
    on_spark, on_sql = both(case, spark)
    plan = build_plan(case)
    high = next(metric for metric in plan.metrics if metric.kind.value == "column_max")
    written = Fraction(Decimal("0.10000000000000000001"))
    assert (
        sql_values(plan, on_sql)[high] == spark_values(plan, on_spark)[high] == written
    )
    assert Fraction(0.1) != written
