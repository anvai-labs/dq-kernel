# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Live GX Core round trips for the greatexpectations engine.

Skipped entirely when the optional extra is absent (unit-lane contract
tests with a mocked gx live in test_greatexpectations_unit.py).
"""

import pytest

pytest.importorskip("great_expectations")

from pyhocon import ConfigFactory

from dq.engine.greatexpectations.greatexpectations_engine import (
    GreatexpectationsEngine,
)
from dq.gate import evaluate_gate
from dq.sinks import InMemorySink
from dq.tests.helpers import (
    assert_any_failure,
    assert_overall_success,
    assert_sink_received_both_artifacts,
)


def gx_config(expectations):
    return ConfigFactory.from_dict(
        {
            "name": "gx-test",
            "engine": "greatexpectations",
            "expectations": expectations,
        }
    )


@pytest.fixture
def sample_frame(spark):
    return spark.createDataFrame(
        [(1, "alice@example.com"), (2, "bob@example.com"), (3, "cathy@example.com")],
        ["id", "email"],
    )


def test_greatexpectations_engine_success(spark, sample_frame):
    config = gx_config(
        [
            {"type": "expect_column_values_to_not_be_null", "column": "id"},
            {"type": "expect_column_values_to_be_unique", "column": "id"},
            {
                "type": "expect_column_values_to_match_regex",
                "column": "email",
                "regex": r".+@.+",
            },
            {
                "type": "expect_table_row_count_to_be_between",
                "min_value": 3,
                "max_value": 3,
            },
        ]
    )
    results = GreatexpectationsEngine(config).apply(sample_frame)
    assert_overall_success(results, "greatexpectations")


def test_greatexpectations_engine_reports_failures(spark, sample_frame):
    config = gx_config(
        [
            {"type": "expect_column_values_to_not_be_null", "column": "id"},
            {
                "type": "expect_column_values_to_be_in_set",
                "column": "id",
                "value_set": [1, 2],
            },
        ]
    )
    results = GreatexpectationsEngine(config).apply(sample_frame)
    assert_any_failure(results, "greatexpectations")


def test_greatexpectations_table_level_expectation_needs_no_column(spark, sample_frame):
    config = gx_config(
        [
            {
                "type": "expect_table_row_count_to_be_between",
                "min_value": 1,
                "max_value": 2,
            }
        ]
    )
    results = GreatexpectationsEngine(config).apply(sample_frame)
    assert_any_failure(results, "table-level")


def test_greatexpectations_rejects_unknown_expectation_type(spark, sample_frame):
    config = gx_config([{"type": "expect_nonexistent_thing", "column": "id"}])
    with pytest.raises(ValueError, match="Unsupported expectation type"):
        GreatexpectationsEngine(config).apply(sample_frame)


def test_greatexpectations_rejects_expectation_without_type(spark, sample_frame):
    config = gx_config([{"column": "id"}])
    with pytest.raises(ValueError, match="requires a 'type'"):
        GreatexpectationsEngine(config).apply(sample_frame)


def test_greatexpectations_persists_both_artifacts_under_one_key(spark, sample_frame):
    config = gx_config(
        [{"type": "expect_column_values_to_not_be_null", "column": "id"}]
    )
    sink = InMemorySink()
    results = GreatexpectationsEngine(config).apply(sample_frame, repository=sink)
    assert_sink_received_both_artifacts(results, sink)


def test_greatexpectations_reuses_batch_definition_across_applies(spark, sample_frame):
    config = gx_config(
        [{"type": "expect_column_values_to_not_be_null", "column": "id"}]
    )
    engine = GreatexpectationsEngine(config)
    engine.apply(sample_frame)
    cached = engine._gx_batch_definition
    engine.apply(sample_frame)
    assert engine._gx_batch_definition is cached


# ------------------------------------------- attribution (each result to its own check)


#: A table-level expectation defined between two on one column. GX groups
#: expectations by column, so it returns these in the order 0, 2, 1 -- not
#: the order they are defined in (observed on GX 1.24.1 and 1.25.0).
INTERLEAVED = [
    {"type": "expect_column_values_to_not_be_null", "column": "id"},
    {"type": "expect_table_row_count_to_be_between", "min_value": 1, "max_value": 2},
    {"type": "expect_column_values_to_be_unique", "column": "id"},
]


def _with_criticalities(row_count, unique):
    return [
        dict(INTERLEAVED[0]),
        dict(INTERLEAVED[1], criticality=row_count),
        dict(INTERLEAVED[2], criticality=unique),
    ]


def test_each_result_is_attributed_to_its_own_expectation(spark, sample_frame):
    """``sample_frame`` has three rows, distinct ids and no nulls.

    So of INTERLEAVED only the row count fails. GX sends that result back
    third; read by position it lands on the uniqueness check, and the
    uniqueness result -- a pass -- on the row count.
    """
    results = GreatexpectationsEngine(gx_config(INTERLEAVED)).apply(sample_frame)
    assert [result["check"] for result in results] == [
        "expect_column_values_to_not_be_null_id",
        "expect_table_row_count_to_be_between",
        "expect_column_values_to_be_unique_id",
    ]
    assert {result["check"]: result["success"] for result in results} == {
        "expect_column_values_to_not_be_null_id": True,
        "expect_table_row_count_to_be_between": False,
        "expect_column_values_to_be_unique_id": True,
    }
    by_check = {result["check"]: result for result in results}
    # Each carries its own observations: the row count saw three rows.
    assert (
        by_check["expect_table_row_count_to_be_between"]["details"]["observed_value"]
        == 3
    )


@pytest.mark.parametrize(
    "criticalities, blocks",
    [
        (("warn", "error"), False),  # the check that failed is warn-level
        (("error", "warn"), True),  # the check that failed is error-level
    ],
    ids=["failed_check_is_warn", "failed_check_is_error"],
)
def test_the_gate_reads_the_criticality_of_the_check_that_failed(
    spark, sample_frame, criticalities, blocks
):
    """``criticalities`` is (row count, uniqueness); only the row count fails."""
    config = gx_config(_with_criticalities(*criticalities))
    results = GreatexpectationsEngine(config).apply(sample_frame)
    decision = evaluate_gate(results, "error")
    assert decision.passed is not blocks
    assert decision.blocking == (
        ("expect_table_row_count_to_be_between",) if blocks else ()
    )
    assert decision.advisory == (
        ("expect_table_row_count_to_be_between",) if not blocks else ()
    )


def test_two_expectations_gx_would_treat_as_one_are_refused(spark, sample_frame):
    """Same type and arguments under two names: GX keeps one copy, runs one."""
    config = gx_config(
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
        ]
    )
    with pytest.raises(
        ValueError, match="Checks 0 and 1 are the same expectation to GX"
    ):
        GreatexpectationsEngine(config).apply(sample_frame)


pytestmark = pytest.mark.spark
