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


pytestmark = pytest.mark.spark
