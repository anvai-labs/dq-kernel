# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from pyhocon import ConfigFactory

from dq.engine.deequ.deequ_engine import DeequEngine
from dq.sinks import InMemorySink
from dq.tests.helpers import assert_any_failure, assert_overall_success


@pytest.fixture
def sample_success_dataframe(spark):
    data = [
        ("Alice", 34, "alice@example.com"),
        ("Bob", 45, "bob@example.com"),
        ("Catherine", 4, "cathy@example.com"),
    ]
    columns = ["name", "age", "email"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def sample_failure_dataframe(spark):
    data = [
        ("Alice", -2, "alice@example.com"),
        ("Bob", 45, "bob@example.com"),
        ("Catherine", None, "not-a-valid-email"),
    ]
    columns = ["name", "age", "email"]
    return spark.createDataFrame(data, columns)


def deequ_checks_config(single_check_mode: bool):
    """Build the four-check deequ config used by the engine tests.

    Args:
        single_check_mode: True bundles every constraint in one Check;
            False emits one Check per constraint.

    Returns:
        The parsed ``deequ`` config subtree.
    """
    config_str = f"""
    deequ {{
        name = "myrule1"
        engine = "deequ"
        single_check_mode = {single_check_mode}
        checks = [
            {{
                constraint = "containsEmail"
                column = "email"
                assertion = "lambda i : i == 1"
            }},
            {{
                constraint = "isPositive"
                column = "age"
            }},
            {{
                alias = "check-datatype"
                constraint = "hasDataType"
                column = "age"
                description = "Check if age is integer type"
                hint = "Check failed as age may not be integer type"
                datatype = Integral
                assertion = "lambda i : i == 1"
            }},
            {{
                constraint = "hasCompleteness"
                column = "name"
                assertion = "lambda i : i == 1"
                hint = "Check failed as name may not be complete"
                description = "Check if name is complete"
            }}
        ]
    }}

    """
    return ConfigFactory.parse_string(config_str).get("deequ", {})


@pytest.mark.parametrize(
    "single_check_mode", [True, False], ids=["single_check", "multi_check"]
)
def test_deequ_engine_success(sample_success_dataframe, single_check_mode):
    engine = DeequEngine(deequ_checks_config(single_check_mode))
    summarymetrics = engine.apply(sample_success_dataframe, repository=None)
    assert_overall_success(summarymetrics, "deequ")


@pytest.mark.parametrize(
    "single_check_mode", [True, False], ids=["single_check", "multi_check"]
)
def test_deequ_engine_failure(sample_failure_dataframe, single_check_mode):
    engine = DeequEngine(deequ_checks_config(single_check_mode))
    summarymetrics = engine.apply(sample_failure_dataframe, repository=None)
    assert_any_failure(summarymetrics, "deequ")


def test_deequ_engine_persists_repository_artifacts(spark, sample_success_dataframe):
    # The persist block lives on DQEngine._persist since the template
    # refactor; exercise it end-to-end so the shared path stays covered.
    sink = InMemorySink()
    engine = DeequEngine(deequ_checks_config(single_check_mode=True))
    engine.apply(sample_success_dataframe, repository=sink)

    writes = sink.writes()
    metric_types = {metric_type for metric_type, _ in writes}
    assert metric_types == {"metrics", "verifications"}
    # Both artifacts must land under one ResultKey for the run.
    assert len({key for _, key in writes}) == 1
    assert sink.rows("metrics", next(iter(writes))[1])


pytestmark = pytest.mark.spark
