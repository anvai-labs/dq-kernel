# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Live DQX round trips for the dqx engine.

Skipped entirely when the optional extra is absent (unit-lane contract
tests with a mocked dqx live in test_dqx_unit.py) and on Python 3.13,
where the dqx extra is not installable. In CI they run inside the main
test matrix (3.12 job) on the shared Spark session.
"""

import pytest

pytest.importorskip("databricks.labs.dqx")

pytestmark = pytest.mark.spark

from pyhocon import ConfigFactory

from dq.engine.dqx.dqx_engine import DqxEngine
from dq.sinks import InMemorySink
from dq.tests.helpers import (
    assert_any_failure,
    assert_sink_received_both_artifacts,
)


def dqx_config(checks):
    return ConfigFactory.from_dict(
        {"name": "dqx-test", "engine": "dqx", "checks": checks}
    )


def results_by_check(results):
    return {result["check"]: result for result in results}


@pytest.fixture
def sample_frame(spark):
    return spark.createDataFrame(
        [(1, "alice@example.com"), (2, None), (3, "cathy@example.com")],
        ["id", "email"],
    )


def test_dqx_engine_naming_and_success(sample_frame):
    config = dqx_config(
        [
            {"check": {"function": "is_not_null", "arguments": {"column": "id"}}},
            {
                "name": "emails_present",
                "criticality": "warn",
                "check": {"function": "is_not_null", "arguments": {"column": "email"}},
            },
        ]
    )
    results = DqxEngine(config).apply(sample_frame)
    assert len(results) == 2
    assert results[0]["check"] == "is_not_null_id"
    assert results[1]["check"] == "emails_present"
    # Row 2 has a null email: the id check passes, the warn check fails.
    assert results[0]["success"] is True
    assert results[1]["success"] is False
    assert results[1]["details"]["criticality"] == "warn"
    assert results[1]["details"]["failed_rows"] == 1
    assert results[1]["details"]["sample_messages"]


def test_dqx_engine_failures_and_persistence(sample_frame):
    config = dqx_config(
        [
            {"check": {"function": "is_not_null", "arguments": {"column": "email"}}},
            {
                "name": "ids_unique",
                "check": {"function": "is_unique", "arguments": {"columns": ["id"]}},
            },
        ]
    )
    sink = InMemorySink()
    results = DqxEngine(config).apply(sample_frame, repository=sink)
    assert_any_failure(results)
    by_check = results_by_check(results)
    assert by_check["is_not_null_email"]["details"]["failed_rows"] == 1
    assert by_check["is_not_null_email"]["details"]["criticality"] == "error"
    assert by_check["ids_unique"]["success"] is True
    assert_sink_received_both_artifacts(results, sink)


def test_dqx_for_each_column_reports_single_aggregate(sample_frame):
    config = dqx_config(
        [
            {
                "name": "non_empty",
                "criticality": "warn",
                "check": {
                    "function": "is_not_null_and_not_empty",
                    "for_each_column": ["id", "email"],
                },
            }
        ]
    )
    results = DqxEngine(config).apply(sample_frame)
    assert len(results) == 1
    assert results[0]["check"] == "non_empty"
    assert results[0]["success"] is False
    assert results[0]["details"]["failed_rows"] == 1  # only the null email


def test_dqx_unknown_function_fails_closed(sample_frame):
    config = dqx_config(
        [
            {
                "check": {
                    "function": "definitely_not_a_real_check",
                    "arguments": {"column": "id"},
                }
            }
        ]
    )
    with pytest.raises(ValueError):
        DqxEngine(config).apply(sample_frame)
