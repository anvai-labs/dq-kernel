# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the Lakeflow pipeline-expectations harvester.

Every workspace interaction is faked (DatabricksCheck test pattern);
no live Databricks is needed and no JVM -- the module only duck-types
the SparkSession used to query the event log.
"""

from types import SimpleNamespace

import pytest

pytest.importorskip("databricks.sdk")

from dq.engine.databricks.pipeline_expectations import (
    PipelineExpectationsHarvester,
    expectation_outcomes,
)
from dq.notifier import GateEvent
from dq.outcomes import normalize_outcomes
from dq.report import build_report


def _flow_row(timestamp, outputs, as_json=False):
    details = {"flow_progress": {"outputs": outputs}}
    if as_json:
        import json

        details = json.dumps(details)
    return {"timestamp": timestamp, "details": details}


def _output(expectations):
    return {"data_quality": {"expectations": expectations}}


@pytest.fixture
def sample_rows():
    return [
        _flow_row(
            1_700_000_000_000,
            {
                "silver_orders": _output(
                    [
                        {
                            "name": "order_id_not_null",
                            "passed_records": 98,
                            "dropped_records": 2,
                            "batch_id": "b-1",
                        },
                        {
                            "name": "amount_positive",
                            "passed_records": 100,
                            "dropped_records": 0,
                        },
                    ]
                ),
                "gold_summary": _output(
                    [
                        {
                            "name": "row_count_floor",
                            "passed_records": 100,
                            "dropped_records": 0,
                        }
                    ]
                ),
            },
        ),
        _flow_row(
            1_700_000_060_000,
            {
                "silver_orders": _output(
                    [
                        {
                            "name": "amount_positive",
                            "passed_records": 50,
                            "dropped_records": 0,
                        }
                    ]
                )
            },
        ),
    ]


# ------------------------------------------------------------ normalizer


def test_normalizer_names_checks_dataset_dot_expectation(sample_rows):
    outcomes = expectation_outcomes(sample_rows)
    # latest-wins: amount_positive appears in two emissions, kept once
    assert [o["check"] for o in outcomes] == [
        "silver_orders.order_id_not_null",
        "silver_orders.amount_positive",
        "gold_summary.row_count_floor",
    ]
    latest = outcomes[1]["details"]
    assert latest["passed_records"] == 50  # newest emission


def test_normalizer_keep_all_returns_every_emission(sample_rows):
    outcomes = expectation_outcomes(sample_rows, keep="all")
    assert len(outcomes) == 4
    with pytest.raises(ValueError, match="keep must be"):
        expectation_outcomes(sample_rows, keep="nope")


def test_normalizer_fails_only_on_drops_by_default(sample_rows):
    outcomes = expectation_outcomes(sample_rows)
    by_check = {o["check"]: o["success"] for o in outcomes}
    assert by_check["silver_orders.order_id_not_null"] is False
    assert by_check["silver_orders.amount_positive"] is True
    assert by_check["gold_summary.row_count_floor"] is True


def test_normalizer_fail_on_drops_false_records_evidence_without_failing(sample_rows):
    outcomes = expectation_outcomes(sample_rows, fail_on_drops=False)
    assert all(o["success"] is True for o in outcomes)
    dropped = next(o for o in outcomes if o["check"].endswith("order_id_not_null"))
    assert dropped["details"]["dropped_records"] == 2


def test_normalizer_carries_counts_and_origin(sample_rows):
    outcome = expectation_outcomes(sample_rows)[0]
    assert outcome["details"] == {
        "dataset": "silver_orders",
        "expectation": "order_id_not_null",
        "passed_records": 98,
        "dropped_records": 2,
        "timestamp": 1_700_000_000_000,
        "batch_id": "b-1",
    }


def test_normalizer_coerces_datetime_timestamps_to_epoch_millis():
    import datetime as dt

    row = _flow_row(
        dt.datetime(2023, 11, 14, tzinfo=dt.timezone.utc),
        {"t": _output([{"name": "e", "passed_records": 1, "dropped_records": 0}])},
    )
    outcome = expectation_outcomes([row])[0]
    assert outcome["details"]["timestamp"] == int(
        dt.datetime(2023, 11, 14, tzinfo=dt.timezone.utc).timestamp() * 1000
    )


def test_normalizer_parses_json_string_details(sample_rows):
    outcomes = expectation_outcomes(
        [
            _flow_row(
                1,
                {
                    "t": _output(
                        [{"name": "e", "passed_records": 1, "dropped_records": 0}]
                    )
                },
                as_json=True,
            )
        ]
    )
    assert outcomes[0]["check"] == "t.e"


def test_normalizer_fails_closed_on_malformed_rows():
    with pytest.raises(ValueError, match="missing 'flow_progress'"):
        expectation_outcomes([{"timestamp": 1, "details": {"unrelated": {}}}])
    with pytest.raises(ValueError, match="missing 'silver.name'"):
        expectation_outcomes(
            [
                {
                    "timestamp": 1,
                    "details": {
                        "flow_progress": {
                            "outputs": {
                                "silver": _output(
                                    [{"passed_records": 1, "dropped_records": 0}]
                                )
                            }
                        }
                    },
                }
            ]
        )
    with pytest.raises(ValueError, match="non-object"):
        expectation_outcomes(
            [
                {
                    "timestamp": 1,
                    "details": {"flow_progress": {"outputs": {"silver": []}}},
                }
            ]
        )
    with pytest.raises(ValueError, match="non-numeric"):
        expectation_outcomes(
            [
                {
                    "timestamp": 1,
                    "details": {
                        "flow_progress": {
                            "outputs": {
                                "silver": _output(
                                    [
                                        {
                                            "name": "x",
                                            "passed_records": True,
                                            "dropped_records": 0,
                                        }
                                    ]
                                )
                            }
                        }
                    },
                }
            ]
        )
    with pytest.raises(ValueError, match="non-numeric"):
        expectation_outcomes(
            [
                {
                    "timestamp": 1,
                    "details": {
                        "flow_progress": {
                            "outputs": {
                                "silver": _output(
                                    [
                                        {
                                            "name": "x",
                                            "passed_records": "many",
                                            "dropped_records": 0,
                                        }
                                    ]
                                )
                            }
                        }
                    },
                }
            ]
        )


# ------------------------------------------------------------ constructor


def test_constructor_accepts_injected_client():
    client = SimpleNamespace()
    harvester = PipelineExpectationsHarvester(workspace_client=client)
    assert harvester._workspace_client is client


def test_constructor_rejects_client_plus_credentials():
    with pytest.raises(ValueError, match="cannot be combined"):
        PipelineExpectationsHarvester(
            workspace_client=SimpleNamespace(), databricks_token="t"
        )


def test_constructor_validates_url_and_token():
    with pytest.raises(ValueError, match="HTTPS origin"):
        PipelineExpectationsHarvester(databricks_url="http://host/path")
    with pytest.raises(ValueError, match="no whitespace"):
        PipelineExpectationsHarvester(databricks_token="has space")


# ---------------------------------------------------------------- fetch


class _FakeSpark:
    def __init__(self, rows):
        self._rows = rows
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        return SimpleNamespace(collect=lambda: list(self._rows))


def _client_with_event_log(
    catalog="main", schema="dq", name="pl_events", pipeline_id="abc-123"
):
    spec = SimpleNamespace(
        event_log=SimpleNamespace(catalog=catalog, schema=schema, name=name)
    )
    pipelines = SimpleNamespace(get=lambda pid: SimpleNamespace(spec=spec))
    calls = []

    def get(pid):
        calls.append(pid)
        return SimpleNamespace(spec=spec)

    return SimpleNamespace(pipelines=SimpleNamespace(get=get)), calls


def test_harvest_queries_the_event_log_tvf_with_since_filter(sample_rows):
    spark = _FakeSpark(sample_rows)
    harvester = PipelineExpectationsHarvester(workspace_client=SimpleNamespace())
    outcomes = harvester.harvest(
        spark, table_ref="main.dq.my_pipeline", since_ms=1_700_000_000_000
    )
    assert len(outcomes) == 3
    assert "FROM main.dq.my_pipeline" in spark.queries[0]
    assert "event_log(TABLE(" not in spark.queries[0]
    assert "flow_progress" in spark.queries[0]
    assert "timestamp_millis(1700000000000)" in spark.queries[0]


def test_harvest_resolves_event_log_table_from_pipeline_id(sample_rows):
    spark = _FakeSpark(sample_rows)
    client, calls = _client_with_event_log()
    harvester = PipelineExpectationsHarvester(workspace_client=client)
    outcomes = harvester.harvest(spark, pipeline_id="abc-123")
    assert calls == ["abc-123"]
    assert "FROM main.dq.pl_events" in spark.queries[0]
    assert len(outcomes) == 3


def test_harvest_rejects_ambiguous_and_missing_sources():
    harvester = PipelineExpectationsHarvester(workspace_client=SimpleNamespace())
    with pytest.raises(ValueError, match="exactly one of table_ref or pipeline_id"):
        harvester.harvest(_FakeSpark([]))
    with pytest.raises(ValueError, match="exactly one of table_ref or pipeline_id"):
        harvester.harvest(_FakeSpark([]), table_ref="a.b.c", pipeline_id="p")


def test_harvest_falls_back_to_managed_event_log_tvf():
    spark = _FakeSpark([])
    client, _ = _client_with_event_log()
    client.pipelines.get = lambda pid: SimpleNamespace(
        spec=SimpleNamespace(event_log=None)
    )
    harvester = PipelineExpectationsHarvester(workspace_client=client)
    harvester.harvest(spark, pipeline_id="p-1")
    assert "event_log('p-1')" in spark.queries[0]
    assert "event_log(TABLE(" not in spark.queries[0]


def test_harvest_empty_window_returns_empty_list():
    spark = _FakeSpark([])
    harvester = PipelineExpectationsHarvester(workspace_client=SimpleNamespace())
    outcomes = harvester.harvest(spark, table_ref="main.dq.my_pipeline")
    assert outcomes == []


# -------------------------------------------------------- evidence path


def test_harvested_outcomes_feed_report_and_gate(sample_rows):
    spark = _FakeSpark(sample_rows)
    harvester = PipelineExpectationsHarvester(workspace_client=SimpleNamespace())
    outcomes = harvester.harvest(spark, table_ref="main.dq.my_pipeline")

    normalized = normalize_outcomes(outcomes)
    report = build_report(
        results=outcomes,
        config_reference="pipelines.conf",
        config_sha256="a" * 64,
        dataset_id="orders:v1",
        dataset_sha256="b" * 64,
        application_id="app-1",
        spark_version="3.5.9",
    )
    assert report["summary"]["total"] == len(normalized) == 3
    assert report["summary"]["failed"] == 1

    event = GateEvent.from_outcomes(
        1, "orders:v1", outcomes, 1_700_000_000_000, "job-1"
    )
    assert event.status == "failed"
    assert "silver_orders.order_id_not_null" in event.failed
