# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Imported outcomes reach the framework's own sinks (SPEC-013).

An imported outcome is an ordinary outcome dictionary, so what takes those
takes it -- framed as the framework frames its own (``outcome_frames``),
with the schema stated. A real Spark session and the durable SQL sink on a
real database show it end to end.
"""

import hashlib
import json

import pytest

pytest.importorskip("sqlalchemy")

from dq.engine.dq_engine import outcome_frames  # noqa: E402
from dq.harvest import harvest  # noqa: E402
from dq.sqlalchemy_sink import SqlAlchemyOutcomeSink  # noqa: E402
from dq.tests.harvest_support import DBT_EXAMPLE  # noqa: E402
from dq.tests.sink_support import provision, rows, sqlite_engine  # noqa: E402

pytestmark = pytest.mark.spark

ORIGIN = {
    "system": "dbt",
    "reference": "target/run_results.json",
    "run": "11cfc1e8-98df-4b89-9570-fc85ee756525",
}


def sink_on(tmp_path, *, arrow):
    engine = sqlite_engine(tmp_path)
    sink = SqlAlchemyOutcomeSink(
        engine, dataset="orders", namespace="team_a", retention_days=30, arrow=arrow
    )
    provision(engine, sink.ddl_statements())
    return engine, sink


def stored(engine):
    return rows(
        engine,
        "SELECT artifact_type, payload FROM dq.artifacts ORDER BY artifact_type, seq",
    )


@pytest.mark.parametrize("arrow", [False, True], ids=["row_path", "arrow_path"])
def test_imported_outcomes_are_written_by_the_sql_sink(tmp_path, spark, arrow):
    if arrow:
        pytest.importorskip("pyarrow")
    source = DBT_EXAMPLE.read_bytes()
    outcomes = harvest(source, reference="target/run_results.json")
    engine, sink = sink_on(tmp_path, arrow=arrow)

    metrics, verifications = outcome_frames(spark, outcomes)
    sink.save(metrics, "metrics", 42)
    sink.save(verifications, "verifications", 42)

    written = stored(engine)
    assert [row["artifact_type"] for row in written] == ["metrics"] * 6 + [
        "verifications"
    ] * 6
    assert [json.loads(row["payload"])["check"] for row in written[:6]] == [
        outcome["check"] for outcome in outcomes
    ]
    # A verification row keeps `details` as JSON text, as for any outcome.
    details = [json.loads(json.loads(row["payload"])["details"]) for row in written[6:]]
    assert [item["status"] for item in details] == [
        "warn",
        "error",
        "warn",
        "pass",
        "fail",
        "skipped",
    ]
    # The provenance crossed whole: it is what tells this evidence apart
    # from the framework's own.
    assert details[0]["origin"] == {
        **ORIGIN,
        "sha256": hashlib.sha256(source).hexdigest(),
    }


def test_an_inferred_schema_would_lose_the_shape(spark):
    """Why the schema is stated. A canary for the warning in ``outcome_frames``.

    If this fails, Spark's inference has stopped stringifying nested
    dictionaries, and the warning can go with it.
    """
    outcomes = harvest(DBT_EXAMPLE.read_bytes(), reference="target/run_results.json")
    inferred = spark.createDataFrame(outcomes).collect()[0].asDict(recursive=True)
    assert isinstance(inferred["details"]["origin"], str)  # "{reference=..., ...}"
    stated = outcome_frames(spark, outcomes)[1].collect()[0].asDict()
    assert json.loads(stated["details"])["origin"] == outcomes[0]["details"]["origin"]
