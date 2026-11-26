# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""A Spark DataFrame's rows reach the durable SQL sink through Arrow (SPEC-007).

A fake cannot show that Spark's Arrow export and the sink agree, so these
run a real Spark session against a real database. Classic PySpark 3.5, the
version this project pins, has no public Arrow export: only
``_collect_as_arrow()``. The tests offer the sink exactly that and nothing
else, so a test about Arrow cannot pass by falling back to the frame's rows.
"""

import json
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyhocon import ConfigFactory

pytest.importorskip("sqlalchemy", reason="the durable sink needs the sqlalchemy extra")

from dq.engine.checkspec import build_result
from dq.engine.dq_engine import DQEngine
from dq.sqlalchemy_sink import SqlAlchemyOutcomeSink
from dq.tests.sink_support import provision, rows, sqlite_engine

pytestmark = pytest.mark.spark

TYPED = (
    (
        Decimal("1.50"),
        datetime(2026, 11, 25, 10),
        date(2026, 11, 25),
        float("nan"),
        None,
        b"\x01\x02",
        [1, 2],
        {"a": "x"},
    ),
)
TYPED_SCHEMA = (
    "amount decimal(10,2), at timestamp, day date, value double, missing double, "
    "blob binary, tags array<long>, nested struct<a string>"
)


class ArrowOnly:
    """A Spark DataFrame with nothing but the Arrow export PySpark 3.5 has."""

    def __init__(self, frame):
        self._frame = frame

    def _collect_as_arrow(self):
        return self._frame._collect_as_arrow()


@pytest.fixture
def engine(tmp_path):
    engine = sqlite_engine(tmp_path)
    provision(engine, SqlAlchemyOutcomeSink(engine, dataset="bars").ddl_statements())
    yield engine
    engine.dispose()


def artifacts(engine, key):
    return [
        row["payload"]
        for row in rows(
            engine,
            "SELECT payload FROM dq.artifacts WHERE run_key = :key ORDER BY seq",
            key=key,
        )
    ]


def test_a_spark_dataframe_is_read_through_its_arrow_export(spark, engine):
    """G2, on the real export: the fallback cannot read this frame at all."""
    pytest.importorskip("pyarrow")
    frame = spark.createDataFrame(
        [("a", True, 1.5), ("b", False, None)],
        "check string, success boolean, value double",
    )
    SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=True).save(
        ArrowOnly(frame), "metrics", 1
    )
    assert artifacts(engine, 1) == [
        '{"check": "a", "success": true, "value": 1.5}',
        '{"check": "b", "success": false, "value": null}',
    ]
    with pytest.raises(AttributeError, match="collect"):
        SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=False).save(
            ArrowOnly(frame), "metrics", 2
        )
    assert artifacts(engine, 2) == []


def test_the_two_ways_of_reading_a_spark_frame_store_the_same_payload(spark, engine):
    """Arrow gives a timestamp a time zone and binary data as ``bytes``; a
    frame's own rows do not. Which one read the frame must not show."""
    pytest.importorskip("pyarrow")
    frame = spark.createDataFrame(TYPED, TYPED_SCHEMA)
    SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=True).save(
        ArrowOnly(frame), "metrics", 3
    )
    SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=False).save(frame, "metrics", 4)
    through_arrow, from_rows = artifacts(engine, 3), artifacts(engine, 4)
    assert len(through_arrow) == len(from_rows) == 1
    assert through_arrow == from_rows
    # The spelling is the one the rows always had: a decimal and a date as
    # text, NaN as null, a timestamp as wall time without its offset, and
    # binary data in the kernel's string form for an object JSON cannot hold.
    assert [json.loads(payload) for payload in through_arrow] == [
        {
            "amount": "1.50",
            "at": "2026-11-25T10:00:00",
            "day": "2026-11-25",
            "value": None,
            "missing": None,
            "blob": str(bytearray(b"\x01\x02")),
            "tags": [1, 2],
            "nested": {"a": "x"},
        }
    ]


def test_an_empty_spark_frame_writes_nothing_through_arrow(spark, engine):
    """An empty frame yields no Arrow batch, so no schema to build on."""
    pytest.importorskip("pyarrow")
    frame = spark.createDataFrame([], "check string, success boolean")
    result = SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=True).save(
        ArrowOnly(frame), "metrics", 5
    )
    assert result.key == 5 and artifacts(engine, 5) == []


def test_an_engine_s_own_artifacts_reach_the_sql_sink_through_one_run(spark, engine):
    """The frames an engine builds, written by the engine's own persist path
    inside one run scope: the durable sink as a framework uses it."""
    pytest.importorskip("pyarrow")

    class NonEmpty(DQEngine):
        CAPABILITIES = {"criticality": "derived", "failed_rows": True}

        def _prepare(self, dataframe):
            return self._config.get("checks", None)

        def _execute(self, dataframe, prepared):
            from pyspark.sql import functions as F

            return [
                build_result(
                    check["name"],
                    dataframe.where(F.col(check["column"]) == "").count() == 0,
                    {"failed_rows": 0},
                    criticality=check["level"],
                )
                for check in prepared
            ]

    frame = spark.createDataFrame(
        [("ann", ""), ("", "Oslo")], "name string, city string"
    )
    engine_rule = NonEmpty(
        ConfigFactory.from_dict(
            {
                "name": "nonempty",
                "checks": [
                    {"name": "names", "column": "name", "level": "Error"},
                    {"name": "cities", "column": "city", "level": "Warning"},
                ],
            }
        )
    )
    sink = SqlAlchemyOutcomeSink(engine, dataset="bars", namespace="team_a", arrow=True)
    with sink.run_scope(99, {"dataset_sha256": "a" * 8}):
        engine_rule.apply(frame, repository=sink)

    written = rows(
        engine,
        "SELECT run_key, artifact_type, seq, payload FROM dq.artifacts "
        "ORDER BY artifact_type, seq",
    )
    assert [(row["run_key"], row["artifact_type"], row["seq"]) for row in written] == [
        (99, "metrics", 0),
        (99, "metrics", 1),
        (99, "verifications", 0),
        (99, "verifications", 1),
    ]
    assert [row["payload"] for row in written if row["artifact_type"] == "metrics"] == [
        '{"check": "names", "success": false}',
        '{"check": "cities", "success": false}',
    ]
    assert rows(
        engine, "SELECT namespace, identity FROM dq.runs WHERE run_key = :key", key=99
    ) == [{"namespace": "team_a", "identity": '{"dataset_sha256": "aaaaaaaa"}'}]
