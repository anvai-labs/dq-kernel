# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Reading an artifact's rows through Arrow (SPEC-007).

pyarrow is optional. When it is installed, a sink reads a pyarrow table, or
anything that exports the Arrow stream interface, through Arrow; when it is
not, or when the frame is not one Arrow can read, the sink reads the frame's
own rows as it always did. Either way the row that reaches the database is
the same.
"""

import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy", reason="the durable sink needs the sqlalchemy extra")

from dq import sqlalchemy_sink as sink_module
from dq.sqlalchemy_sink import (
    SqlAlchemyOutcomeSink,
    _arrow_table,
    _spelled_like_frame_rows,
)
from dq.tests.helpers import FakeDataFrame
from dq.tests.sink_support import provision, rows, sqlite_engine

TABLE_ROWS = [{"check": "a", "success": True}, {"check": "b", "success": False}]
TABLE_PAYLOADS = ['{"check": "a", "success": true}', '{"check": "b", "success": false}']


@pytest.fixture
def engine(tmp_path):
    engine = sqlite_engine(tmp_path)
    provision(engine, SqlAlchemyOutcomeSink(engine, dataset="bars").ddl_statements())
    yield engine
    engine.dispose()


def stored(engine, key):
    return [
        row["payload"]
        for row in rows(
            engine,
            "SELECT payload FROM dq.artifacts WHERE run_key = :key ORDER BY seq",
            key=key,
        )
    ]


def test_a_pyarrow_table_is_read_as_it_is(engine):
    pa = pytest.importorskip("pyarrow")
    SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=True).save(
        pa.table({"check": ["a", "b"], "success": [True, False]}), "verifications", 1
    )
    # The same artifact by the fallback: the frame's own rows.
    SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=False).save(
        FakeDataFrame(TABLE_ROWS), "verifications", 2
    )
    assert stored(engine, 1) == stored(engine, 2) == TABLE_PAYLOADS


def test_the_arrow_exports_are_found_by_what_the_frame_offers(engine):
    """Each is a method on the object: Spark Connect and PySpark 4 offer
    ``toArrow``, classic PySpark 3.5 only ``_collect_as_arrow``, and Polars,
    pandas, DuckDB and DataFusion the stream interface. None is imported."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"check": ["a", "b"], "success": [True, False]})
    exporting = SimpleNamespace(toArrow=lambda: table)
    batches_only = SimpleNamespace(
        _collect_as_arrow=lambda: table.to_batches(),
        collect=lambda: (_ for _ in ()).throw(AssertionError("fell back")),
    )
    streaming = SimpleNamespace(__arrow_c_stream__=table.__arrow_c_stream__)
    for frame in (exporting, batches_only, streaming):
        assert _arrow_table(pa, frame).to_pylist() == TABLE_ROWS
    # A frame that offers no export is left to its own rows.
    assert _arrow_table(pa, object()) is None
    # Classic PySpark yields no batch for an empty frame, so there is no schema.
    empty = SimpleNamespace(_collect_as_arrow=lambda: [])
    assert _arrow_table(pa, empty).to_pylist() == []


def test_an_arrow_value_is_spelled_as_a_frame_s_own_rows_spell_it():
    """Spark's Arrow export gives a timestamp its time zone and binary data as
    ``bytes``; its rows give the wall time and a ``bytearray``."""
    zone = timezone(timedelta(hours=-6))
    read_through_arrow = {
        "at": datetime(2026, 11, 25, 10, tzinfo=zone),
        "naive": datetime(2026, 11, 25, 10),
        "blob": b"\x01",
        "tags": [datetime(2026, 11, 25, 10, tzinfo=zone), b"\x02", 1],
        "nested": {"at": datetime(2026, 11, 25, 10, tzinfo=zone)},
        "amount": Decimal("1.50"),
        "day": date(2026, 11, 25),
    }
    assert _spelled_like_frame_rows(read_through_arrow) == {
        "at": datetime(2026, 11, 25, 10),
        "naive": datetime(2026, 11, 25, 10),
        "blob": bytearray(b"\x01"),
        "tags": [datetime(2026, 11, 25, 10), bytearray(b"\x02"), 1],
        "nested": {"at": datetime(2026, 11, 25, 10)},
        "amount": Decimal("1.50"),
        "day": date(2026, 11, 25),
    }


def test_a_frame_arrow_cannot_read_falls_back_to_its_own_rows(engine):
    pytest.importorskip("pyarrow")
    SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=True).save(
        FakeDataFrame(TABLE_ROWS), "verifications", 3
    )
    assert stored(engine, 3) == TABLE_PAYLOADS


def test_without_pyarrow_the_frame_s_own_rows_are_read(engine, monkeypatch):
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=True).save(
        FakeDataFrame(TABLE_ROWS), "verifications", 4
    )
    assert stored(engine, 4) == TABLE_PAYLOADS


def test_arrow_switched_off_is_not_even_looked_for(engine, monkeypatch):
    def never():
        raise AssertionError("pyarrow was consulted although arrow is off")

    monkeypatch.setattr(sink_module, "_pyarrow", never)
    SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=False).save(
        FakeDataFrame(TABLE_ROWS), "verifications", 5
    )
    assert stored(engine, 5) == TABLE_PAYLOADS


def test_values_arrow_hands_back_are_stored_as_strict_json(engine):
    """Arrow gives back a Decimal, a datetime and a NaN as a float: a JSON
    document can hold none of them as they are."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "amount": pa.array([Decimal("1.50")], pa.decimal128(10, 2)),
            "at": pa.array(
                [datetime(2026, 11, 25, 10, tzinfo=timezone(timedelta(hours=-6)))],
                pa.timestamp("us", tz="America/Chicago"),
            ),
            "value": pa.array([float("nan")], pa.float64()),
        }
    )
    SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=True).save(table, "metrics", 6)
    assert stored(engine, 6) == [
        '{"amount": "1.50", "at": "2026-11-25T10:00:00", "value": null}'
    ]
