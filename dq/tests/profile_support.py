# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""One dataset before both profilers, and the drafts read back (SPEC-019)."""

import decimal
import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pyhocon import ConfigFactory

from dq.utils.config_utils import config_tree_to_python, load_config

#: Ten in the morning, UTC, on the day of the fixture -- the instant both
#: zoned columns hold -- and a null beside it.
ZONED_VALUES = [dt.datetime(2026, 11, 25, 10, tzinfo=dt.timezone.utc), None] * 4


def instants_in(instants, zone) -> pa.Array:
    """Return timestamp values for ``instants`` (each aware, in UTC, or null),
    spelled in ``zone``.

    A zoned array is not built from a zoned Python value of another zone:
    where ``PYARROW_IGNORE_TIMEZONE`` is set in the process -- importing
    pandera, or pyspark.pandas, sets it -- pyarrow reads a zoned value's
    wall time as if it were UTC. An array cast from UTC keeps the instant.
    """
    utc = pa.array(instants, pa.timestamp("us", tz="UTC"))
    return utc.cast(pa.timestamp("us", tz=zone))


#: Eight rows, one column of every family a profile meets: an integer, a
#: float with a null, a not-a-number, a negative, a signed zero and its
#: positive, a decimal, text with a null and an empty string, a boolean, a
#: date, a timestamp without a zone, one with (in UTC, and in Chicago),
#: binary, and a nested column.
FIXTURE = pa.table(
    {
        "id": pa.array([1, 2, 3, 4, 5, 6, 7, 8], pa.int64()),
        "amount": pa.array(
            [1.5, None, float("nan"), -2.0, 0.0, -0.0, 3.25, 1.5], pa.float64()
        ),
        "price": pa.array(
            [
                decimal.Decimal("1.50"),
                None,
                decimal.Decimal("-0.05"),
                decimal.Decimal("1.50"),
            ]
            * 2,
            pa.decimal128(10, 2),
        ),
        "name": pa.array(["ann", None, "", "bob", "cy", "di", "ann", None]),
        "flag": pa.array([True, None, False, True, True, False, None, True]),
        "day": pa.array(
            [dt.date(2026, 11, 25), None, dt.date(2026, 1, 1), dt.date(2026, 11, 25)]
            * 2
        ),
        "at": pa.array(
            [
                dt.datetime(2026, 11, 25, 10),
                None,
                dt.datetime(2026, 1, 1),
                dt.datetime(2026, 11, 25, 10),
            ]
            * 2,
            pa.timestamp("us"),
        ),
        "zoned": pa.array(
            [dt.datetime(2026, 11, 25, 10, tzinfo=dt.timezone.utc), None] * 4,
            pa.timestamp("us", tz="UTC"),
        ),
        # The same instant as `zoned`, in a zone that is not UTC.
        "chicago": instants_in(ZONED_VALUES, "America/Chicago"),
        "blob": pa.array(
            [b"\x01", None, b"", b"\x01\x02", b"\x01", b"", None, b"\x01"]
        ),
        "tags": pa.array(
            [["a"], None, [], ["a", "b"], ["a"], [], None, ["a"]], pa.list_(pa.string())
        ),
    }
)

#: What the fixture's profile says, column by column: the numbers both
#: engines were measured to agree on once the profile's rules are applied.
SAID = {
    "id": ("integer", 0, 8, 1, 8),
    "amount": ("float", 1, 5, -2.0, 3.25),
    "price": ("decimal", 2, 2, "-0.05", "1.50"),
    "name": ("text", 2, 5, None, None),
    "flag": ("boolean", 2, 2, None, None),
    "day": ("date", 2, 2, "2026-01-01", "2026-11-25"),
    "at": ("timestamp", 2, 2, "2026-01-01T00:00:00", "2026-11-25T10:00:00"),
    # One instant four times over: one distinct value.
    "zoned": ("timestamp", 4, 1, "2026-11-25T10:00:00", "2026-11-25T10:00:00"),
    # The same instant as `zoned`, written in a zone that is not UTC.
    "chicago": ("timestamp", 4, 1, "2026-11-25T10:00:00", "2026-11-25T10:00:00"),
    "blob": ("binary", 2, 3, None, None),
    "tags": ("nested", 2, None, None, None),
}


#: One column, a few rows, each table a corner a suggestion has to survive:
#: a draft drawn from one of these must pass on it, in every vocabulary.
#: Labelled by what the corner is.
NOT_A_NUMBER = float("nan")
SMALL_TABLES = {
    "int": pa.table({"c": pa.array([3, 1, 2], pa.int64())}),
    "int-negative": pa.table({"c": pa.array([-1, 2], pa.int64())}),
    "int-repeated": pa.table({"c": pa.array([1, 1], pa.int64())}),
    "int-with-null": pa.table({"c": pa.array([1, None], pa.int64())}),
    "all-null": pa.table({"c": pa.array([None, None], pa.int64())}),
    "no-row": pa.table({"c": pa.array([], pa.int64())}),
    "single-row": pa.table({"c": pa.array([7], pa.int64())}),
    "float-not-a-number": pa.table({"c": pa.array([1.0, 2.0, NOT_A_NUMBER, 0.5])}),
    "float-two-not-a-number": pa.table(
        {"c": pa.array([1.0, NOT_A_NUMBER, NOT_A_NUMBER])}
    ),
    "float-all-not-a-number": pa.table({"c": pa.array([NOT_A_NUMBER, NOT_A_NUMBER])}),
    "float-signed-zeros": pa.table({"c": pa.array([0.0, -0.0])}),
    "float-infinite": pa.table({"c": pa.array([float("inf"), 1.0, 0.0])}),
    "float-minus-infinite": pa.table({"c": pa.array([float("-inf"), 1.0])}),
    "float-negative": pa.table({"c": pa.array([-1.0, 2.0])}),
    "float-repeated": pa.table({"c": pa.array([1.5, 1.5, 0.0])}),
    "single-precision": pa.table({"c": pa.array([1.5, 2.5], pa.float32())}),
    "decimal": pa.table(
        {
            "c": pa.array(
                [decimal.Decimal("0.00"), decimal.Decimal("1.50")], pa.decimal128(10, 2)
            )
        }
    ),
    "decimal-negative": pa.table(
        {
            "c": pa.array(
                [decimal.Decimal("-0.05"), decimal.Decimal("1.50")],
                pa.decimal128(10, 2),
            )
        }
    ),
    "text": pa.table({"c": pa.array(["", "a", "ab"])}),
    "text-repeated": pa.table({"c": pa.array(["a", "a"])}),
    "boolean": pa.table({"c": pa.array([True, False, True])}),
    "date": pa.table({"c": pa.array([dt.date(2026, 1, 2), dt.date(2026, 1, 1)])}),
    "timestamp": pa.table(
        {
            "c": pa.array(
                [dt.datetime(2026, 1, 2, 3), dt.datetime(2026, 1, 1)],
                pa.timestamp("us"),
            )
        }
    ),
    # One instant, twice over, in a zone.
    "timestamp-zoned": pa.table(
        {
            "c": pa.array(
                [dt.datetime(2026, 1, 2, 10, tzinfo=dt.timezone.utc)] * 2,
                pa.timestamp("us", tz="UTC"),
            )
        }
    ),
    "binary": pa.table({"c": pa.array([b"", b"\x00\x01"])}),
    "array": pa.table({"c": pa.array([[1], [1, 2], []])}),
    "struct": pa.table({"c": pa.array([{"a": 1}, {"a": 2}])}),
    "map": pa.table(
        {"c": pa.array([[("k", 1)], [("k", 2)]], pa.map_(pa.string(), pa.int64()))}
    ),
    # Spellings of a family that both engines read from one Parquet file
    # alike: measured, the two profilers agree on each of these.
    "unsigned-byte": pa.table({"c": pa.array([0, 255, 7], pa.uint8())}),
    "tiny-integer": pa.table({"c": pa.array([-128, 0, 127], pa.int8())}),
    "widest-integer": pa.table({"c": pa.array([-(2**63), 0, 2**63 - 1], pa.int64())}),
    "wide-decimal": pa.table(
        {
            "c": pa.array(
                [decimal.Decimal("1.5"), decimal.Decimal("-2.5"), None],
                pa.decimal128(38, 1),
            )
        }
    ),
    "large-text": pa.table({"c": pa.array(["", "é", None], pa.large_string())}),
    "dictionary-encoded": pa.table(
        {"c": pa.array(["a", None, "a", "b"]).dictionary_encode()}
    ),
    "unicode-text": pa.table({"c": pa.array(["日本", "🙂", ""])}),
    "fixed-binary": pa.table(
        {"c": pa.array([b"\x00\x00", b"\xff\x01", None], pa.binary(2))}
    ),
    "millisecond-timestamp": pa.table(
        {
            "c": pa.array(
                [dt.datetime(2026, 1, 1, 12, 30, 1, 500000), None], pa.timestamp("ms")
            )
        }
    ),
    "list-of-struct": pa.table({"c": pa.array([[{"a": 1}], [], None])}),
    "map-of-lists": pa.table(
        {
            "c": pa.array(
                [[("k", [1, 2])], None, []], pa.map_(pa.string(), pa.list_(pa.int64()))
            )
        }
    ),
    "all-null-text": pa.table({"c": pa.array([None, None], pa.string())}),
}


def corner_frames() -> dict:
    """Return SMALL_TABLES laid side by side, one table per row count.

    A check costs a job whatever frame it runs on, so the corners share
    frames: each column is one of SMALL_TABLES, named for it, among rows of
    its own count. Keyed by the row count.
    """
    groups = {}
    for label, table in SMALL_TABLES.items():
        groups.setdefault(table.num_rows, []).append(
            (label.replace("-", "_"), table.column("c"))
        )
    return {rows: pa.table(dict(columns)) for rows, columns in groups.items()}


def parquet_of(table, directory) -> Path:
    """Write ``table`` as Parquet under ``directory``; return its path."""
    path = Path(directory) / "fixture.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def with_a_broken_id(table):
    """Return ``table`` with its id column broken, as a draft would find it:
    a null, and a value repeated."""
    ids = table.column("id").to_pylist()
    ids[0], ids[1] = None, ids[2]
    field = table.schema.field("id")
    return table.set_column(
        table.column_names.index("id"), field, pa.array(ids, field.type)
    )


def wide_table(columns: int = 60, rows: int = 40):
    """Return a table of ``columns`` integer columns, the first a sentinel."""
    first = list(range(rows))
    return pa.table(
        {"sentinel": pa.array(first, pa.int64())}
        | {f"c{index}": pa.array(first, pa.int64()) for index in range(1, columns)}
    )


def draft_pack(path) -> dict:
    """Return the rule pack a written draft holds, read by the package's loader."""
    return config_tree_to_python(load_config(str(path)))


def as_config_tree(pack: dict):
    """Return a rule pack the engines take, as a ``ConfigTree``."""
    return ConfigFactory.from_dict(pack)


def plain(document) -> dict:
    """Return a YAML-loaded view of a profile document, for comparison."""
    return yaml.safe_load(document)
