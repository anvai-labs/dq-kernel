# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Arrow profiler and the two commands against real pyarrow (SPEC-019).

Skipped entirely when the arrow extra is absent. The Spark half of gate G1,
the driver-side gate G2 and the engines running their drafts are in
test_profile_spark.py.
"""

import datetime as dt
import decimal
import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

pytest.importorskip("pyarrow", reason="the Arrow profiler needs the arrow extra")

from dq import runner  # noqa: E402
from dq.exceptions import ConfigurationError  # noqa: E402
from dq.profile import arrow, suggest  # noqa: E402
from dq.profile.cli import profile_main, snapshot_digest, suggest_main  # noqa: E402
from dq.profile.profile import Profile  # noqa: E402
from dq.profile.suggest import TARGETS  # noqa: E402
from dq.tests.profile_support import (  # noqa: E402
    FIXTURE,
    SAID,
    corner_frames,
    instants_in,
    parquet_of,
    wide_table,
    with_a_broken_id,
)
from dq.validate import validate_config  # noqa: E402


def column_of(table, name):
    return next(
        column
        for column in arrow.profile_table(table, "t").columns
        if column.name == name
    )


# ---------------------------------------------------------------- G1, Arrow


def test_the_fixture_says_what_was_measured():
    profile = arrow.profile_table(FIXTURE, "orders")
    assert profile.rows == 8 and profile.source == "arrow"
    assert [column.name for column in profile.columns] == list(FIXTURE.column_names)
    for column in profile.columns:
        family, nulls, distinct, least, most = SAID[column.name]
        said = (
            column.type,
            column.nulls,
            column.distinct,
            column.min,
            column.max,
        )
        assert said == (family, nulls, distinct, least, most), column.name
        assert column.distinct_exact is True


def test_a_not_a_number_is_a_value_counted_and_never_a_bound():
    amount = column_of(FIXTURE, "amount")
    assert amount.not_a_number == 1
    assert (amount.min, amount.max) == (-2.0, 3.25)


def test_a_signed_zero_is_one_value():
    zeros = pa.table({"z": pa.array([0.0, -0.0, 1.0, 1.0, 2.0, 2.0])})
    assert column_of(zeros, "z").distinct == 3  # Spark says 3


def test_text_and_binary_say_how_long_and_never_which():
    profile = arrow.profile_table(FIXTURE, "orders")
    by = {column.name: column for column in profile.columns}
    assert (by["name"].min_length, by["name"].max_length) == (0, 3)
    assert by["name"].min is None and by["name"].max is None
    assert (by["blob"].min_length, by["blob"].max_length) == (0, 2)
    assert by["blob"].min is None and by["blob"].max is None


def test_a_boolean_has_no_order_and_a_nested_column_only_its_nulls():
    by = {column.name: column for column in arrow.profile_table(FIXTURE, "o").columns}
    assert by["flag"].min is None and by["flag"].max is None
    assert by["flag"].distinct == 2
    assert by["tags"].distinct is None
    assert by["tags"].min is None and by["tags"].min_length is None


def test_a_profile_s_size_depends_on_the_column_count_alone():
    def size(columns, rows):
        table = pa.table(
            {f"c{index}": pa.array([1] * rows, pa.int64()) for index in range(columns)}
        )
        return len(arrow.profile_table(table, "t").to_json())

    assert size(80, 40) > size(10, 40)
    assert size(10, 4000) == size(10, 40) + 2  # the row count's own two digits

    long_values = pa.table({"t": pa.array(["x" * 100_000, "y" * 100_000])})
    short_values = pa.table({"t": pa.array(["x", "y"])})
    said_long = len(arrow.profile_table(long_values, "t").to_json())
    said_short = len(arrow.profile_table(short_values, "t").to_json())
    # Two hundred thousand characters of values cost ten bytes: two length
    # bounds, five digits more in each.
    assert said_long == said_short + 10


def test_an_empty_table_and_an_all_null_column_have_nothing_to_bound():
    empty = arrow.profile_table(FIXTURE.slice(0, 0), "t")
    assert empty.rows == 0
    assert all(
        column.nulls == 0
        and column.distinct in (0, None)
        and column.min is None
        and column.max is None
        and column.min_length is None
        for column in empty.columns
    )
    all_null = pa.table({"x": pa.array([None, None, None], pa.int64())})
    column = column_of(all_null, "x")
    assert (column.nulls, column.distinct, column.min, column.max) == (3, 0, None, None)


#: Half past four in the afternoon, UTC, on the day of the fixture: the
#: instant every zoned column here holds.
SIXTEEN_UTC = dt.datetime(2026, 11, 25, 16, 0, tzinfo=dt.timezone.utc)


@pytest.mark.parametrize(
    ("values", "kind", "family"),
    [
        ([1, None], pa.int32(), "integer"),
        ([1, None], pa.uint8(), "integer"),
        ([1.5, None], pa.float16(), "float"),
        ([1.5, None], pa.float32(), "float"),
        ([1.5, None], pa.dictionary(pa.int32(), pa.float64()), "float"),
        (["a", None], pa.large_string(), "text"),
        ([b"ab", None], pa.binary(2), "binary"),
        ([True, None], pa.bool_(), "boolean"),
        ([dt.date(2026, 1, 1), None], pa.date32(), "date"),
        # Zoned in UTC: a zoned Python value of another zone would follow
        # the process, here (see instants_in).
        ([SIXTEEN_UTC, None], pa.timestamp("ms", tz="UTC"), "timestamp"),
        ([{"a": 1}], pa.struct([("a", pa.int64())]), "nested"),
        ([[("k", 1)]], pa.map_(pa.string(), pa.int64()), "nested"),
        ([None, None], pa.null(), "unknown"),
    ],
    ids=[
        "int32",
        "uint8",
        "half",
        "single",
        "dictionary-encoded",
        "large-string",
        "fixed-binary",
        "boolean",
        "date",
        "zoned-timestamp",
        "struct",
        "map",
        "null-type",
    ],
)
def test_types_are_named_in_the_profile_s_words_whatever_arrow_spells_them(
    values, kind, family
):
    column = column_of(pa.table({"x": pa.array(values, kind)}), "x")
    assert column.type == family


def test_a_string_view_is_text_where_arrow_has_it():
    if not hasattr(pa, "string_view"):
        pytest.skip("this pyarrow has no string_view")
    import pyarrow.compute as pc

    encoded = pc.cast(pa.array(["a", None]), pa.string_view())
    column = column_of(pa.table({"x": encoded}), "x")
    assert column.type == "text"
    assert (column.nulls, column.distinct, column.min_length, column.max_length) == (
        1,
        1,
        1,
        1,
    )


def test_a_timestamp_with_a_zone_is_spelled_in_utc():
    zoned = pa.table({"x": instants_in([SIXTEEN_UTC, None], "America/Chicago")})
    column = column_of(zoned, "x")
    assert (column.min, column.max) == ("2026-11-25T16:00:00",) * 2


def test_a_zoned_timestamp_is_the_same_moment_whatever_the_process_s_switch(
    monkeypatch,
):
    """Importing pandera, or pyspark.pandas, sets PYARROW_IGNORE_TIMEZONE for
    the process. The profile is the same under it: the profiler asks for
    the moment itself, and never for a zone's spelling of it."""
    zoned = pa.table({"x": instants_in([SIXTEEN_UTC, None], "America/Chicago")})
    spelled = arrow.profile_table(zoned, "t")
    monkeypatch.setenv("PYARROW_IGNORE_TIMEZONE", "1")
    assert arrow.profile_table(zoned, "t") == spelled

    # The control: the switch is read where a zoned Python value comes in
    # -- its wall time is taken as if it were UTC -- which is why the
    # fixtures spell their instants from UTC (`instants_in`). Half past four
    # in the afternoon, UTC, is ten in the morning in Chicago: said so, it
    # comes in six hours early.
    in_chicago = pa.array(
        [dt.datetime(2026, 11, 25, 10, 0, tzinfo=dt.timezone(dt.timedelta(hours=-6)))],
        pa.timestamp("us", tz="America/Chicago"),
    )
    said_from_utc = instants_in([SIXTEEN_UTC], "America/Chicago")
    six_hours = 6 * 60 * 60 * 10**6
    assert (
        said_from_utc.cast(pa.int64())[0].as_py()
        - in_chicago.cast(pa.int64())[0].as_py()
        == six_hours
    )


def test_a_decimal_keeps_its_precision_and_scale():
    column = column_of(FIXTURE, "price")
    assert (column.precision, column.scale) == (10, 2)
    assert (column.min, column.max) == ("-0.05", "1.50")


def test_an_infinite_bound_is_spelled_as_strict_json_spells_it():
    endless = pa.table({"x": pa.array([float("inf"), None, float("-inf")])})
    column = column_of(endless, "x")
    assert (column.min, column.max) == ("-Infinity", "Infinity")
    assert Profile.from_json(arrow.profile_table(endless, "t").to_json()).columns == (
        column,
    )


def test_a_type_the_profile_does_not_know_is_asked_only_for_its_nulls():
    """No column fails a profile: a type with no family has no measure to
    ask for, on either profiler."""
    for values, kind in (
        ([dt.timedelta(days=1), None, dt.timedelta(days=1)], pa.duration("us")),
        ([dt.time(1), None, dt.time(1)], pa.time32("s")),
        ([None, None, None], pa.null()),
    ):
        column = column_of(pa.table({"x": pa.array(values, kind)}), "x")
        assert column.type == "unknown"
        assert (column.nulls, column.distinct) == (values.count(None), None)
        assert column.min is None and column.max is None and column.min_length is None


def test_a_decimal_is_spelled_in_plain_notation_to_its_own_scale():
    """Python writes a decimal of a negative scale as ``-5E+2``: a profile
    spells the number, minus five hundred."""
    hundreds = pa.table(
        {
            "x": pa.array(
                [decimal.Decimal("1200"), decimal.Decimal("-500")],
                pa.decimal128(10, -2),
            )
        }
    )
    profile = arrow.profile_table(hundreds, "t")
    (column,) = profile.columns
    assert (column.precision, column.scale) == (10, -2)
    assert (column.min, column.max) == ("-500", "1200")
    assert Profile.from_json(profile.to_json()) == profile
    wide = pa.table(
        {"x": pa.array([decimal.Decimal("1.500"), None], pa.decimal256(40, 3))}
    )
    (column,) = arrow.profile_table(wide, "t").columns
    assert (column.precision, column.scale, column.min, column.max) == (
        40,
        3,
        "1.500",
        "1.500",
    )


def test_something_that_is_not_a_table_is_refused():
    with pytest.raises(ConfigurationError, match="takes a pyarrow Table"):
        arrow.profile_table([[1, 2]], "t")


def test_a_profile_whose_every_float_is_a_not_a_number_has_no_bound():
    column = column_of(pa.table({"x": pa.array([float("nan"), None])}), "x")
    assert column.min is None and column.max is None and column.not_a_number == 1


def test_what_arrow_cannot_measure_is_refused_in_the_profiler_s_words(monkeypatch):
    import pyarrow.compute as pc

    def no_kernel(*arguments, **options):
        raise pa.ArrowException("No kernel installed for this type")

    monkeypatch.setattr(pc, "count_distinct", no_kernel)
    with pytest.raises(ConfigurationError) as refused:
        arrow.profile_table(FIXTURE, "orders")
    assert str(refused.value) == (
        "column 'id', of type integer, cannot be counted in Arrow: "
        "No kernel installed for this type"
    )
    monkeypatch.undo()
    monkeypatch.setattr(pc, "min_max", no_kernel)
    with pytest.raises(
        ConfigurationError, match="column 'id', of type integer, cannot be bounded"
    ):
        arrow.profile_table(FIXTURE, "orders")


def test_the_arrow_profiler_names_the_extra_when_it_is_missing():
    code = """
import sys

sys.modules["pyarrow"] = None
from dq.profile import arrow

try:
    arrow.profile_table(None, "t")
except ImportError as error:
    assert "-E arrow" in str(error), error
    print("named")
"""
    done = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        env={
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("COV_CORE_", "COVERAGE_"))
        },
        cwd=Path(__file__).resolve().parents[2],
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "named"


# ------------------------------------------------------------- G5: they compose


def profile_the_fixture(directory) -> tuple:
    """Run dq-profile over the fixture; return ``(its path, the profile's path)``."""
    data = parquet_of(FIXTURE, directory)
    written = directory / "orders.profile.json"
    code = profile_main(["--dataset", f"orders={data}", "--profile", str(written)])
    assert code == 0
    return data, written


@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("suffix", [".conf", ".yaml", ".json"])
def test_dq_profile_and_dq_suggest_compose_into_a_draft_that_validates(
    tmp_path, target, suffix
):
    data, profile_path = profile_the_fixture(tmp_path)
    profile = Profile.from_json(profile_path.read_text())
    assert profile == arrow.profile_table(FIXTURE, "orders")
    assert profile.snapshot_sha256 == snapshot_digest(data)

    draft = tmp_path / f"draft.{suffix}"
    code = suggest_main([str(profile_path), "--engine", target, "--output", str(draft)])
    assert code == 0, (target, suffix)
    assert validate_config(str(draft)), (target, suffix)


def test_the_portable_draft_passes_under_dq_run_on_the_data_it_was_drawn_from(tmp_path):
    data, profile_path = profile_the_fixture(tmp_path)
    draft = tmp_path / "draft.yaml"
    assert (
        suggest_main(
            [str(profile_path), "--engine", "portable", "--output", str(draft)]
        )
        == 0
    )

    report = tmp_path / "report.json"
    result = runner.run(str(draft), {"orders": data}, report=str(report))
    assert result.decision.passed
    document = json.loads(report.read_text())
    assert document["status"] == "passed"
    assert document["dataset"]["sha256"] == snapshot_digest(data)
    assert all(check["success"] for check in document["checks"])


@pytest.mark.parametrize("rows", sorted(corner_frames()))
def test_the_portable_draft_of_every_corner_passes_under_dq_run(tmp_path, rows):
    """Every corner of dq.tests.profile_support.SMALL_TABLES: a draft drawn
    from the data holds on the data."""
    table = corner_frames()[rows]
    path = parquet_of(table, tmp_path)
    pack = suggest.suggest(
        arrow.profile_table(table, "corners"),
        "portable",
        snapshot_sha256=snapshot_digest(path),
    )
    rules = pack["dqframework"]["dqrules"]
    assert bool(rules) == (rows > 0)
    if not rules:
        return
    draft = tmp_path / "draft.json"
    draft.write_text(suggest.render(pack, "json"))

    result = runner.run(
        str(draft), {"corners": path}, report=str(tmp_path / "report.json")
    )

    said = {outcome["check"]: outcome["success"] for outcome in result.outcomes}
    assert all(said.values()), [check for check, holds in said.items() if not holds]
    # What the portable subset cannot say of a not-a-number is not said.
    if "float_all_not_a_number" in table.column_names:
        assert "float_all_not_a_number_complete" not in said
        assert "float_all_not_a_number" not in " ".join(
            said
        )  # nothing below zero to say either


def test_dq_profile_reads_a_directory_and_digests_its_files(tmp_path):
    import pyarrow.parquet as pq

    data = tmp_path / "dataset"
    pq.write_to_dataset(FIXTURE, str(data))
    written = tmp_path / "orders.profile.json"
    code = profile_main(["--dataset", f"orders={data}", "--profile", str(written)])
    assert code == 0
    profile = Profile.from_json(written.read_text())
    assert profile.rows == 8
    assert profile.snapshot_sha256 == snapshot_digest(data)
    assert {column.name for column in profile.columns} == set(FIXTURE.column_names)


def test_a_wide_table_is_profiled_whole(tmp_path):
    data = parquet_of(wide_table(columns=80, rows=4000), tmp_path)
    written = tmp_path / "wide.profile.json"
    assert profile_main(["--dataset", f"wide={data}", "--profile", str(written)]) == 0
    profile = Profile.from_json(written.read_text())
    assert profile.rows == 4000 and len(profile.columns) == 80
    assert all(column.max == 3999 for column in profile.columns)


def test_dq_profile_refuses_a_file_pyarrow_cannot_read(tmp_path):
    data = tmp_path / "orders.parquet"
    data.write_bytes(b"not parquet")
    assert profile_main(["--dataset", f"orders={data}"]) == 2


def test_a_portable_draft_s_failed_warning_leaves_the_gate_open(tmp_path, capsys):
    """The draft over data that breaks it: the failure is written down, and
    dq-run exits 0 -- as the default gate policy, "any", would not have."""
    _, profile_path = profile_the_fixture(tmp_path)
    draft = tmp_path / "draft.yaml"
    assert (
        suggest_main(
            [str(profile_path), "--engine", "portable", "--output", str(draft)]
        )
        == 0
    )
    broken = parquet_of(with_a_broken_id(FIXTURE), tmp_path / "broken")
    report = tmp_path / "report.json"

    code = runner.main(
        [str(draft), "--dataset", f"orders={broken}", "--report", str(report)]
    )

    assert code == 0
    document = json.loads(report.read_text())
    assert [(check["check"], check["success"]) for check in document["checks"]] == [
        ("id_complete", False),
        ("id_non_negative.low", True),
    ]
    assert document["status"] == "failed"  # the facts, whatever the policy
    assert document["summary"]["blocked"] == 0
    assert document["summary"]["warned"] == 1
    said = capsys.readouterr().out
    assert "Under the 'error' gate policy: 0 blocking, 1 advisory" in said
    assert "Advisory failures (not blocking): id_complete" in said
    assert "The gate passed." in said


def test_a_table_that_names_a_column_twice_is_refused():
    twice = pa.Table.from_arrays(
        [*FIXTURE.columns, FIXTURE.column(0)],
        schema=pa.schema([*FIXTURE.schema, FIXTURE.schema.field(0)]),
    )
    assert twice.column_names.count("id") == 2  # Arrow allows it
    with pytest.raises(ConfigurationError, match=r"names a column twice: \['id'\]"):
        arrow.profile_table(twice, "orders")


def test_nothing_warranted_is_a_command_that_says_so(tmp_path, caplog, capsys):
    caplog.set_level("INFO")
    data = parquet_of(pa.table({"x": pa.array([None, None], pa.int64())}), tmp_path)
    written = tmp_path / "nulls.profile.json"
    assert profile_main(["--dataset", f"orders={data}", "--profile", str(written)]) == 0
    assert suggest_main([str(written), "--engine", "deequ"]) == 0
    assert "warrants no rule" in caplog.text
    assert capsys.readouterr().out == ""
