# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Arrow adapter against the shared semantic corpus (SPEC-015, G1).

The same two layers the Spark adapter is checked on, so a divergence is
located precisely: the metrics the adapter computes (``check_metrics``) and
the decisions it reaches (``check_case``), the fail-closed cases included.
A table reaches the adapter in more than one shape -- as one chunk, as
several, with its columns dictionary-encoded the way a Parquet reader returns
them -- so the metrics are checked in each.
"""

import math

import pytest

from dq import arrow_adapter
from dq.arrow_adapter import execute_plan, validated_bindings
from dq.exceptions import ConfigurationError
from dq.sdk.fixtures import (
    DATASET_NAME,
    build_plan,
    check_case,
    check_metrics,
    load_cases,
)
from dq.tests.fixture_frames import arrow_table

pytest.importorskip("pyarrow", reason="the Arrow adapter needs the arrow extra")

EVERY_CASE = load_cases()
DECIDABLE = [case for case in EVERY_CASE if not case.expect_error]
FAIL_CLOSED = [case for case in EVERY_CASE if case.expect_error]

#: How a corpus case is laid out in memory. ``encoded="all"`` dictionary-
#: encodes every column.
SHAPES = {
    "one-chunk": {},
    "three-chunks": {"chunks": 3},
    "encoded": {"encoded": "all"},
}

#: What a fail-closed case makes each adapter say, to the word.
REFUSALS = {
    "ranges-infinite-bound-fails-closed": (
        "column 'v' in dataset 'ds' has an infinite bound; ranges/v1 bounds "
        "must be finite"
    ),
    "ranges-text-bounds-fail-closed": (
        "column 's' in dataset 'ds' produced a bound of an unsupported type: str"
    ),
}


def laid_out(case, shape):
    """Return a corpus case as an Arrow table in the named shape."""
    options = dict(SHAPES[shape])
    if options.get("encoded") == "all":
        options["encoded"] = case.column_names
    return arrow_table(case, **options)


def compute(plan, frames):
    return arrow_adapter.compute_plan_values(plan, validated_bindings(plan, frames))


# ------------------------------------------------------------- the shapes


def same_rows(table, other):
    """Whether two tables hold the same cells, a NaN counting as a NaN."""
    left, right = table.to_pylist(), other.to_pylist()
    return len(left) == len(right) and all(
        a == b or (isinstance(a, float) and math.isnan(a) and math.isnan(b))
        for row_left, row_right in zip(left, right)
        for a, b in zip(row_left.values(), row_right.values())
    )


def zero_signs(table, column):
    """The signs of the zeros a column holds, sorted: ``[-1.0, 1.0]`` is both."""
    return sorted(
        math.copysign(1, value)
        for row in table.select([column]).to_pylist()
        for value in row.values()
        if value == 0
    )


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_the_builder_lays_a_case_out_as_named(shape):
    """Each shape must be what it says, or a test in that shape is hollow."""
    pa = pytest.importorskip("pyarrow")
    case = next(c for c in DECIDABLE if len(c.rows) >= 5)
    table = laid_out(case, shape)
    assert same_rows(table, arrow_table(case)), "the values changed"
    if shape == "one-chunk":
        assert all(table.column(name).num_chunks == 1 for name in case.column_names)
    elif shape == "three-chunks":
        assert all(1 < table.column(name).num_chunks <= 3 for name in case.column_names)
    else:
        assert all(
            pa.types.is_dictionary(table.column(name).type)
            for name in case.column_names
        )


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_every_shape_keeps_a_signed_zero_signed(shape):
    """The signed-zero cases only test anything if both zeros still arrive:
    a shape that merged them would pass those cases by accident."""
    case = next(c for c in DECIDABLE if c.id == "groups-signed-zeros-are-one-value")
    table = laid_out(case, shape)
    # east holds 0.0 and -0.0, and so does west.
    assert zero_signs(table, "v") == [-1.0, -1.0, 1.0, 1.0]


# ------------------------------------------------------------- metrics


@pytest.mark.parametrize("shape", sorted(SHAPES))
@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_arrow_computes_the_documented_metrics(case, shape):
    frames = {DATASET_NAME: laid_out(case, shape)}
    assert check_metrics(case, compute, frames) == []


# ------------------------------------------------------------ decisions


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_arrow_decides_every_case_as_the_corpus_documents(case):
    frames = {DATASET_NAME: laid_out(case, "three-chunks")}
    assert check_case(case, execute_plan, frames) == []
    assert build_plan(case).fingerprint  # a plan always carries its identity


@pytest.mark.parametrize("case", FAIL_CLOSED, ids=lambda case: case.id)
def test_arrow_fails_closed_where_the_corpus_documents_an_error(case):
    frames = {DATASET_NAME: laid_out(case, "encoded")}
    assert check_case(case, execute_plan, frames) == []
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(build_plan(case), frames)
    assert str(raised.value) == REFUSALS[case.id]


def test_every_fail_closed_case_has_its_refusal_pinned():
    """A new fail-closed case must be pinned here, not only matched loosely."""
    assert {case.id for case in FAIL_CLOSED} == set(REFUSALS)


# ------------------------------------------------ what the normalising is for


def test_the_signed_zero_cases_fail_unless_a_float_is_normalised(monkeypatch):
    """The corpus holds these cases because Arrow would otherwise fail them:
    it counts 0.0 and -0.0 apart, where the corpus says one value."""
    pa = pytest.importorskip("pyarrow")
    case = next(c for c in DECIDABLE if c.id == "groups-signed-zeros-are-one-value")
    table = arrow_table(case)
    zeros = pa.array([0.0, -0.0, 1.0, 1.0, 2.0, 2.0])
    assert (
        pa.compute.count_distinct(zeros).as_py() == 4
    ), "Arrow no longer splits signed zeros"
    assert pa.compute.count_distinct(pa.compute.add(zeros, 0.0)).as_py() == 3

    monkeypatch.setattr(arrow_adapter, "canonical", lambda column: column)
    assert check_metrics(
        case, compute, {DATASET_NAME: table}
    ), "the case passed with signed zeros left apart, so it no longer tests anything"


def test_an_encoded_column_is_counted_wrong_unless_it_is_decoded(monkeypatch):
    """A Parquet reader returns a dictionary-encoded column when the file
    stored it that way. Its type then says ``dictionary``, not ``double``:
    until it is decoded nothing can tell that a NaN is in it, and a present
    count would count the NaN as present -- with no error."""
    case = next(
        c for c in DECIDABLE if c.id == "counts-nan-only-and-infinity-only-columns"
    )
    table = laid_out(case, "encoded")
    assert check_metrics(case, compute, {DATASET_NAME: table}) == []

    monkeypatch.setattr(arrow_adapter, "canonical", lambda column: column)
    problems = check_metrics(case, compute, {DATASET_NAME: table})
    assert problems == [f"{case.id}: present_count:a expected 0, got 2"]


def test_an_encoded_column_has_no_bound_unless_it_is_decoded(monkeypatch):
    case = next(c for c in DECIDABLE if c.id == "ranges-integer-bounds")
    table = laid_out(case, "encoded")
    assert check_metrics(case, compute, {DATASET_NAME: table}) == []

    monkeypatch.setattr(arrow_adapter, "canonical", lambda column: column)
    with pytest.raises(
        ConfigurationError, match="has no column_min in Arrow"
    ) as raised:
        compute(build_plan(case), {DATASET_NAME: table})
    assert "column 'amount' in dataset 'ds'" in str(raised.value)


def holds_view_columns(pa):
    """Whether this pyarrow can hold a ``string_view`` column: 16 has the
    name but not a cast to it, so a test of view columns cannot be built."""
    if not hasattr(pa, "string_view"):
        return False
    try:
        pa.array(["a"]).cast(pa.string_view())
    except pa.ArrowException:
        return False
    return True


def viewed(table, pa):
    """The same table with every string column as ``string_view``."""
    return pa.table(
        {
            name: (
                column.cast(pa.string_view())
                if pa.types.is_string(column.type)
                else column
            )
            for name in case_names(table)
            for column in [table.column(name)]
        }
    )


def case_names(table):
    return table.schema.names


GROUP_CASES = [case for case in DECIDABLE if case.semantics == "groups/v1"]


@pytest.mark.parametrize("case", GROUP_CASES, ids=lambda case: case.id)
def test_a_view_column_is_measured_as_the_same_column_as_a_string(case):
    """Polars hands its strings over as ``string_view``. They must measure
    exactly as they do as ``string``."""
    pa = pytest.importorskip("pyarrow")
    if not holds_view_columns(pa):
        pytest.skip("this pyarrow cannot hold a view column")
    plain = arrow_table(case)
    plan = build_plan(case)
    assert compute(plan, {DATASET_NAME: viewed(plain, pa)}) == compute(
        plan, {DATASET_NAME: plain}
    )


def test_a_view_column_fails_closed_as_a_string_does(monkeypatch):
    """A text column has no bound. One that arrives as ``string_view`` must
    say so in the same words, not in Arrow's."""
    pa = pytest.importorskip("pyarrow")
    if not holds_view_columns(pa):
        pytest.skip("this pyarrow cannot hold a view column")
    case = next(c for c in FAIL_CLOSED if c.id == "ranges-text-bounds-fail-closed")
    frames = {DATASET_NAME: viewed(arrow_table(case), pa)}
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(build_plan(case), frames)
    assert str(raised.value) == REFUSALS[case.id]

    monkeypatch.setattr(arrow_adapter, "canonical", lambda column: column)
    with pytest.raises(ConfigurationError, match="has no column_.* in Arrow") as raised:
        execute_plan(build_plan(case), frames)
    assert str(raised.value) != REFUSALS[case.id]
