# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""What the Arrow adapter accepts as a dataset (SPEC-015, G3).

Arrow interchange is what lets one adapter reach pandas, Polars and DataFusion
without importing any of them. A dataset can arrive by six routes, and each
is tested with an object that offers that one route and nothing else, so a
test cannot pass through another: the lesson of SPEC-007, where a fake offered
``toArrow()`` and three passing tests had never run through Arrow. The real
libraries are tested too, when they are installed -- here and in CI always,
since they are development dependencies.
"""

import subprocess
import sys
from decimal import Decimal
from types import SimpleNamespace

import pytest

from dq import arrow_adapter
from dq.arrow_adapter import execute_plan, table_of, validated_bindings
from dq.exceptions import ConfigurationError
from dq.plan import (
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
    Severity,
)

pa = pytest.importorskip("pyarrow", reason="the Arrow adapter needs the arrow extra")

DATASET = DatasetRef("orders", "a" * 64)
ROWS = [
    {"region": "east", "id": "a", "amount": Decimal("1.25")},
    {"region": "east", "id": None, "amount": None},
    {"region": None, "id": "b", "amount": Decimal("3.50")},
    {"region": "west", "id": "b", "amount": Decimal("2.00")},
]
SOURCE = pa.table(
    {
        "region": pa.array([row["region"] for row in ROWS], pa.string()),
        "id": pa.array([row["id"] for row in ROWS], pa.string()),
        "amount": pa.array([row["amount"] for row in ROWS], pa.decimal128(10, 2)),
    }
)


def completeness_plan(column="id", threshold="0.75"):
    return ExecutionPlan(
        (
            RuleSpec(
                "present",
                DATASET,
                RuleKind.COMPLETENESS,
                Predicate(Comparison.EQ, Decimal(threshold)),
                ColumnRef(column),
                Severity.ERROR,
            ),
        )
    )


def present_count(frame):
    outcomes = execute_plan(completeness_plan(), {"orders": frame})
    return outcomes[0].to_legacy()["details"]["observed"]["numerator"]


# ------------------------------------------------------- each route alone


def test_a_pyarrow_table_is_read_as_it_is():
    assert table_of(SOURCE) is SOURCE


def test_a_record_batch_is_read_as_a_table_of_one_batch():
    table = table_of(SOURCE.to_batches()[0])
    assert table.to_pylist() == SOURCE.to_pylist()
    assert isinstance(table, pa.Table)


def test_a_record_batch_reader_is_read_whole_and_keeps_its_chunks():
    reader = pa.RecordBatchReader.from_batches(
        SOURCE.schema, SOURCE.to_batches(max_chunksize=3)
    )
    table = table_of(reader)
    assert table.to_pylist() == SOURCE.to_pylist()
    assert table.column("id").num_chunks == 2


def test_an_object_exporting_only_the_stream_interface_is_read():
    """Polars, DataFusion and pandas 2.2 and later all offer this."""
    streamed = SimpleNamespace(__arrow_c_stream__=SOURCE.__arrow_c_stream__)
    assert table_of(streamed).to_pylist() == SOURCE.to_pylist()
    assert present_count(streamed) == 3


def test_a_single_batch_exported_as_a_stream_is_read():
    batch_stream = SimpleNamespace(
        __arrow_c_stream__=SOURCE.to_batches()[0].__arrow_c_stream__
    )
    assert table_of(batch_stream).to_pylist() == SOURCE.to_pylist()


def test_an_object_offering_only_to_arrow_table_is_read_through_it():
    """DataFusion's own method, offered alone."""
    offered = SimpleNamespace(to_arrow_table=lambda: SOURCE)
    assert table_of(offered) is SOURCE
    assert present_count(offered) == 3


def test_an_object_offering_only_to_arrow_is_read_through_it():
    """Spark Connect and PySpark 4: ``toArrow()`` and nothing else."""
    offered = SimpleNamespace(toArrow=lambda: SOURCE)
    assert table_of(offered) is SOURCE


def test_an_object_offering_only_the_classic_pyspark_export_is_read():
    """Classic PySpark 3.5 has nothing but ``_collect_as_arrow()``. The object
    has nothing but that either, so this cannot pass by another route."""
    offered = SimpleNamespace(
        _collect_as_arrow=lambda: SOURCE.to_batches(max_chunksize=3)
    )
    for absent in ("toArrow", "collect", "__arrow_c_stream__", "to_arrow_table"):
        assert not hasattr(offered, absent)
    table = table_of(offered)
    assert table.to_pylist() == SOURCE.to_pylist()
    assert table.column("id").num_chunks == 2
    assert present_count(offered) == 3


def test_an_empty_classic_pyspark_frame_keeps_its_column_names():
    """An empty frame exports no batch, so there is no schema to read. The
    columns are still known by name, and hold no value of any type."""
    empty = SimpleNamespace(
        _collect_as_arrow=lambda: [], columns=["region", "id", "amount"]
    )
    table = table_of(empty)
    assert table.schema.names == ["region", "id", "amount"]
    assert table.num_rows == 0
    # Every rule is decided on it: nothing is present, and the table is empty.
    outcomes = execute_plan(completeness_plan(), {"orders": empty})
    assert outcomes[0].success is False
    assert outcomes[0].to_legacy()["details"]["reason"] == "empty_dataset"


def test_an_empty_export_without_column_names_is_a_table_of_nothing():
    bare = SimpleNamespace(_collect_as_arrow=lambda: [])
    table = table_of(bare)
    assert table.schema.names == []
    assert table.to_pylist() == []


@pytest.mark.parametrize("offered", [object(), None, 7, "orders.parquet", [1, 2]])
def test_anything_else_is_not_read_and_the_caller_decides(offered):
    assert table_of(offered) is None
    with pytest.raises(
        ConfigurationError, match="must be an Arrow table or export one"
    ):
        validated_bindings(completeness_plan(), {"orders": offered})


# ------------------------------------------------------ the real libraries


def test_pandas_is_read_through_the_stream_interface():
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame(
        {
            "id": ["a", None, "c", "a"],
            "v": [1.5, float("nan"), None, 2.0],
            "d": [Decimal("1.25"), None, Decimal("3.50"), Decimal("2.00")],
        }
    )
    assert hasattr(frame, "__arrow_c_stream__")
    table = table_of(frame)
    assert table.column_names == ["id", "v", "d"]
    assert table.schema.field("d").type == pa.decimal128(3, 2)
    assert present_count(frame) == 3


def test_a_pandas_index_becomes_a_column_as_pyarrow_makes_it():
    """Worth knowing: a pandas frame's index is data to Arrow when it is not
    the plain range index."""
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame({"id": ["a", None, "c"]}).set_axis(
        pd.Index([7, 8, 9], name="row")
    )
    assert table_of(frame).column_names == ["id", "row"]
    plain = pd.DataFrame({"id": ["a", None, "c"]})
    assert table_of(plain).column_names == ["id"]


def test_a_pandas_frame_without_the_stream_interface_is_read_through_pyarrow(
    monkeypatch,
):
    """pandas before 2.2 has no stream interface. The method is hidden on the
    real class to read a real frame the way such a version would be."""
    pd = pytest.importorskip("pandas")
    monkeypatch.delattr(pd.DataFrame, "__arrow_c_stream__")
    frame = pd.DataFrame({"id": ["a", None, "c", "a"]})
    assert not hasattr(frame, "__arrow_c_stream__")
    assert table_of(frame).to_pylist() == [
        {"id": "a"},
        {"id": None},
        {"id": "c"},
        {"id": "a"},
    ]
    assert present_count(frame) == 3


def test_a_pandas_frame_with_nullable_dtypes_is_read_as_its_values():
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame({"id": ["a", None, "c"], "n": [1, None, 3]}).convert_dtypes()
    assert present_count(frame) == 2
    assert table_of(frame).column("n").to_pylist() == [1, None, 3]


def test_polars_is_read_and_its_view_columns_are_measured():
    pl = pytest.importorskip("polars")
    frame = pl.DataFrame(
        {
            "region": [row["region"] for row in ROWS],
            "id": [row["id"] for row in ROWS],
            "amount": [row["amount"] for row in ROWS],
        }
    )
    assert hasattr(frame, "__arrow_c_stream__") and not hasattr(frame, "to_arrow_table")
    table = table_of(frame)
    assert table.to_pylist() == SOURCE.to_pylist()
    # Polars hands its strings over as string_view, and its decimals at the
    # widest precision: both are measured as the values they hold.
    assert present_count(frame) == 3
    grouped = ExecutionPlan(
        (
            RuleSpec(
                "a_value_in_every_region",
                DATASET,
                RuleKind.GROUPED_DISTINCT,
                Predicate(Comparison.GE, Decimal(1)),
                ColumnRef("id"),
                Severity.ERROR,
                (ColumnRef("region"),),
                MetricKind.GROUP_MIN_DISTINCT,
            ),
        )
    )
    outcomes = execute_plan(grouped, {"orders": frame})
    assert outcomes[0].success is True  # the null-key group holds one value: b


def test_a_polars_lazy_frame_is_refused_by_its_type():
    pl = pytest.importorskip("polars")
    lazy = pl.LazyFrame({"id": ["a", None, "c"]})
    assert table_of(lazy) is None
    with pytest.raises(ConfigurationError, match="got LazyFrame"):
        validated_bindings(completeness_plan(), {"orders": lazy})


def test_datafusion_is_read_through_the_stream_interface():
    datafusion = pytest.importorskip("datafusion")
    frame = datafusion.SessionContext().from_arrow(SOURCE, name="orders")
    assert hasattr(frame, "__arrow_c_stream__") and hasattr(frame, "to_arrow_table")
    assert table_of(frame).to_pylist() == SOURCE.to_pylist()
    assert present_count(frame) == 3


def test_datafusion_is_read_through_to_arrow_table_when_that_is_all_it_offers():
    datafusion = pytest.importorskip("datafusion")
    frame = datafusion.SessionContext().from_arrow(SOURCE, name="orders")
    offered = SimpleNamespace(to_arrow_table=frame.to_arrow_table)
    assert table_of(offered).to_pylist() == SOURCE.to_pylist()


# ------------------------------------------------ nothing else is imported


def test_every_route_works_with_no_library_but_pyarrow_installed():
    """The adapter imports no library but pyarrow, and asks an object only
    for the method it offers. In a process where pandas, Polars, DataFusion
    and Spark are absent, every route is still read and a plan still runs.

    pyarrow itself loads pandas when pandas is installed, so the libraries
    are made absent here rather than their import forbidden.
    """
    code = """
import sys

ABSENT = {"pyspark", "pydeequ", "great_expectations", "datafusion", "pandas", "polars", "sqlalchemy"}


class NotInstalled:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in ABSENT:
            raise ModuleNotFoundError(f"No module named {fullname!r}", name=fullname)

sys.meta_path.insert(0, NotInstalled())
from types import SimpleNamespace

import pyarrow as pa

from dq.arrow_adapter import execute_plan, table_of
from dq.sdk.fixtures import DATASET_NAME, build_plan, decisions, load_cases
from dq.tests.fixture_frames import arrow_table

source = pa.table({"id": pa.array(["a", None, "c"], pa.string())})
rows = source.to_pylist()
for offered in (
    SimpleNamespace(__arrow_c_stream__=source.__arrow_c_stream__),
    SimpleNamespace(to_arrow_table=lambda: source),
    SimpleNamespace(toArrow=lambda: source),
    SimpleNamespace(_collect_as_arrow=lambda: source.to_batches()),
):
    assert table_of(offered).to_pylist() == rows, offered

loaded = [n for n in sys.modules if n.split(".")[0] in ABSENT]
assert not loaded, loaded
checked = 0
for case in load_cases():
    if case.expect_error:
        continue
    # As a classic Spark frame does: the export, and the column names for
    # the case where an empty frame gives no batch.
    exported = SimpleNamespace(
        _collect_as_arrow=lambda: arrow_table(case).to_batches(),
        columns=list(case.column_names),
    )
    got = decisions(execute_plan(build_plan(case), {DATASET_NAME: table_of(exported)}))
    assert got == dict(case.expected), (case.id, got)
    checked += 1
assert checked >= 20, checked
loaded = [n for n in sys.modules if n.split(".")[0] in ABSENT]
assert not loaded, loaded
print("ok", checked)
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().startswith("ok ")
