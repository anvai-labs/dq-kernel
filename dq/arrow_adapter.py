# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Arrow compute adapter: portable plans over Arrow tables, with no Spark.

The kernel owns the semantics (``dq.plan``). This adapter computes metric
values and hands them to the kernel's own evaluation, so a decision is made
in one place whichever engine computed. A counts/v1 plan reads each column
once, a groups/v1 plan groups each distinct grouping once and then counts
distinct values per measured column, and a ranges/v1 plan reads each column
once. What leaves the computation is bounded: counts, and bounds as exact
fractions.

What arrives is put in order twice, because "it is an Arrow table" is not
yet "Arrow can compute on it".

1. To an Arrow table (:func:`table_of`): a table, a record batch, anything
   that exports Arrow's stream interface -- Polars, DataFusion, pandas -- or
   a Spark DataFrame through the export it has. No library but pyarrow is
   imported.
2. Column by column, to the form every kernel the adapter needs is written
   for (:func:`canonical`). A Polars string arrives as ``string_view``,
   which has no distinct-count kernel on some pyarrow; a half-float has no
   minimum, and neither have ``decimal32`` and ``decimal64``; a null-typed
   column cannot be counted at all; and a dictionary-encoded column, which
   ``pq.read_table`` returns for a column stored that way, has none of it.
   Each is decoded or widened, losslessly, before it is measured.

   Every float is measured as double, and ``0.0`` is added to it. IEEE
   arithmetic turns ``-0.0`` into ``0.0`` by that and leaves every other
   value alone, which is what makes Arrow count signed zeros as one value:
   it counts them apart, and Spark counts them as one.

What Arrow cannot compute on is refused, never approximated: a list or a
struct has no distinct count and no bound, and the refusal names the
dataset, the column and the metric.

This module imports with pyarrow absent. The import happens when a plan is
executed, and without pyarrow the adapter refuses, naming the extra.
"""

from __future__ import annotations

from collections.abc import Mapping
from fractions import Fraction
from typing import Any

from dq.compute_support import (
    ComputeAdapter,
    as_exact_fraction,
    capability_sets,
    execute_envelope_with,
    execute_plan_with,
    metrics_by_grouping,
    metrics_of,
    require_exact_bindings,
    require_exact_column,
    values_by_dataset,
)
from dq.exceptions import ConfigurationError
from dq.outcomes import CheckOutcome
from dq.plan import (
    CapabilitySet,
    ExecutionPlan,
    MetricKind,
    SEMANTIC_GROUPS_VERSION,
    SEMANTIC_RANGES_VERSION,
    SEMANTICS_VERSION,
)
from dq.utils.imports import require_extra

ADAPTER_NAME = "arrow"
ADAPTER_VERSION = "1"
_SETS = capability_sets(ADAPTER_NAME, ADAPTER_VERSION)
#: What this adapter can compute for each semantic version. These names are
#: what ``capabilities_for`` reads, so a caller can narrow one for a test.
CAPABILITIES = _SETS[SEMANTICS_VERSION]
GROUP_CAPABILITIES = _SETS[SEMANTIC_GROUPS_VERSION]
RANGES_CAPABILITIES = _SETS[SEMANTIC_RANGES_VERSION]


def capabilities_for(plan: ExecutionPlan) -> CapabilitySet:
    """The Arrow adapter capabilities matching a plan's semantics."""
    if plan.semantic_version == SEMANTIC_GROUPS_VERSION:
        return GROUP_CAPABILITIES
    if plan.semantic_version == SEMANTIC_RANGES_VERSION:
        return RANGES_CAPABILITIES
    return CAPABILITIES


# ------------------------------------------------------------- what arrives


_EXTRA = ("pyarrow>=16,<27", "adapter")


def _pyarrow():
    """Return pyarrow, or name the extra that provides it."""
    return require_extra("pyarrow", "pyarrow", "arrow", pin=_EXTRA[0], kind=_EXTRA[1])


def _arrow():
    """Return ``(pyarrow, pyarrow.compute)``, or name the extra.

    A bare ``import pyarrow`` does not make ``pyarrow.compute`` an
    attribute, so the submodule is imported on its own, the same way.
    """
    pin, kind = _EXTRA
    return (
        _pyarrow(),
        require_extra("pyarrow.compute", "pyarrow", "arrow", pin=pin, kind=kind),
    )


def table_of(frame):
    """Return ``frame`` as a pyarrow table, or ``None`` when Arrow cannot.

    Read, in this order, each found by name on the object:

    * a pyarrow table, as it is;
    * a record batch;
    * a Spark DataFrame, through the export it has: ``toArrow()`` on Spark
      Connect and PySpark 4, and the private ``_collect_as_arrow()`` on
      classic PySpark 3.5. An empty frame exports no batch, so its table is
      built from the frame's column names with no row in it.
    * an object with ``__arrow_c_stream__``, Arrow's stream interface:
      Polars, DataFusion, pandas 2.2 and later;
    * an object with ``to_arrow_table()``: DataFusion;
    * a pandas DataFrame, through pyarrow's own conversion.

    Args:
        frame: What a dataset was bound to.

    Returns:
        The table, or ``None`` for anything else -- so that a caller with a
        fallback may take it, and one without refuses by the object's type.
    """
    pa = _pyarrow()
    if isinstance(frame, pa.Table):
        return frame
    if isinstance(frame, pa.RecordBatch):
        return pa.Table.from_batches([frame])
    to_arrow = getattr(frame, "toArrow", None)
    if callable(to_arrow):
        return to_arrow()
    collect = getattr(frame, "_collect_as_arrow", None)
    if callable(collect):
        batches = collect()
        if batches:
            return pa.Table.from_batches(batches)
        # No batch, so no schema to build from. The columns are known by
        # name, and hold no value of any type.
        columns = getattr(frame, "columns", None) or ()
        return pa.table({str(name): pa.array([], type=pa.null()) for name in columns})
    if hasattr(frame, "__arrow_c_stream__"):
        return pa.table(frame)
    to_table = getattr(frame, "to_arrow_table", None)
    if callable(to_table):
        return to_table()
    if _is_pandas(frame):
        return pa.table(frame)
    return None


def _is_pandas(frame) -> bool:
    """Whether ``frame`` is a pandas DataFrame, without importing pandas."""
    return any(cls.__module__.split(".")[0] == "pandas" for cls in type(frame).__mro__)


# ------------------------------------------------------ the form we compute


def _is_view(pa, kind) -> bool:
    """Whether a type is ``string_view`` or ``binary_view``."""
    types = pa.types
    is_string_view = getattr(types, "is_string_view", None)
    is_binary_view = getattr(types, "is_binary_view", None)
    return bool(
        (is_string_view and is_string_view(kind))
        or (is_binary_view and is_binary_view(kind))
    )


def _is_narrow_decimal(pa, kind) -> bool:
    """Whether a type is ``decimal32`` or ``decimal64``."""
    types = pa.types
    is_decimal32 = getattr(types, "is_decimal32", None)
    is_decimal64 = getattr(types, "is_decimal64", None)
    return bool(
        (is_decimal32 and is_decimal32(kind)) or (is_decimal64 and is_decimal64(kind))
    )


def canonical(column):
    """Return a column in the form every kernel the adapter needs has.

    Each step is lossless, and a column already in the form is returned as
    it is:

    * a dictionary-encoded column is decoded to its values;
    * a null-typed column -- all nulls, or no row at all -- becomes null
      integers, which can be counted: Arrow counts an all-null column of
      the null type as one distinct value, and Spark counts none;
    * ``string_view`` and ``binary_view`` become ``string`` and ``binary``;
    * a half- or single-precision float is widened to double, and so is
      ``decimal32`` or ``decimal64`` to ``decimal128``.

    A float column of any width comes back as double with ``0.0`` added, so
    that its signed zeros are one value.

    Args:
        column: A ``pyarrow.ChunkedArray`` or ``pyarrow.Array``.

    Returns:
        The column's values in the form described, of the same length.
    """
    pa, pc = _arrow()
    types = pa.types
    kind = column.type
    if types.is_dictionary(kind):
        column = pc.cast(column, kind.value_type)
    elif types.is_null(kind):
        column = pc.cast(column, pa.int64())
    elif _is_view(pa, kind):
        column = pc.cast(
            column, pa.string() if types.is_string_view(kind) else pa.binary()
        )
    kind = column.type
    if types.is_floating(kind):
        if kind != pa.float64():
            column = pc.cast(column, pa.float64())
        column = pc.add(column, pa.scalar(0.0, type=pa.float64()))
    elif _is_narrow_decimal(pa, kind):
        column = pc.cast(column, pa.decimal128(kind.precision, kind.scale))
    return column


def _computed(dataset: str, column: str, metric: str, compute):
    """Run one Arrow computation, refusing what Arrow cannot compute."""
    try:
        return compute()
    except Exception as error:
        if not isinstance(error, _pyarrow().ArrowException):
            raise
        raise ConfigurationError(
            f"column {column!r} in dataset {dataset!r} has no {metric} "
            f"in Arrow: {error}"
        ) from error


def _resolve(table, dataset: str, columns) -> None:
    """Refuse a column that is missing or ambiguous, by the shared message."""
    schema = table.schema
    fields = {field.name: field.type for field in schema}
    for column in columns:
        require_exact_column(dataset, column, schema.names, fields)


def _column(table, dataset: str, column: str, metric: str):
    """Return one column in the form the adapter computes on."""
    return _computed(dataset, column, metric, lambda: canonical(table.column(column)))


def _present_mask(array):
    """Where a value is present: not null, and for a float not NaN either."""
    pa, pc = _arrow()
    types = pa.types
    valid = pc.is_valid(array)
    if types.is_floating(array.type):
        return pc.and_(valid, pc.invert(pc.is_nan(array)))
    return valid


# ------------------------------------------------------------- the metrics


def _dataset_counts(name: str, table, plan: ExecutionPlan) -> dict:
    """Row count from the table, and one pass per column for the present."""
    pa, pc = _arrow()
    metrics = metrics_of(plan, name)
    _resolve(table, name, [metric.column.name for metric in metrics if metric.column])
    values = {
        metric: table.num_rows
        for metric in metrics
        if metric.kind is MetricKind.ROW_COUNT
    }
    for metric in metrics:
        if metric.kind is MetricKind.PRESENT_COUNT:
            column = metric.column.name
            array = _column(table, name, column, "present_count")
            present = _computed(
                name,
                column,
                "present_count",
                lambda: pc.sum(pc.cast(_present_mask(array), pa.int64())).as_py(),
            )
            values[metric] = 0 if present is None else int(present)
    return values


def _dataset_group_metrics(name: str, table, plan: ExecutionPlan) -> dict:
    """Grouped distinct bounds: the keys grouped once, then one distinct
    count per measured column.

    Each is a step of its own, so that a refusal names the column it is
    about: Arrow counts distinct values in a hash table, and reports a
    measured column it cannot hash as a key it cannot group on.
    """
    pa, pc = _arrow()
    values = {}
    metrics = metrics_of(plan, name)
    for grouping, group_metrics in metrics_by_grouping(metrics).items():
        keys = [column.name for column in grouping]
        measured = sorted(
            {metric.column.name for metric in group_metrics if metric.column}
        )
        labels = {}  # the first metric that asks for a column names it
        for metric in group_metrics:
            if metric.column is not None:
                labels.setdefault(metric.column.name, metric.kind.value)
        _resolve(table, name, measured)
        _resolve(table, name, keys)

        keyed = pa.table(
            {key: _column(table, name, key, "group_count") for key in keys}
        )
        count = _computed(
            name,
            "+".join(keys),
            "group_count",
            lambda: keyed.group_by(keys).aggregate([]),
        ).num_rows

        bounds = {}
        for column in measured:
            if count == 0:
                break
            source = pa.table(
                {
                    **{key: keyed.column(key) for key in keys if key != column},
                    column: _column(table, name, column, labels[column]),
                }
            )
            per_group = _computed(
                name,
                column,
                labels[column],
                lambda: source.group_by(keys).aggregate([(column, "count_distinct")]),
            )
            bounds[column] = _computed(
                name,
                column,
                labels[column],
                lambda: pc.min_max(
                    per_group.column(f"{column}_count_distinct")
                ).as_py(),
            )

        for metric in group_metrics:
            if metric.kind is MetricKind.GROUP_COUNT:
                values[metric] = count
            elif count == 0:
                values[metric] = 0
            else:
                low = metric.kind is MetricKind.GROUP_MIN_DISTINCT
                values[metric] = int(
                    bounds[metric.column.name]["min" if low else "max"]
                )
    return values


def _dataset_range_metrics(name: str, table, plan: ExecutionPlan) -> dict:
    """Value-range bounds: one pass per column, nulls and NaN left out."""
    _, pc = _arrow()
    metrics = metrics_of(plan, name)
    columns = sorted({metric.column.name for metric in metrics if metric.column})
    _resolve(table, name, columns)
    measured = {}
    for column in columns:
        array = _column(table, name, column, "column_min")
        kept = _computed(
            name,
            column,
            "column_count",
            lambda: pc.filter(array, _present_mask(array)),
        )
        bounds = (
            _computed(
                name,
                column,
                "column_min",
                lambda: pc.min_max(kept).as_py(),
            )
            if len(kept)
            else None
        )
        measured[column] = (
            len(kept),
            bounds["min"] if bounds else None,
            bounds["max"] if bounds else None,
        )
    values = {}
    for metric in metrics:
        count, low, high = measured[metric.column.name]
        if metric.kind is MetricKind.COLUMN_COUNT:
            values[metric] = count
            continue
        bound = as_exact_fraction(
            low if metric.kind is MetricKind.COLUMN_MIN else high,
            name,
            metric.column.name,
        )
        values[metric] = Fraction(0) if bound is None else bound
    return values


# ------------------------------------------------------------------ surface


def validated_bindings(plan: ExecutionPlan, datasets) -> dict:
    """Return the plan's datasets as Arrow tables, or refuse the bindings.

    Raises:
        ConfigurationError: When the bindings are not a mapping, do not name
            exactly the plan's datasets, or hold something Arrow cannot read.
    """
    if not isinstance(datasets, Mapping):
        raise ConfigurationError(
            "dataset bindings require a mapping of dataset names to Arrow tables"
        )
    bindings = dict(datasets)
    require_exact_bindings((metric.dataset.name for metric in plan.metrics), bindings)
    tables = {}
    for name, frame in bindings.items():
        table = table_of(frame)
        if table is None:
            raise ConfigurationError(
                f"dataset binding {name!r} must be an Arrow table or export "
                f"one; got {type(frame).__name__}"
            )
        tables[name] = table
    return tables


def compute_plan_values(plan: ExecutionPlan, bindings: dict) -> dict:
    """Compute every plan metric over the bound Arrow tables."""
    return values_by_dataset(
        plan,
        bindings,
        _dataset_counts,
        _dataset_group_metrics,
        _dataset_range_metrics,
    )


def execute_plan(
    plan: ExecutionPlan, datasets: Mapping[str, Any]
) -> tuple[CheckOutcome, ...]:
    """Compute the plan's metrics with Arrow and evaluate exact outcomes."""
    return execute_plan_with(plan, datasets, _ADAPTER)


def execute_envelope(envelope, datasets: Mapping[str, Any]) -> tuple[CheckOutcome, ...]:
    """Execute every subplan of a :class:`~dq.envelope.RulesetEnvelope` with Arrow.

    Mixed-semantics rulesets get one entry point, as for every adapter
    (:func:`dq.compute_support.execute_envelope_with`).
    """
    return execute_envelope_with(envelope, datasets, _ADAPTER)


#: What this module offers a plan: its capabilities, its binding check, its computation.
_ADAPTER = ComputeAdapter(capabilities_for, validated_bindings, compute_plan_values)
