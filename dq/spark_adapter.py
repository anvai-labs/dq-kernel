# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Native Spark adapter executing portable plans over shared aggregations.

Counts/v1 and groups/v1 semantics are owned by ``dq.plan``. This adapter
computes only bounded aggregates: counts/v1 plans get one aggregate action per
dataset with every required metric combined; groups/v1 plans get one aggregate
per distinct grouping, reducing group-level distinct counts to count/min/max.
Duplicate metric requests share a single scan, and the driver receives exactly
one aggregate row per dataset — never dataset or per-group rows.
Unsupported capabilities and missing, extra, non-DataFrame, or case-ambiguous
bindings fail before any Spark job runs. ``DatasetRef`` digests certify
caller-supplied snapshot identity; this adapter does not verify content.
"""

from __future__ import annotations

from collections.abc import Mapping
from fractions import Fraction
from typing import TYPE_CHECKING

from dq.compute_support import ComputeAdapter
from dq.compute_support import as_exact_fraction as _as_exact_fraction
from dq.compute_support import (
    execute_envelope_with,
    execute_plan_with,
    require_exact_bindings,
    require_exact_column,
)
from dq.exceptions import ConfigurationError
from dq.outcomes import CheckOutcome
from dq.plan import (
    CapabilitySet,
    ExecutionPlan,
    MetricKind,
    SEMANTIC_GROUPS_VERSION,
    SEMANTIC_RANGES_VERSION,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

ADAPTER_NAME = "spark"
ADAPTER_VERSION = "1"
CAPABILITIES = CapabilitySet(
    ADAPTER_NAME,
    ADAPTER_VERSION,
    frozenset({MetricKind.ROW_COUNT, MetricKind.PRESENT_COUNT}),
)
GROUP_CAPABILITIES = CapabilitySet(
    ADAPTER_NAME,
    ADAPTER_VERSION,
    frozenset(
        {
            MetricKind.GROUP_COUNT,
            MetricKind.GROUP_MIN_DISTINCT,
            MetricKind.GROUP_MAX_DISTINCT,
        }
    ),
    SEMANTIC_GROUPS_VERSION,
)
RANGES_CAPABILITIES = CapabilitySet(
    ADAPTER_NAME,
    ADAPTER_VERSION,
    frozenset(
        {
            MetricKind.COLUMN_COUNT,
            MetricKind.COLUMN_MIN,
            MetricKind.COLUMN_MAX,
        }
    ),
    SEMANTIC_RANGES_VERSION,
)


def capabilities_for(plan: ExecutionPlan) -> CapabilitySet:
    """The Spark adapter capabilities matching a plan's semantics."""
    if plan.semantic_version == SEMANTIC_GROUPS_VERSION:
        return GROUP_CAPABILITIES
    if plan.semantic_version == SEMANTIC_RANGES_VERSION:
        return RANGES_CAPABILITIES
    return CAPABILITIES


_capabilities_for = capabilities_for


def execute_plan(
    plan: ExecutionPlan, datasets: Mapping[str, DataFrame]
) -> tuple[CheckOutcome, ...]:
    """Compute the plan's metrics with Spark and evaluate exact outcomes."""
    return execute_plan_with(plan, datasets, _ADAPTER)


def execute_envelope(
    envelope, datasets: Mapping[str, DataFrame]
) -> tuple[CheckOutcome, ...]:
    """Execute every subplan of a :class:`RulesetEnvelope` with Spark.

    Mixed-semantics rulesets get one entry point, as for every adapter
    (:func:`dq.compute_support.execute_envelope_with`).
    """
    return execute_envelope_with(envelope, datasets, _ADAPTER)


def compute_plan_values(plan: ExecutionPlan, bindings: dict) -> dict:
    """Compute every plan metric over the bound DataFrames."""

    values = {}
    for name in sorted(bindings):
        if plan.semantic_version == SEMANTIC_GROUPS_VERSION:
            values.update(_dataset_group_metrics(name, bindings[name], plan))
        elif plan.semantic_version == SEMANTIC_RANGES_VERSION:
            values.update(_dataset_range_metrics(name, bindings[name], plan))
        else:
            values.update(_dataset_counts(name, bindings[name], plan))
    return values


def _validated_bindings(plan: ExecutionPlan, datasets) -> dict[str, DataFrame]:
    if not isinstance(datasets, Mapping):
        raise ConfigurationError(
            "dataset bindings require a mapping of dataset names to DataFrames"
        )
    bindings = dict(datasets)
    require_exact_bindings((metric.dataset.name for metric in plan.metrics), bindings)
    from pyspark.sql import DataFrame

    for name, dataframe in bindings.items():
        if not isinstance(dataframe, DataFrame):
            raise ConfigurationError(
                f"dataset binding {name!r} must be a Spark DataFrame"
            )
    return bindings


def _require_exact_column(dataframe, name: str, column: str):
    """Resolve one column by exact name, rejecting ambiguity, and return its field."""
    fields = dataframe.schema.fields
    return require_exact_column(
        name,
        column,
        [field.name for field in fields],
        {field.name: field for field in fields},
    )


def _dataset_counts(name: str, dataframe, plan: ExecutionPlan) -> dict:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, FloatType

    metrics = [metric for metric in plan.metrics if metric.dataset.name == name]

    aggregates = [F.count(F.lit(1)).alias("dq_row_count")]
    aliases = {}
    for index, metric in enumerate(metrics):
        if metric.kind is not MetricKind.PRESENT_COUNT or metric.column is None:
            continue
        column = metric.column.name
        field = _require_exact_column(dataframe, name, column)
        column_expr = F.col(column)
        condition = column_expr.isNotNull()
        if isinstance(field.dataType, (FloatType, DoubleType)):
            condition = ~(column_expr.isNull() | F.isnan(column_expr))
        alias = f"dq_present_{index}"
        aggregates.append(F.count(F.when(condition, F.lit(1))).alias(alias))
        aliases[metric] = alias

    row = dataframe.agg(*aggregates).head()
    values = {}
    for metric in metrics:
        if metric.kind is MetricKind.ROW_COUNT:
            values[metric] = int(row["dq_row_count"])
        else:
            values[metric] = int(row[aliases[metric]])
    return values


def _dataset_group_metrics(name: str, dataframe, plan: ExecutionPlan) -> dict:
    """Compute grouped distinct bounds, one aggregate per distinct grouping.

    Group-level distinct counts are reduced to the group count and the
    minimum and maximum per column inside Spark; the driver receives one
    summary row per grouping. On empty input the bounds are 0 and every
    grouped rule passes vacuously in the kernel.
    """
    from pyspark.sql import functions as F

    # Lazy like the pyspark imports above: keeps this module importable
    # without a JVM (see test_kernel_imports_without_optional_dependencies).
    from dq.utils.spark_expr import grouped_bounds_summary

    metrics = [metric for metric in plan.metrics if metric.dataset.name == name]
    groupings = {}
    for metric in metrics:
        groupings.setdefault(metric.group_by, []).append(metric)

    values = {}
    for grouping in sorted(
        groupings, key=lambda columns: [column.name for column in columns]
    ):
        group_metrics = groupings[grouping]
        columns = sorted(
            {
                metric.column.name
                for metric in group_metrics
                if metric.column is not None
            }
        )
        for column in columns:
            _require_exact_column(dataframe, name, column)
        for column_ref in grouping:
            _require_exact_column(dataframe, name, column_ref.name)

        summary = grouped_bounds_summary(
            dataframe,
            [F.col(column_ref.name) for column_ref in grouping],
            columns,
        )
        group_count = int(summary["dq_group_count"])

        column_index = {column: index for index, column in enumerate(columns)}
        for metric in group_metrics:
            if metric.kind is MetricKind.GROUP_COUNT:
                values[metric] = group_count
            elif group_count == 0:
                values[metric] = 0
            elif metric.kind is MetricKind.GROUP_MIN_DISTINCT:
                values[metric] = int(
                    summary[f"dq_min_{column_index[metric.column.name]}"]
                )
            else:
                values[metric] = int(
                    summary[f"dq_max_{column_index[metric.column.name]}"]
                )
    return values


def _dataset_range_metrics(name: str, dataframe, plan: ExecutionPlan) -> dict:
    """Compute value-range bounds, one aggregate per dataset.

    Nulls and NaN are excluded from the count and the bounds; bounds are
    converted to exact fractions (binary-float bounds render through their
    shortest decimal, and infinite bounds fail closed). The driver
    receives one summary row per dataset — never column values.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, FloatType

    metrics = [metric for metric in plan.metrics if metric.dataset.name == name]
    columns = sorted({metric.column.name for metric in metrics if metric.column})
    for column in columns:
        _require_exact_column(dataframe, name, column)

    aggregates = []
    column_index = {column: index for index, column in enumerate(columns)}
    for index, column in enumerate(columns):
        field = _require_exact_column(dataframe, name, column)
        comparable = F.col(column)
        if isinstance(field.dataType, (FloatType, DoubleType)):
            # Spark ranks NaN above every value, so min/max would return it
            # unless the comparison values are filtered explicitly.
            comparable = F.when(
                ~(F.col(column).isNull() | F.isnan(F.col(column))), F.col(column)
            )
        else:
            comparable = F.when(F.col(column).isNotNull(), F.col(column))
        aggregates.extend(
            [
                F.count(comparable).alias(f"dq_count_{index}"),
                F.min(comparable).alias(f"dq_min_{index}"),
                F.max(comparable).alias(f"dq_max_{index}"),
            ]
        )
    summary = dataframe.agg(*aggregates).head()

    values = {}
    for metric in metrics:
        index = column_index[metric.column.name]
        if metric.kind is MetricKind.COLUMN_COUNT:
            values[metric] = int(summary[f"dq_count_{index}"])
            continue
        raw = summary[
            (
                f"dq_min_{index}"
                if metric.kind is MetricKind.COLUMN_MIN
                else f"dq_max_{index}"
            )
        ]
        bound = _as_exact_fraction(raw, name, metric.column.name)
        values[metric] = Fraction(0) if bound is None else bound
    return values


#: What this module offers a plan: its capabilities, its binding check, its computation.
_ADAPTER = ComputeAdapter(capabilities_for, _validated_bindings, compute_plan_values)
