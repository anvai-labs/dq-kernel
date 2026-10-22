# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared Spark aggregation builders for grouped distinct-bounds pipelines.

Both the portable adapter (``dq.spark_adapter``) and the businessrules engine's
grouped strategy build the same ``dq_distinct_{i}`` / ``dq_group_count``
/ ``dq_min_{i}`` / ``dq_max_{i}`` pipeline; this module is its single
definition. Consumers that lazy-import pyspark (the adapter) must also
lazy-import this module to preserve JVM-free core imports.
"""

from pyspark.sql import functions as F


def grouped_bounds_summary(dataframe, group_cols, columns, per_column_extra=None):
    """Reduce per-group distinct counts to one bounded summary row.

    Args:
        dataframe: Incoming DataFrame to aggregate.
        group_cols: Grouping columns (names or Column expressions).
        columns: Measured columns, indexed into the ``dq_distinct_{i}``
            aliases.
        per_column_extra: Optional callable ``(distinct_col, index) ->
            Column`` appended to each column's min/max aggregates (e.g.
            a violations sum).

    Returns:
        The collected summary Row (empty-safe coercions are the
        caller's concern).
    """
    grouped_counts = dataframe.groupBy(*group_cols).agg(
        *[
            F.countDistinct(F.col(column)).alias(f"dq_distinct_{index}")
            for index, column in enumerate(columns)
        ]
    )
    aggregate_exprs = [F.count(F.lit(1)).alias("dq_group_count")]
    for index in range(len(columns)):
        distinct = F.col(f"dq_distinct_{index}")
        per_column = [
            F.min(distinct).alias(f"dq_min_{index}"),
            F.max(distinct).alias(f"dq_max_{index}"),
        ]
        if per_column_extra is not None:
            per_column.append(per_column_extra(distinct, index))
        aggregate_exprs.extend(per_column)
    return grouped_counts.agg(*aggregate_exprs).head()
