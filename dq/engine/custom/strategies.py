# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Constraint strategy objects for the custom engine (TD-ARCH-9).

Each strategy owns one constraint's Spark planning and its bounded outcome
rows. Strategies are stateless and addressable by their legacy constraint
name; the ``CustomEngine`` facade only extracts configuration and dispatches.
Result shapes are permanent contracts: rule packs depend on the metric and
verification row layouts, so strategies may change how they compute a
summary, never what they return (ADR-003).
"""

import logging
from typing import List, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from dq.exceptions import ConfigurationError
from dq.identifiers import ColumnName, IdentifierError, TableName
from dq.utils.spark_expr import grouped_bounds_summary

logger = logging.getLogger(__name__)

_ENTITY = "MultiColumn"


def metric_row(instance: str, dimension: str, value: int) -> list:
    """One legacy metric row: (entity, instance, name, value)."""
    return [_ENTITY, instance, dimension, value]


def verification_row(
    constraint: str,
    level: str,
    check_status: str,
    instance: str,
    constraint_status: str,
    message: str,
) -> list:
    """One legacy verification row with the six contract fields."""
    return [constraint, level, check_status, instance, constraint_status, message]


def no_data_results(constraint, instance, dimension, level, value):
    """Both rows for the legacy 'No suitable data' outcome."""
    return (
        [metric_row(instance, dimension, value)],
        [
            verification_row(
                constraint,
                level,
                "Success",
                instance,
                "Success",
                "No suitable data to validate this rule",
            )
        ],
    )


def violation_expression(value, threshold_min, threshold_max):
    """Build a violation predicate mirroring legacy truthiness rules.

    A threshold of ``0`` or ``None`` disables that bound, exactly like the
    previous per-row Python comparison. Null metric values never violate:
    the comparison stays null and aggregations skip it.
    """
    violation = None
    if threshold_min:
        violation = value < F.lit(threshold_min)
    if threshold_max:
        over = value > F.lit(threshold_max)
        violation = over if violation is None else violation | over
    return F.lit(False) if violation is None else violation


def column_list(value, constraint, param):
    """Normalize a column parameter to a list of column names.

    A bare string names one column; a list or tuple names several. Anything
    else is a configuration error. Without this guard a bare string is
    iterated character by character downstream (``groupBy(*"region")``) and
    surfaces as a cryptic ``UNRESOLVED_COLUMN 'r'`` Spark analysis error
    (issue #25).
    """
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)) and all(
        isinstance(column, str) for column in value
    ):
        return list(value)
    raise ValueError(
        f"{constraint}: '{param}' must be a column name or a list of column "
        f"names, got {value!r}"
    )


def validated_grouped_params(columns, group_by, name):
    """Normalize ``columns``/``group_by`` and require a grouping column."""
    columns = column_list(columns, name, "columns")
    group_by = column_list(group_by, name, "group_by")
    if not group_by:
        raise ValueError(f"{name}: 'group_by' must name at least one grouping column")
    return columns, group_by


def group_result_emitter(name, level, dq_dimension):
    """Build the bounded append pair for a per-column grouped summary.

    Returns:
        Tuple of (emit callable, metric rows, verification rows); the
        lists are the emitter's own accumulators.
    """
    metric_results = []
    check_verifications = []

    def emit(instance, value, check_status, constraint_status, constraint_message):
        metric_results.append(metric_row(instance, dq_dimension, value))
        check_verifications.append(
            verification_row(
                name,
                level,
                check_status,
                instance,
                constraint_status,
                constraint_message,
            )
        )

    return emit, metric_results, check_verifications


def column_result_emitter(name, level, dq_dimension, source):
    """Build the append pair for a per-column reference/negativity summary.

    The verification instance keeps its historical double space after the
    constraint name; legacy outputs are pinned to that exact string. A
    ``timeSeries`` source renders as empty, matching legacy output.

    Returns:
        Tuple of (emit callable, metric rows, verification rows); the
        lists are the emitter's own accumulators.
    """
    if source == "timeSeries":
        source = ""
    metric_results = []
    check_verifications = []

    def emit(column, value, check_status, constraint_status, message):
        metric_results.append(
            metric_row(f"{name} for {column} {source}", dq_dimension, value)
        )
        check_verifications.append(
            verification_row(
                name,
                level,
                check_status,
                f"{name}  for {column} {source}",
                constraint_status,
                message,
            )
        )

    return emit, metric_results, check_verifications


class GroupedDistinctBoundsStrategy:
    """Validate distinct counts of columns within groups meet thresholds.

    Per-group distinct counts are reduced through a distributed minimum,
    maximum, and violation count, so the driver receives one bounded
    summary per column instead of one row per group.
    """

    def apply(
        self,
        dataframe: DataFrame,
        name: str,
        dq_dimension: str,
        level: str,
        columns,
        group_by,
        threshold_min,
        threshold_max,
    ) -> Tuple[List[list], List[list]]:
        columns, group_by = validated_grouped_params(columns, group_by, name)
        logger.info(
            "Checking distinctness of columns '%s' by group '%s'", columns, group_by
        )
        summary = grouped_bounds_summary(
            dataframe,
            group_by,
            columns,
            per_column_extra=lambda distinct, index: F.sum(
                violation_expression(distinct, threshold_min, threshold_max).cast(
                    "long"
                )
            ).alias(f"dq_violations_{index}"),
        )
        group_count = int(summary["dq_group_count"] or 0)

        emit, metric_results, check_verifications = group_result_emitter(
            name, level, dq_dimension
        )
        for index, column in enumerate(columns):
            if group_count == 0:
                break
            minimum = summary[f"dq_min_{index}"]
            maximum = summary[f"dq_max_{index}"]
            violations = int(summary[f"dq_violations_{index}"] or 0)
            instance = f"{name} {group_by} for {column}"
            if violations:
                value = 0
                check_status = "Error"
                constraint_status = "Failure"
                if threshold_min and minimum < threshold_min:
                    constraint_message = (
                        f"{violations} of {group_count} groups below the "
                        f"threshold - {threshold_min} (lowest observed {minimum})"
                    )
                else:
                    constraint_message = (
                        f"{violations} of {group_count} groups above the "
                        f"threshold - {threshold_max} (highest observed {maximum})"
                    )
            else:
                value = 1
                check_status = "Success"
                constraint_status = "Success"
                constraint_message = (
                    f"distinct counts of {group_count} groups in "
                    f"[{minimum}, {maximum}] meet the thresholds"
                )
            emit(instance, value, check_status, constraint_status, constraint_message)

        if len(metric_results) == 0:
            instance = f"{name} {group_by} for {','.join(columns)}"
            return no_data_results(name, instance, dq_dimension, level, 1)
        return metric_results, check_verifications


class ConsecutivePercentChangeStrategy:
    """Detect sudden rate-of-change spikes between consecutive rows.

    Consecutive-pair changes are computed with a distributed ``lag()``
    window expression and reduced to one bounded summary per column. A pair
    is not evaluable when either value is null or the baseline is zero;
    skipped pairs are reported instead of crashing the run.
    """

    def apply(
        self,
        dataframe: DataFrame,
        name: str,
        dq_dimension: str,
        level: str,
        columns,
        group_by,
        sort_by,
        threshold_min,
        threshold_max,
    ) -> Tuple[List[list], List[list]]:
        columns, group_by = validated_grouped_params(columns, group_by, name)
        sort_columns = column_list(sort_by, name, "sort_by")
        if len(sort_columns) != 1:
            raise ValueError(
                f"{name}: 'sort_by' must name exactly one ordering column, "
                f"got {sort_columns!r}"
            )
        sort_by = sort_columns[0]
        logger.info(
            "Checking rate of change for '%s' by group '%s' and sort by '%s'",
            columns,
            group_by,
            sort_by,
        )
        window = Window.partitionBy(*group_by).orderBy(F.col(sort_by))
        projected = dataframe
        aggregate_exprs = []
        for index, column in enumerate(columns):
            has_previous = F.lag(F.lit(1)).over(window).isNotNull()
            previous = F.lag(F.col(column)).over(window)
            current = F.col(column)
            both_present = previous.isNotNull() & current.isNotNull()
            change = F.when(
                both_present & (previous != 0),
                F.abs((previous - current) / previous) * 100,
            )
            # Window expressions must materialize in a projection before the
            # global aggregation; Spark rejects them inside agg().
            projected = projected.withColumn(
                f"dq_roc_change_{index}", change
            ).withColumn(f"dq_roc_pair_{index}", has_previous.cast("int"))
            aggregate_exprs.extend(
                [
                    F.count(F.col(f"dq_roc_change_{index}")).alias(
                        f"dq_roc_evaluated_{index}"
                    ),
                    F.sum(F.col(f"dq_roc_pair_{index}")).alias(f"dq_roc_pairs_{index}"),
                    F.sum(
                        violation_expression(
                            F.col(f"dq_roc_change_{index}"),
                            threshold_min,
                            threshold_max,
                        ).cast("long")
                    ).alias(f"dq_roc_violations_{index}"),
                    F.min(F.col(f"dq_roc_change_{index}")).alias(f"dq_roc_min_{index}"),
                    F.max(F.col(f"dq_roc_change_{index}")).alias(f"dq_roc_max_{index}"),
                ]
            )
        summary = projected.agg(*aggregate_exprs).head()

        emit, metric_results, check_verifications = group_result_emitter(
            name, level, dq_dimension
        )
        for index, column in enumerate(columns):
            evaluated = int(summary[f"dq_roc_evaluated_{index}"] or 0)
            pairs_total = int(summary[f"dq_roc_pairs_{index}"] or 0)
            skipped = pairs_total - evaluated
            if pairs_total == 0:
                continue
            violations = int(summary[f"dq_roc_violations_{index}"] or 0)
            minimum_change = summary[f"dq_roc_min_{index}"]
            maximum_change = summary[f"dq_roc_max_{index}"]
            instance = f"{name} {group_by} for {column}"
            if violations:
                value = 0
                check_status = "Error"
                constraint_status = "Failure"
                constraint_message = (
                    f"{violations} violating of {evaluated} evaluated "
                    f"consecutive pairs ({minimum_change} to "
                    f"{maximum_change}%); {skipped} pairs skipped"
                )
            else:
                value = 1
                check_status = "Success"
                constraint_status = "Success"
                constraint_message = (
                    f"{evaluated} evaluated consecutive pairs within thresholds "
                    f"({minimum_change} to {maximum_change}%); "
                    f"{skipped} pairs skipped"
                )
            emit(instance, value, check_status, constraint_status, constraint_message)

        if len(metric_results) == 0:
            instance = f"{name} {group_by} for {columns}"
            return no_data_results(name, instance, dq_dimension, level, 0)
        return metric_results, check_verifications


class ColumnNamesInReferenceTableStrategy:
    """Check if DataFrame column names are present as rows in a reference table.

    Matches the schema-sized column-name list against the reference table
    through a distributed left-semi join and collects only the matched
    names, never the reference table's rows. Reference identifiers are
    validated before any Spark SQL is built.
    """

    def apply(
        self,
        dataframe: DataFrame,
        name: str,
        dq_dimension: str,
        level: str,
        ref_table=None,
        ref_columns=None,
        ignore_columns=None,
        source="timeSeries",
    ) -> Tuple[List[list], List[list]]:
        logger.debug("Running LookupBasedOnColumnNameList constraint")
        try:
            reference_table = TableName.parse(ref_table, label="ref_table")
            reference_column = ColumnName.parse(ref_columns, label="ref_columns")
        except IdentifierError as error:
            raise ConfigurationError(str(error)) from error
        if ignore_columns and len(ignore_columns) > 0:
            for col in ignore_columns:
                dataframe = dataframe.drop(col)
        reference = dataframe.sparkSession.sql(
            f"Select {reference_column.quoted} from {reference_table.quoted}"
        )
        reference_values = reference.select(
            F.coalesce(F.col(ref_columns).cast("string"), F.lit("None")).alias(
                "ref_value"
            )
        )
        column_names = dataframe.sparkSession.createDataFrame(
            [(name,) for name in dataframe.columns], ["column_name"]
        )
        matched_names = {
            row["column_name"]
            for row in column_names.join(
                reference_values,
                column_names["column_name"] == reference_values["ref_value"],
                "left_semi",
            ).collect()
        }
        emit, metric_results, check_verifications = column_result_emitter(
            name, level, dq_dimension, source
        )
        for column in dataframe.columns:
            if column in matched_names:
                value = 1
                check_status = "Success"
                constraint_status = "Success"
                message = "Column found in the ref table"
            else:
                value = 0
                check_status = "Failure"
                constraint_status = "Failure"
                message = "Column not found in the ref table"
            emit(column, value, check_status, constraint_status, message)

        return metric_results, check_verifications


class NoNegativeValuesStrategy:
    """Check for negative values across all numeric columns in a wide table.

    One aggregate row carries every column's negative count, so the driver
    receives a schema-bounded summary, never dataset rows.
    """

    def apply(
        self,
        dataframe: DataFrame,
        name: str,
        dq_dimension: str,
        level: str,
        ignore_columns=None,
        source="timeSeries",
    ) -> Tuple[List[list], List[list]]:
        logger.debug("Running WideTablesNegativeValuesCheck constraint")
        if ignore_columns and len(ignore_columns) > 0:
            for col in ignore_columns:
                dataframe = dataframe.drop(col)
        columns_to_check = dataframe.columns
        negative_counts = (
            dataframe.select(
                [(F.sum((F.col(c) < 0).cast("int"))).alias(c) for c in columns_to_check]
            )
            .collect()[0]
            .asDict()
        )
        emit, metric_results, check_verifications = column_result_emitter(
            name, level, dq_dimension, source
        )
        for column in dataframe.columns:
            count = negative_counts[column]
            if count is not None and isinstance(count, int) and count > 0:
                value = 0
                check_status = "Failure"
                constraint_status = "Failure"
                message = f"Negative values found for {column} {source}"
            else:
                value = 1
                check_status = "Success"
                constraint_status = "Success"
                message = f"Rule NoNegative values passed for {column} {source}"
            emit(column, value, check_status, constraint_status, message)

        return metric_results, check_verifications
