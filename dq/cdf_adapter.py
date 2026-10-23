# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

"""Delta Change Data Feed adapter for the portable plan kernel (ADR-011 role B).

Validates the *increment*: after a Delta table refresh, read the Change
Data Feed window for the new version range and evaluate aggregate checks
scoped to the change batch through the kernel's versioned semantics --
the aggregate/uniqueness/bounds residue that platform row-level
expectations cannot express.

V1 semantics are explicitly **change-batch scoped**: row counts,
completeness, within-batch distinct bounds, and value ranges are
computed over the change rows of the window (``insert`` plus
``update_postimage``; deletes and preimages are null-filled for data
columns and excluded from column validation). A record changed more
than once inside the window contributes one row per commit, so counts
measure *change volume*, not logical record counts; full-table and
deduplicated-by-key semantics remain future modes. Reading requires the
table property ``delta.enableChangeDataFeed=true`` (the ``delta`` extra
provides the Delta data source outside Databricks/EMR).

Like ``dq.spark_adapter`` this is a standalone module function (not an
EngineLoader engine, per the ADR-011 standalone precedent); metric
computation is delegated to the Spark adapter so both adapters share
one implementation of counts/groups/ranges.
"""

from dq.exceptions import ConfigurationError
from dq.plan import (
    CapabilitySet,
    ExecutionPlan,
    SEMANTIC_GROUPS_VERSION,
    SEMANTIC_RANGES_VERSION,
)
from dq.spark_adapter import (
    CAPABILITIES as _SPARK_COUNTS,
    GROUP_CAPABILITIES as _SPARK_GROUPS,
    RANGES_CAPABILITIES as _SPARK_RANGES,
    compute_plan_values,
)

ADAPTER_NAME = "spark-cdf"
ADAPTER_VERSION = "1"


def _cdf_capabilities(spark_caps: CapabilitySet) -> CapabilitySet:
    return CapabilitySet(
        ADAPTER_NAME,
        ADAPTER_VERSION,
        spark_caps.metrics,
        spark_caps.semantic_version,
    )


CAPABILITIES = _cdf_capabilities(_SPARK_COUNTS)
GROUP_CAPABILITIES = _cdf_capabilities(_SPARK_GROUPS)
RANGES_CAPABILITIES = _cdf_capabilities(_SPARK_RANGES)

_CHANGE_ROW_TYPES = ["insert", "update_postimage"]
_CHANGE_METADATA_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")
_CDF_ERROR_MARKERS = ("change data feed", "change data was not recorded")


def capabilities_for(plan: ExecutionPlan) -> CapabilitySet:
    """The spark-cdf adapter capabilities matching a plan's semantics."""
    if plan.semantic_version == SEMANTIC_GROUPS_VERSION:
        return GROUP_CAPABILITIES
    if plan.semantic_version == SEMANTIC_RANGES_VERSION:
        return RANGES_CAPABILITIES
    return CAPABILITIES


def _as_cdf_configuration_error(exc):
    message = str(exc).lower()
    if any(marker in message for marker in _CDF_ERROR_MARKERS):
        return ConfigurationError(
            "the Delta table does not expose a readable change data feed "
            "for this window; enable it with ALTER TABLE <table> SET "
            "TBLPROPERTIES ('delta.enableChangeDataFeed' = true) and verify "
            "the requested versions were written after enabling"
        )
    return None


def read_changes(spark, table_ref, starting_version, ending_version=None):
    """Read the change batch of a Delta Change Data Feed window.

    Args:
        spark: Active SparkSession.
        table_ref: Delta table reference (``catalog.schema.table``).
        starting_version: First table version in the window (int >= 0).
        ending_version: Last version inclusive; the feed's latest when
            omitted.

    Returns:
        The change-row frame: ``insert`` and ``update_postimage`` rows
        with the ``_change_type``/``_commit_version``/``_commit_timestamp``
        metadata columns dropped. One row per commit -- see the module
        docstring for the change-volume semantics.

    Raises:
        ConfigurationError: On invalid version arguments, a blank table
            reference, or a table/window that does not expose change
            data (with the enablement remediation).
    """
    if (
        not isinstance(starting_version, int)
        or isinstance(starting_version, bool)
        or starting_version < 0
    ):
        raise ConfigurationError(
            f"starting_version must be a non-negative integer, got {starting_version!r}"
        )
    if ending_version is not None and (
        not isinstance(ending_version, int)
        or isinstance(ending_version, bool)
        or ending_version < starting_version
    ):
        raise ConfigurationError(
            f"ending_version must be an integer >= starting_version "
            f"({starting_version}), got {ending_version!r}"
        )
    if not isinstance(table_ref, str) or not table_ref.strip():
        raise ConfigurationError("Delta table reference must not be blank")

    reader = spark.read.option("readChangeFeed", "true").option(
        "startingVersion", str(starting_version)
    )
    if ending_version is not None:
        reader = reader.option("endingVersion", str(ending_version))
    try:
        changes = reader.table(table_ref)
        return changes.filter(changes["_change_type"].isin(_CHANGE_ROW_TYPES)).drop(
            *_CHANGE_METADATA_COLUMNS
        )
    except Exception as exc:  # translated at read or scan time
        translated = _as_cdf_configuration_error(exc)
        if translated is not None:
            raise translated from exc
        raise


def execute_cdf_plan(plan: ExecutionPlan, changes):
    """Evaluate a portable plan against a Delta change batch.

    Args:
        plan: Single-dataset ExecutionPlan (counts/v1, groups/v1, or
            ranges/v1 semantics).
        changes: The change-row frame (from :func:`read_changes` or an
            equivalent source).

    Returns:
        Normalized CheckOutcome tuple from ``plan.evaluate``, with the
        adapter stamped as ``spark-cdf``.

    Raises:
        ConfigurationError: If the plan spans more than one dataset
            (CDF validation is per-table), the frame is not a Spark
            DataFrame, the capabilities do not cover the plan, or a
            Delta change-feed error surfaces during metric execution.
    """
    dataset_names = {metric.dataset.name for metric in plan.metrics}
    if len(dataset_names) != 1:
        raise ConfigurationError(
            "a CDF-validated plan must target exactly one dataset, got "
            f"{sorted(dataset_names)}"
        )
    from pyspark.sql import DataFrame

    if not isinstance(changes, DataFrame):
        raise ConfigurationError(
            "changes must be a Spark DataFrame (from read_changes or an "
            f"equivalent source), got {type(changes).__name__}"
        )
    capabilities = capabilities_for(plan)
    plan.validate_for(capabilities)
    (dataset_name,) = dataset_names
    try:
        values = compute_plan_values(plan, {dataset_name: changes})
    except Exception as exc:  # scan-time Delta errors surface at actions
        translated = _as_cdf_configuration_error(exc)
        if translated is not None:
            raise translated from exc
        raise
    return plan.evaluate(values, capabilities)
