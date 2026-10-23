# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

"""Delta Change Data Feed adapter for the portable plan kernel (ADR-011 role B).

Validates the *increment*: after a Delta table refresh, read the Change
Data Feed batch for the new version range and evaluate aggregate checks
scoped to the change batch through the kernel's versioned semantics --
the aggregate/uniqueness/bounds residue that platform row-level
expectations cannot express.

V1 semantics are explicitly **delta-scoped**: row counts, completeness,
within-batch distinct bounds, and value ranges are computed over the
net-change batch only (``insert`` plus ``update_postimage`` rows;
deletes and preimages are null-filled for data columns and excluded
from column validation). Full-table semantics (uniqueness against all
history, totals) remain a future mode. Reading requires the table
property ``delta.enableChangeDataFeed=true``.

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
    _compute_values,
    CAPABILITIES as _SPARK_COUNTS_CAPABILITIES,
    GROUP_CAPABILITIES as _SPARK_GROUP_CAPABILITIES,
    RANGES_CAPABILITIES as _SPARK_RANGES_CAPABILITIES,
)

ADAPTER_NAME = "spark-cdf"
ADAPTER_VERSION = "1"

CAPABILITIES = CapabilitySet(
    ADAPTER_NAME,
    ADAPTER_VERSION,
    _SPARK_COUNTS_CAPABILITIES.metrics,
)
GROUP_CAPABILITIES = CapabilitySet(
    ADAPTER_NAME,
    ADAPTER_VERSION,
    _SPARK_GROUP_CAPABILITIES.metrics,
    SEMANTIC_GROUPS_VERSION,
)
RANGES_CAPABILITIES = CapabilitySet(
    ADAPTER_NAME,
    ADAPTER_VERSION,
    _SPARK_RANGES_CAPABILITIES.metrics,
    SEMANTIC_RANGES_VERSION,
)

_NET_CHANGE_TYPES = ("insert", "update_postimage")
_CHANGE_METADATA_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")


def _capabilities_for(plan: ExecutionPlan) -> CapabilitySet:
    if plan.semantic_version == SEMANTIC_GROUPS_VERSION:
        return GROUP_CAPABILITIES
    if plan.semantic_version == SEMANTIC_RANGES_VERSION:
        return RANGES_CAPABILITIES
    return CAPABILITIES


def read_changes(spark, table_ref, starting_version, ending_version=None):
    """Read the net-change batch of a Delta Change Data Feed window.

    Args:
        spark: Active SparkSession.
        table_ref: Delta table reference (``catalog.schema.table``).
        starting_version: First table version in the window (int >= 0).
        ending_version: Last version inclusive; the feed's latest when
            omitted.

    Returns:
        The net-change frame: ``insert`` and ``update_postimage`` rows
        with the ``_change_type``/``_commit_version``/``_commit_timestamp``
        metadata columns dropped.

    Raises:
        ConfigurationError: On invalid version arguments, or when the
            table does not expose a change data feed (enable the
            ``delta.enableChangeDataFeed`` table property).
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

    try:
        from pyspark.sql.utils import AnalysisException
    except ImportError:  # pragma: no cover - pyspark guaranteed at call time
        AnalysisException = ()

    reader = spark.read.option("readChangeFeed", "true").option(
        "startingVersion", str(starting_version)
    )
    if ending_version is not None:
        reader = reader.option("endingVersion", str(ending_version))
    try:
        changes = reader.table(table_ref)
    except AnalysisException as exc:
        message = str(exc)
        if "change data feed" in message.lower():
            raise ConfigurationError(
                f"table {table_ref!r} does not expose a change data feed; "
                "enable it with ALTER TABLE <table> SET TBLPROPERTIES "
                "('delta.enableChangeDataFeed' = true)"
            ) from exc
        raise
    return changes.filter(
        getattr(changes, "_change_type").isin(_NET_CHANGE_TYPES)
    ).drop(*_CHANGE_METADATA_COLUMNS)


def execute_cdf_plan(plan: ExecutionPlan, changes):
    """Evaluate a portable plan against a Delta net-change batch.

    Args:
        plan: Single-dataset ExecutionPlan (counts/v1, groups/v1, or
            ranges/v1 semantics).
        changes: The net-change frame (from :func:`read_changes` or an
            equivalent source).

    Returns:
        Normalized CheckOutcome tuple from ``plan.evaluate``, with the
        adapter stamped as ``spark-cdf``.

    Raises:
        ConfigurationError: If the plan spans more than one dataset
            (CDF validation is per-table) or the capabilities do not
            cover the plan.
    """
    dataset_names = {metric.dataset.name for metric in plan.metrics}
    if len(dataset_names) != 1:
        raise ConfigurationError(
            "a CDF-validated plan must target exactly one dataset, got "
            f"{sorted(dataset_names)}"
        )
    capabilities = _capabilities_for(plan)
    plan.validate_for(capabilities)
    (dataset_name,) = dataset_names
    values = _compute_values(plan, {dataset_name: changes})
    return plan.evaluate(values, capabilities)
