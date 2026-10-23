# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Harvest Lakeflow Declarative Pipeline expectation metrics as outcomes.

Lakeflow pipelines enforce row-level expectations natively and record
per-expectation pass/drop counts in the pipeline event log -- a Delta
table in Unity Catalog -- under ``flow_progress`` events. This
harvester reads those events (via the ``event_log(TABLE(...))``
table-valued function or the event-log table resolved from the
pipeline's spec) and normalizes them into the framework's legacy
outcome dicts, so pipeline-native quality joins the same
report/sink/notifier evidence stream as every engine.

Limitations inherited from the platform: ``FAIL UPDATE`` expectations
halt the update before aggregate metrics are emitted; temporary views
may compute metrics at query time or duplicate them across consumers;
the table-valued function requires the pipeline owner or a published
view over ``event_log()``.
"""

import json
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_EVENT_TYPE = "flow_progress"


def expectation_outcomes(event_rows, *, fail_on_drops: bool = True) -> List[dict]:
    """Normalize flow_progress event rows into legacy outcome dicts.

    Args:
        event_rows: Iterable of mappings with ``timestamp`` and
            ``details`` keys, where ``details`` is (or parses as) the
            event-log ``flow_progress`` payload.
        fail_on_drops: When True (default) an outcome succeeds only if
            the expectation dropped zero records; when False drop
            counts are recorded as evidence without failing the check.

    Returns:
        Legacy metric dicts (``check``, ``success``, ``details``) ready
        for ``normalize_outcomes`` / ``build_report`` /
        ``GateEvent.from_outcomes``.

    Raises:
        ValueError: If an event row or expectation entry is malformed
            (missing ``flow_progress`` details or expectation name);
            the error names the row origin for triage.
    """
    outcomes: List[dict] = []
    for row in event_rows:
        details = row.get("details")
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"flow_progress event at {row.get('timestamp')!r} has "
                    f"unparseable details: {exc}"
                ) from exc
        progress = _require(details, "flow_progress", row)
        outputs = progress.get("outputs") or {}
        if not isinstance(outputs, dict):
            raise ValueError(
                f"flow_progress event at {row.get('timestamp')!r} has "
                "non-object outputs"
            )
        for dataset, output in outputs.items():
            quality = (output or {}).get("data_quality") or {}
            for expectation in quality.get("expectations") or []:
                name = _require(expectation, "name", row, dataset)
                passed = _number(expectation, "passed_records", row, dataset, name)
                dropped = _number(expectation, "dropped_records", row, dataset, name)
                outcome_details: Dict[str, Any] = {
                    "dataset": dataset,
                    "expectation": name,
                    "passed_records": passed,
                    "dropped_records": dropped,
                    "timestamp": row.get("timestamp"),
                }
                for source_key in ("batch_id", "update_id"):
                    if source_key in expectation:
                        outcome_details[source_key] = expectation[source_key]
                outcomes.append(
                    {
                        "check": f"{dataset}.{name}",
                        "success": dropped == 0 if fail_on_drops else True,
                        "details": outcome_details,
                    }
                )
    return outcomes


def _require(payload, key, row, dataset=None):
    value = (payload or {}).get(key) if isinstance(payload, dict) else None
    if value is None:
        origin = f"{dataset}.{key}" if dataset else key
        raise ValueError(
            f"flow_progress event at {row.get('timestamp')!r} is missing " f"{origin!r}"
        )
    return value


def _number(payload, key, row, dataset, name):
    value = _require(payload, key, row, f"{dataset}.{name}")
    if not isinstance(value, (int, float)):
        raise ValueError(
            f"flow_progress event at {row.get('timestamp')!r} has "
            f"non-numeric {dataset}.{name}.{key}: {value!r}"
        )
    return value


class PipelineExpectationsHarvester:
    """Reads Lakeflow pipeline expectation metrics as framework outcomes.

    Construction mirrors :class:`DatabricksCheck`: prefer unified auth
    (no arguments) so the SDK resolves OAuth / workload identity; the
    explicit token and URL remain for backwards compatibility, and a
    client can be injected for tests.
    """

    def __init__(
        self, databricks_token=None, databricks_url=None, workspace_client=None
    ):
        from dq.engine.databricks.databricks_check import DatabricksCheck

        if workspace_client is not None:
            if databricks_token is not None or databricks_url is not None:
                raise ValueError(
                    "workspace_client cannot be combined with explicit credentials"
                )
            self._workspace_client = workspace_client
            return
        options = {}
        if databricks_url is not None:
            options["host"] = DatabricksCheck._validate_workspace_url(databricks_url)
        if databricks_token is not None:
            options["token"] = DatabricksCheck._validate_token(databricks_token)
        self._workspace_client = WorkspaceClient(**options)

    def harvest(
        self,
        spark,
        table_ref: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        since_ms: Optional[int] = None,
    ) -> List[dict]:
        """Harvest expectation outcomes for one Lakeflow pipeline.

        Args:
            spark: Active SparkSession used only to query the event log
                (duck-typed; never used to run validations).
            table_ref: Explicit event-log target. Either a pipeline
                object reference (``catalog.schema.pipeline``) passed to
                the ``event_log(TABLE(...))`` function, or a direct
                ``catalog.schema.table`` reference to a published
                event-log table.
            pipeline_id: Pipeline UUID; the event-log table is resolved
                from ``w.pipelines.get(...).spec.event_log`` instead.
            since_ms: Only harvest ``flow_progress`` events at or after
                this epoch-millisecond timestamp.

        Returns:
            Legacy outcome dicts from :func:`expectation_outcomes`.

        Raises:
            ValueError: If neither/both of ``table_ref`` and
                ``pipeline_id`` are given, the reference is blank, or
                the pipeline exposes no event-log table.
        """
        if (table_ref is None) == (pipeline_id is None):
            raise ValueError("exactly one of table_ref or pipeline_id must be provided")
        if pipeline_id is not None:
            table_ref = self._resolve_event_log_table(pipeline_id)
        if not isinstance(table_ref, str) or not table_ref.strip():
            raise ValueError("event log table reference must not be blank")

        query = (
            f"SELECT timestamp, details FROM event_log(TABLE({table_ref})) "
            f"WHERE event_type = '{_EVENT_TYPE}'"
        )
        if since_ms is not None:
            query += f" AND timestamp >= timestamp_millis({int(since_ms)})"
        rows = [
            {"timestamp": row["timestamp"], "details": row["details"]}
            for row in spark.sql(query).collect()
        ]
        return expectation_outcomes(rows)

    def _resolve_event_log_table(self, pipeline_id: str) -> str:
        response = self._workspace_client.pipelines.get(pipeline_id)
        event_log = getattr(getattr(response, "spec", None), "event_log", None)
        catalog = getattr(event_log, "catalog", None)
        schema = getattr(event_log, "schema", None)
        name = getattr(event_log, "name", None)
        if not (catalog and schema and name):
            raise ValueError(
                f"pipeline {pipeline_id!r} exposes no Unity Catalog event log"
            )
        return f"{catalog}.{schema}.{name}"
