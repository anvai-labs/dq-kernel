# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Harvest Lakeflow Declarative Pipeline expectation metrics as outcomes.

Lakeflow pipelines enforce row-level expectations natively and record
per-expectation pass/drop counts in the pipeline event log -- for
pipelines with a configured Unity Catalog event log this is a plain
Delta table; otherwise the managed event log is queried through the
``event_log('<pipeline_id>')`` table-valued function (the ``TABLE()``
form returns a *table's* lineage log and does not carry pipeline
expectation metrics). This harvester reads those events and normalizes
them into the framework's legacy outcome dicts, so pipeline-native
quality joins the same report/sink/notifier evidence stream as every
engine.

Semantics and limitations: ``flow_progress`` metrics are cumulative
per update, so by default only the latest emission per
(dataset, expectation) in the window becomes an outcome (``keep="all"``
returns every emission as raw evidence); an empty window returns an
empty list -- callers should treat that as "no events", not as a
zero-check pass, since ``normalize_outcomes`` rejects empty batches.
``FAIL UPDATE`` expectations halt the update before aggregate metrics
are emitted; temporary views may compute metrics at query time or
duplicate them across consumers; the table-valued function requires
the pipeline owner or a published view over ``event_log()``.
"""

import datetime
import json
from typing import Dict, List, Optional

from databricks.sdk import WorkspaceClient

from dq.engine.databricks.workspace_auth import (
    validate_token,
    validate_workspace_url,
)

_EVENT_TYPE = "flow_progress"


def expectation_outcomes(
    event_rows, *, fail_on_drops: bool = True, keep: str = "latest"
) -> List[dict]:
    """Normalize flow_progress event rows into legacy outcome dicts.

    Args:
        event_rows: Iterable of mappings with ``timestamp`` and
            ``details`` keys, where ``details`` is (or parses as) the
            event-log ``flow_progress`` payload. Timestamps may be epoch
            millis or datetimes; they are coerced to epoch millis.
        fail_on_drops: When True (default) an outcome succeeds only if
            the expectation dropped zero records; when False drop
            counts are recorded as evidence without failing the check.
        keep: ``"latest"`` (default) keeps only the newest emission per
            (dataset, expectation) in the window; ``"all"`` keeps every
            emission as separate raw-evidence outcomes.

    Returns:
        Legacy metric dicts (``check``, ``success``, ``details``) ready
        for ``normalize_outcomes`` / ``build_report`` /
        ``GateEvent.from_outcomes`` (the caller handles empty results).

    Raises:
        ValueError: If an event row or expectation entry is malformed
            (unparseable details, non-object payloads, missing names,
            non-numeric or boolean counts); the error names the row
            origin for triage.
    """
    if keep not in ("latest", "all"):
        raise ValueError(f"keep must be 'latest' or 'all', got {keep!r}")
    collected: Dict[tuple, int] = {}
    ordered: List[dict] = []
    for row in event_rows:
        timestamp = _epoch_millis(row.get("timestamp"))
        details = row.get("details")
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"flow_progress event at {timestamp!r} has "
                    f"unparseable details: {exc}"
                ) from exc
        progress = _require_object(details, "flow_progress", timestamp)
        outputs = _require_object(progress, "outputs", timestamp)
        for dataset, output in outputs.items():
            quality = _require_object(output, "data_quality", timestamp, dataset)
            expectations = _require(quality, "expectations", timestamp, dataset)
            if not isinstance(expectations, list):
                raise ValueError(
                    f"flow_progress event at {timestamp!r} dataset "
                    f"{dataset!r} has non-list expectations"
                )
            for expectation in expectations:
                name = _require(expectation, "name", timestamp, dataset)
                passed = _number(
                    expectation, "passed_records", timestamp, dataset, name
                )
                dropped = _number(
                    expectation, "dropped_records", timestamp, dataset, name
                )
                outcome = {
                    "check": f"{dataset}.{name}",
                    "success": dropped == 0 if fail_on_drops else True,
                    "details": {
                        "dataset": dataset,
                        "expectation": name,
                        "passed_records": passed,
                        "dropped_records": dropped,
                        "timestamp": timestamp,
                    },
                }
                for source_key in ("batch_id", "update_id"):
                    if isinstance(expectation, dict) and source_key in expectation:
                        outcome["details"][source_key] = expectation[source_key]
                key = (dataset, name)
                if keep == "latest":
                    if key in collected:
                        ordered[collected[key]] = outcome
                    else:
                        collected[key] = len(ordered)
                        ordered.append(outcome)
                else:
                    ordered.append(outcome)
    return ordered


def _epoch_millis(value):
    if isinstance(value, datetime.datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, str):
        return value
    return str(value)


def _require(payload, key, timestamp, dataset=None):
    if not isinstance(payload, dict):
        raise ValueError(
            f"flow_progress event at {timestamp!r} has a non-object payload"
        )
    value = payload.get(key)
    if value is None:
        origin = f"{dataset}.{key}" if dataset else key
        raise ValueError(f"flow_progress event at {timestamp!r} is missing {origin!r}")
    return value


def _require_object(payload, key, timestamp, dataset=None):
    value = _require(payload, key, timestamp, dataset)
    if not isinstance(value, dict):
        origin = f"{dataset}.{key}" if dataset else key
        raise ValueError(
            f"flow_progress event at {timestamp!r} has non-object {origin!r}"
        )
    return value


def _number(payload, key, timestamp, dataset, name):
    value = _require(payload, key, timestamp, f"{dataset}.{name}")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            f"flow_progress event at {timestamp!r} has "
            f"non-numeric {dataset}.{name}.{key}: {value!r}"
        )
    return value


class PipelineExpectationsHarvester:
    """Reads Lakeflow pipeline expectation metrics as framework outcomes.

    Construction mirrors :class:`DatabricksCheck`: prefer unified auth
    (no arguments) so the SDK resolves OAuth / workload identity; the
    explicit token and URL remain for backwards compatibility, and a
    client can be injected for tests. This module imports neither
    pyspark nor cstriggers -- the SparkSession is only used, duck-typed,
    to query the event log.
    """

    def __init__(
        self, databricks_token=None, databricks_url=None, workspace_client=None
    ):
        if workspace_client is not None:
            if databricks_token is not None or databricks_url is not None:
                raise ValueError(
                    "workspace_client cannot be combined with explicit credentials"
                )
            self._workspace_client = workspace_client
            return
        options = {}
        if databricks_url is not None:
            options["host"] = validate_workspace_url(databricks_url)
        if databricks_token is not None:
            options["token"] = validate_token(databricks_token)
        self._workspace_client = WorkspaceClient(**options)

    def harvest(
        self,
        spark,
        table_ref: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        since_ms: Optional[int] = None,
        keep: str = "latest",
    ) -> List[dict]:
        """Harvest expectation outcomes for one Lakeflow pipeline.

        Args:
            spark: Active SparkSession used only to query the event log
                (duck-typed; never used to run validations).
            table_ref: Direct reference to a published or configured
                event-log Delta table (``catalog.schema.table``);
                queried with a plain SELECT.
            pipeline_id: Pipeline UUID. When the pipeline has a
                configured Unity Catalog event log it is resolved and
                read directly; otherwise the managed event log is
                queried through ``event_log('<pipeline_id>')``.
            since_ms: Only harvest ``flow_progress`` events at or after
                this epoch-millisecond timestamp.
            keep: Passed through to :func:`expectation_outcomes`.

        Returns:
            Legacy outcome dicts from :func:`expectation_outcomes`;
            empty when the window contains no expectation-bearing
            events (callers gate on emptiness before
            ``normalize_outcomes``, which rejects empty batches).

        Raises:
            ValueError: If neither/both of ``table_ref`` and
                ``pipeline_id`` are given, the reference is blank, or
                the pipeline exposes neither a configured nor a
                queryable event log.
        """
        if (table_ref is None) == (pipeline_id is None):
            raise ValueError("exactly one of table_ref or pipeline_id must be provided")
        query = None
        if table_ref is not None:
            if not isinstance(table_ref, str) or not table_ref.strip():
                raise ValueError("event log table reference must not be blank")
            query = self._select_from(table_ref, since_ms)
        else:
            resolved = self._resolve_event_log_table(pipeline_id)
            if resolved is not None:
                query = self._select_from(resolved, since_ms)
            else:
                query = (
                    f"SELECT timestamp, details FROM event_log('{pipeline_id}') "
                    f"WHERE event_type = '{_EVENT_TYPE}'"
                )
                if since_ms is not None:
                    query += f" AND timestamp >= timestamp_millis({int(since_ms)})"
        rows = [
            {"timestamp": row["timestamp"], "details": row["details"]}
            for row in spark.sql(query).collect()
        ]
        return expectation_outcomes(rows, keep=keep)

    @staticmethod
    def _select_from(table_ref: str, since_ms: Optional[int]) -> str:
        query = (
            f"SELECT timestamp, details FROM {table_ref} "
            f"WHERE event_type = '{_EVENT_TYPE}'"
        )
        if since_ms is not None:
            query += f" AND timestamp >= timestamp_millis({int(since_ms)})"
        return query

    def _resolve_event_log_table(self, pipeline_id: str) -> Optional[str]:
        """Return the configured event-log table ref, or None for managed."""
        response = self._workspace_client.pipelines.get(pipeline_id)
        event_log = getattr(getattr(response, "spec", None), "event_log", None)
        catalog = getattr(event_log, "catalog", None)
        schema = getattr(event_log, "schema", None)
        name = getattr(event_log, "name", None)
        if not (catalog and schema and name):
            return None
        return f"{catalog}.{schema}.{name}"
