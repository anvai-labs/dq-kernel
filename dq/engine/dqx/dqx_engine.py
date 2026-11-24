# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Databricks Labs DQX engine.

Validates Spark DataFrames against Databricks Labs DQX metadata checks
(``databricks-labs-dqx``, optional extra ``dqx``). Imports are lazy so
core installs stay DQX-free.

DQX constructs its engine with a Databricks ``WorkspaceClient`` and
verifies workspace connectivity at construction time. The kernel exposes
two client modes via config:

* ``client.mode = "workspace"`` -- build a real ``WorkspaceClient``
  (optionally ``client.profile = "<profile>"``); requires a reachable
  Databricks workspace and enables DQX's workspace-backed features
  (check storage, telemetry).
* ``client.mode = "stub"`` (default) -- a local stub client: DQX's
  validation path never touches the workspace, so checks run on plain
  local Spark without a Databricks deployment. Workspace-only calls
  raise loudly if ever reached.

Results: DQX appends ``_errors``/``_warnings`` result arrays to the
DataFrame; the kernel explodes and aggregates them into one metric per
configured check ({``check``, ``success``, ``details`` with
``failed_rows`` and capped ``sample_messages``}). A check that passes on
every row produces no result entries, so absence of a failure row means
success.
"""

import logging
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from pyhocon import ConfigTree
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq.engine.checkspec import build_result
from dq.engine.dq_engine import DQEngine
from dq.engine.dqx.dqx_check import DqxCheck
from dq.utils.imports import require_extra

logger = logging.getLogger(__name__)

# Per-check diagnostic cap: sample messages are truncated and bounded so
# the aggregated details payload stays well inside the outcome boundary's
# per-outcome byte budget even for wide failures.
_MAX_SAMPLE_MESSAGES = 5
_MAX_MESSAGE_LENGTH = 1024

_MODE_KEY = "client.mode"
_PROFILE_KEY = "client.profile"
_MODE_STUB = "stub"
_MODE_WORKSPACE = "workspace"


class _StubWorkspaceClient:
    """WorkspaceClient stand-in for workspace-free DQX runs.

    Satisfies exactly what ``DQEngineBase._verify_workspace_client``
    touches at construction: ``config._product_info`` (pre-set so no
    write-back happens) and ``clusters.select_spark_version``. Any
    workspace-backed call later raises ``AttributeError`` loudly instead
    of silently succeeding.
    """

    def __init__(self, product_info):
        self.config = SimpleNamespace(_product_info=product_info)
        self.clusters = SimpleNamespace(
            select_spark_version=lambda: "kernel-stub-local"
        )


class DqxEngine(DQEngine):
    """Engine applying Databricks Labs DQX metadata checks to a DataFrame."""

    #: DQX natively routes per-rule severity (error rows vs warn rows) and
    #: scopes rules with row filters; the kernel aggregates failed-row counts.
    CAPABILITIES = {
        "criticality": "native",
        "row_filter": True,
        "failed_rows": True,
    }

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        super().__init__(config, dqts)
        # Built lazily on first apply() and reused across calls, mirroring
        # the GX engine's batch-definition/suite caching.
        self._dqx: Optional[SimpleNamespace] = None
        self._dqx_engine = None
        self._checks: Optional[List[Dict[str, Any]]] = None
        self._check_index: Dict[str, tuple] = {}

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply the configured DQX checks to the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional ``OutcomeSink``; both artifacts of the
                run are persisted under one millisecond key when given.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details``
            keys -- one per configured check, in configuration order.

        Raises:
            ImportError: If the optional databricks-labs-dqx package is
                not installed.
            ValueError: If checks fail kernel-side or DQX-side validation.
        """
        self._import_dqx()
        self._log_processing_start()

        if self._dqx_engine is None:
            self._dqx_engine = self._build_dqx_engine(dataframe)
        if self._checks is None:
            self._build_checks()

        result_df = self._dqx_engine.apply_checks_by_metadata(dataframe, self._checks)
        failure_rows = self._aggregate_failures(result_df)
        metrics = self._metrics_from_failures(failure_rows)

        if repository:
            metrics_df, verifications_df = self._artifact_frames(metrics, dataframe)
            self._persist(repository, metrics_df, verifications_df)
        return metrics

    def _import_dqx(self) -> None:
        """Import the optional DQX distribution lazily, once.

        Raises:
            ImportError: With install instructions when the package is
                missing.
        """
        if self._dqx is not None:
            return
        require_extra(
            "databricks.labs.dqx.engine",
            "databricks-labs-dqx",
            "dqx",
            pin="databricks-labs-dqx>=0.17.2,<0.18",
            note="DQX 0.17.x requires Python <3.13 and pins databricks-sdk"
            "~=0.73, which is incompatible with the databricks extra.",
        )
        try:
            from databricks.labs.dqx import __version__ as dqx_version_module
            from databricks.labs.dqx import engine as dqx_engine_module
            from databricks.labs.dqx.reporting_columns import DefaultColumnNames
        except ImportError as exc:
            raise ImportError(
                "The dqx engine requires the optional databricks-labs-dqx "
                "package (installed distribution is incomplete or skewed; "
                "reinstall with `poetry install -E dqx`)."
            ) from exc
        self._dqx = SimpleNamespace(
            DQEngine=dqx_engine_module.DQEngine,
            DefaultColumnNames=DefaultColumnNames,
            version=getattr(dqx_version_module, "__version__", "unknown"),
        )

    def _build_dqx_engine(self, dataframe: DataFrame):
        """Instantiate the DQX engine with the configured client mode.

        Args:
            dataframe: Spark DataFrame providing the SparkSession DQX
                should bind to (avoids DQX's global session builder).

        Returns:
            A ``databricks.labs.dqx.engine.DQEngine`` bound to the frame's
            session and the configured workspace client.
        """
        mode = self._config.get(_MODE_KEY, _MODE_STUB)
        if mode == _MODE_WORKSPACE:
            from databricks.sdk import WorkspaceClient

            profile = self._config.get(_PROFILE_KEY, None)
            client = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
        elif mode == _MODE_STUB:
            client = _StubWorkspaceClient(("dqx", self._dqx.version))
        else:
            raise ValueError(
                f"Unsupported dqx client mode {mode!r}: use {_MODE_STUB!r} "
                f"or {_MODE_WORKSPACE!r}"
            )
        logger.debug(
            "DQX engine bound with client mode %s (version %s)",
            mode,
            self._dqx.version,
        )
        return self._dqx.DQEngine(client, spark=dataframe.sparkSession)

    def _build_checks(self) -> None:
        """Build and validate the metadata checks exactly once.

        Kernel-side guarantees come from ``DqxCheck``; DQX-side structural
        validation (function registry, argument types) runs before any
        data flows so a broken rule fails closed.

        Raises:
            ValueError: When checks are structurally invalid.
        """
        self._checks = DqxCheck(self._config).build_checks()
        self._check_index = {
            check["name"]: (check["check"]["function"], check["criticality"])
            for check in self._checks
        }
        status = self._dqx_engine.validate_checks(self._checks)
        if status.has_errors:
            raise ValueError(
                "dqx checks failed DQX structural validation: "
                + "; ".join(status.errors[:10])
            )

    def _aggregate_failures(self, result_df: DataFrame) -> List[Dict[str, Any]]:
        """Explode and aggregate DQX result arrays into failure rows.

        Args:
            result_df: DataFrame returned by DQX's ``apply_checks_by_metadata``
                with the configured error/warning result arrays appended.

        Returns:
            One dict per (check name, criticality) that failed at least one
            row: ``name``, ``criticality``, ``function``, ``failed_rows``,
            and ``sample_messages`` (capped).
        """
        # The adapter never overrides DQX's ExtraParams, so the result
        # columns are always the documented defaults.
        errors_col = self._dqx.DefaultColumnNames.ERRORS.value
        warnings_col = self._dqx.DefaultColumnNames.WARNINGS.value

        def _entries(column: str, criticality: str):
            return result_df.select(F.explode(F.col(column)).alias("e")).select(
                F.col("e.name").alias("name"),
                F.col("e.function").alias("function"),
                F.col("e.message").alias("message"),
                F.lit(criticality).alias("criticality"),
            )

        failures = (
            _entries(errors_col, "error")
            .unionByName(_entries(warnings_col, "warn"))
            .groupBy("name", "criticality")
            .agg(
                F.first("function", ignorenulls=True).alias("function"),
                F.count(F.lit(1)).alias("failed_rows"),
                F.slice(F.collect_set("message"), 1, _MAX_SAMPLE_MESSAGES).alias(
                    "sample_messages"
                ),
            )
            .collect()
        )
        return [row.asDict() for row in failures]

    def _metrics_from_failures(
        self, failure_rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Join aggregated failures back to configured checks.

        Pure (no Spark), so the join semantics are unit-testable: a check
        with no failure row is a success; details are strict-JSON native
        with capped sample messages.

        Args:
            failure_rows: Aggregated failure dicts from
                ``_aggregate_failures``.

        Returns:
            Metric dicts in configuration order.
        """
        by_key = {
            (row.get("name"), row.get("criticality")): row for row in failure_rows
        }
        metrics: List[Dict[str, Any]] = []
        for name, (function, criticality) in self._check_index.items():
            failure = by_key.get((name, criticality))
            if failure is None:
                metrics.append(
                    build_result(
                        name,
                        True,
                        {
                            "function": function,
                            "criticality": criticality,
                            "failed_rows": 0,
                            "sample_messages": [],
                        },
                    )
                )
                continue
            metrics.append(
                build_result(
                    name,
                    False,
                    {
                        "function": str(failure.get("function") or function),
                        "criticality": criticality,
                        "failed_rows": int(failure.get("failed_rows") or 0),
                        "sample_messages": sorted(
                            str(message)[:_MAX_MESSAGE_LENGTH]
                            for message in (failure.get("sample_messages") or [])
                        ),
                    },
                )
            )
        return metrics
