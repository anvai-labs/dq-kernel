# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Great Expectations (GX Core) engine.

Validates Spark DataFrames against an ``expectations`` HOCON payload
through the GX Core 1.x datasource API (ephemeral context, in-memory
spark dataframe asset). The great_expectations distribution is an
optional extra; it is imported lazily so core imports stay GE-free.
"""

import datetime
import json
import logging
import math
from collections.abc import Mapping
from typing import Any, Dict, List, Optional

from pyhocon import ConfigTree
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from dq.engine.dq_engine import DQEngine
from dq.engine.greatexpectations.greatexpectations_check import (
    GreatexpectationsCheck,
)
from dq.utils import constants

logger = logging.getLogger(__name__)

# Outcome diagnostics are capped at depth 16 by dq.outcomes, and the
# framework wraps details two levels deep ({check, success, details} ->
# details) before that gate counts, so the sanitizer must stop two
# levels earlier. Strings and lists are also truncated well inside the
# 1 MiB per-outcome byte budget.
_MAX_SANITIZE_DEPTH = 14
_MAX_SANITIZE_STRING = 65_536
_MAX_SANITIZE_LIST = 2_000


def _to_strict_json(value: Any, depth: int = 0) -> Any:
    """Normalize a GX result payload to strict-JSON native types.

    GX results can carry tuples, numpy scalars, bytes, dates, non-finite
    floats, and raw row values -- every one of which the CheckOutcome
    boundary rejects. Unknown objects fall back to their string form.

    Args:
        value: Arbitrary payload fragment.
        depth: Current nesting depth (defensive recursion bound).

    Returns:
        The payload rebuilt from JSON-native types only.
    """
    if depth > _MAX_SANITIZE_DEPTH:
        return str(value)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        if len(value) <= _MAX_SANITIZE_STRING:
            return value
        return value[:_MAX_SANITIZE_STRING] + "...[truncated]"
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, bytes):
        return value[:_MAX_SANITIZE_STRING].decode("utf-8", "replace")
    if isinstance(value, Mapping):
        return {
            str(key): _to_strict_json(item, depth + 1) for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        if len(value) <= _MAX_SANITIZE_LIST:
            return [_to_strict_json(item, depth + 1) for item in value]
        truncated = [
            _to_strict_json(item, depth + 1)
            for item in list(value)[:_MAX_SANITIZE_LIST]
        ]
        return truncated + ["...[truncated]"]
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _to_strict_json(item(), depth + 1)
        except Exception:  # noqa: BLE001 - numpy-like scalar unwrap
            pass
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


class GreatexpectationsEngine(DQEngine):
    """Engine applying GX Core expectations to a Spark DataFrame."""

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        super().__init__(config, dqts)
        # Ephemeral context plumbing is built once per engine and reused
        # across apply() calls; building it per run is measurably costly.
        self._gx_batch_definition = None
        self._gx_suite = None

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply the configured expectations to the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional ``OutcomeSink``; both artifacts of the
                run are persisted under one millisecond key when given.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        gx = self._import_gx()
        self._log_processing_start()

        if self._gx_batch_definition is None:
            context = gx.get_context()
            self._gx_batch_definition = (
                context.data_sources.add_spark(name="dq_kernel_gx")
                .add_dataframe_asset(name="dq_kernel_frames")
                .add_batch_definition_whole_dataframe("dq_kernel_whole_frame")
            )
        batch = self._gx_batch_definition.get_batch(
            batch_parameters={"dataframe": dataframe}
        )

        if self._gx_suite is None:
            rule_name = self._config.get(constants.DQ_RULE_NAME, "gx-rule")
            self._gx_suite = GreatexpectationsCheck(
                self._config.get("expectations", [])
            ).build_suite(gx, rule_name)
        validation = batch.validate(self._gx_suite)

        metrics = self._extract_metrics_from_validation_output(validation)
        if repository:
            metrics_df, verifications_df = self._artifact_frames(metrics, dataframe)
            self._persist(repository, metrics_df, verifications_df)
        return metrics

    @staticmethod
    def _import_gx():
        try:
            import great_expectations as gx
        except ImportError as exc:
            raise ImportError(
                "The greatexpectations engine requires the optional "
                "great_expectations package; install it with "
                "`poetry install -E greatexpectations` (or "
                "`pip install 'great_expectations>=1.24,<2'`)."
            ) from exc
        return gx

    def _extract_metrics_from_validation_output(
        self, validation_output
    ) -> List[Dict[str, Any]]:
        """Extract metric dictionaries from the GX validation output.

        Every result is coerced through the strict-JSON sanitizer (type
        normalization, depth and size truncation) so payload shape can
        never breach the outcome boundary's structural gates.
        """
        summarymetrics = []
        for exp_result in validation_output.results:
            sanitized = _to_strict_json(exp_result.result)
            summarymetrics.append(
                {
                    "check": exp_result.expectation_config.type,
                    "success": bool(exp_result.success),
                    "details": json.loads(json.dumps(sanitized, allow_nan=False)),
                }
            )
        return summarymetrics

    @staticmethod
    def _artifact_frames(metrics, dataframe):
        """Build the run's metrics and verifications artifact DataFrames."""
        spark = dataframe.sparkSession
        metrics_df = spark.createDataFrame(
            [(metric["check"], metric["success"]) for metric in metrics],
            StructType(
                [
                    StructField("check", StringType()),
                    StructField("success", BooleanType()),
                ]
            ),
        )
        verifications_df = spark.createDataFrame(
            [
                (
                    metric["check"],
                    metric["success"],
                    json.dumps(metric["details"], sort_keys=True),
                )
                for metric in metrics
            ],
            StructType(
                [
                    StructField("check", StringType()),
                    StructField("success", BooleanType()),
                    StructField("details", StringType()),
                ]
            ),
        )
        return metrics_df, verifications_df
