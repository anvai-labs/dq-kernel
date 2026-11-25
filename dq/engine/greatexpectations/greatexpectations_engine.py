# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Great Expectations (GX Core) engine.

Validates Spark DataFrames against an ``expectations`` HOCON payload
through the GX Core 1.x datasource API (ephemeral context, in-memory
spark dataframe asset). The great_expectations distribution is an
optional extra; it is imported lazily so core imports stay GE-free.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from pyhocon import ConfigTree

from dq.engine.dq_engine import DQEngine
from dq.engine.greatexpectations.greatexpectations_check import (
    GreatexpectationsCheck,
)
from dq.engine.checkspec import build_result
from dq.utils import keys
from dq.utils.imports import require_extra
from dq.utils.sanitization import to_strict_json as _to_strict_json

logger = logging.getLogger(__name__)


class GreatexpectationsEngine(DQEngine):
    """Engine applying GX Core expectations to a Spark DataFrame."""

    #: GX has no per-rule severity (the kernel synthesizes it) and no
    #: per-rule result counts for all expectation kinds; row scoping is
    #: native via the ``row_condition`` constructor kwarg.
    CAPABILITIES = {
        "criticality": "derived",
        "row_filter": True,
        "failed_rows": False,
        "persistence": True,
    }

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        super().__init__(config, dqts)
        # Ephemeral context plumbing is built once per engine and reused
        # across apply() calls; building it per run is measurably costly.
        self._gx_batch_definition = None
        self._gx_suite = None
        self._gx_check = None
        self._gx_module = None

    def _prepare(self, dataframe) -> None:
        """Build the ephemeral GX context, batch definition, and suite once."""
        gx = self._import_gx()
        self._gx_module = gx
        if self._gx_batch_definition is None:
            context = gx.get_context()
            self._gx_batch_definition = (
                context.data_sources.add_spark(name="dq_kernel_gx")
                .add_dataframe_asset(name="dq_kernel_frames")
                .add_batch_definition_whole_dataframe("dq_kernel_whole_frame")
            )
        if self._gx_suite is None:
            rule_name = self._config.get(keys.DQ_RULE_NAME, "gx-rule")
            self._gx_check = GreatexpectationsCheck(
                self._config.get("expectations", []),
            )
            self._gx_suite = self._gx_check.build_suite(gx, rule_name)
        return None

    def _execute(self, dataframe, prepared) -> List[Dict[str, Any]]:
        """Validate the frame against the suite and extract metric dicts."""
        batch = self._gx_batch_definition.get_batch(
            batch_parameters={"dataframe": dataframe}
        )
        validation = batch.validate(self._gx_suite)
        return self._extract_metrics_from_validation_output(validation)

    @staticmethod
    def _import_gx():
        return require_extra(
            "great_expectations",
            "great_expectations",
            "greatexpectations",
            pin="great_expectations>=1.24,<2",
        )

    def _extract_metrics_from_validation_output(
        self, validation_output
    ) -> List[Dict[str, Any]]:
        """Extract metric dictionaries from the GX validation output.

        Results are paired with the suite's ``(label, criticality)`` pairs
        by index (GX returns one result per expectation, in order), and
        every result is coerced through the strict-JSON sanitizer (type
        normalization, depth and size truncation) so payload shape can
        never breach the outcome boundary's structural gates.
        """
        summarymetrics = []
        for (label, criticality), exp_result in zip(
            self._gx_check.labels(), validation_output.results, strict=True
        ):
            sanitized = _to_strict_json(exp_result.result)
            summarymetrics.append(
                build_result(
                    label,
                    exp_result.success,
                    json.loads(json.dumps(sanitized, allow_nan=False)),
                    criticality=criticality,
                )
            )
        return summarymetrics
