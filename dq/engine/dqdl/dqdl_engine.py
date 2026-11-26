# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Compatibility engine for Deequ's Data Quality Definition Language."""

import logging
from typing import Any, Dict, List, Optional

from pydeequ.dqdl import EvaluateDataQuality
from pyhocon import ConfigTree

from dq.engine.checkspec import build_result
from dq.engine.dq_engine import DQEngine
from dq.exceptions import ConfigurationError, ValidationError

logger = logging.getLogger(__name__)


class DqdlEngine(DQEngine):
    """Evaluate a vendor-specific DQDL ruleset through PyDeequ.

    This adapter intentionally returns only one bounded summary per rule. PyDeequ's
    row-level DQDL output remains an explicit lower-level API because collecting it at
    the framework boundary could exhaust driver memory.
    """

    #: DQDL rule evaluation returns bounded outcome dicts only; no vendor
    #: artifact frames exist to persist.
    CAPABILITIES = {
        "criticality": "absent",
        "row_filter": False,
        "failed_rows": False,
        "persistence": False,
    }

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        super().__init__(config, dqts)

    def _execute(self, dataframe, prepared) -> List[Dict[str, Any]]:
        """Evaluate the DQDL ruleset with bounded driver collection."""
        ruleset = self._config.get("ruleset", None)
        if not isinstance(ruleset, str) or not ruleset.strip():
            raise ConfigurationError("DQDL engine requires a non-empty 'ruleset'")

        max_outcomes = self._config.get("max_outcomes", 1000)
        if type(max_outcomes) is not int or not 1 <= max_outcomes <= 10000:
            raise ConfigurationError(
                "DQDL max_outcomes must be an integer from 1 to 10000"
            )

        outcomes = EvaluateDataQuality.process(
            dataframe.sparkSession, dataframe, ruleset
        )
        metrics = []
        rows = outcomes.limit(max_outcomes + 1).collect()
        if not rows:
            raise ValidationError("DQDL emitted zero rule outcomes")
        if len(rows) > max_outcomes:
            raise ValidationError("DQDL rule outcomes exceed max_outcomes")
        for row in rows:
            details = row.asDict(recursive=True)
            outcome = details.get("Outcome")
            if outcome not in ("Passed", "Failed"):
                raise ValidationError(f"Unknown DQDL outcome: {outcome!r}")
            rule = details.get("Rule") or details.get("EvaluatedRule")
            if not isinstance(rule, str) or not rule.strip():
                raise ValidationError("DQDL outcome has no rule identity")
            metrics.append(build_result(rule, outcome == "Passed", details))

        logger.info("DQDL emitted %d bounded rule outcomes", len(metrics))
        return metrics
