# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
import re
from typing import Optional, List, Dict, Any

from dq.engine import checkspec
from dq.engine.checkspec import build_result
from dq.engine.dq_engine import DQEngine
from dq.engine.businessrules.strategies import (
    ColumnNamesInReferenceTableStrategy,
    ConsecutivePercentChangeStrategy,
    GroupedDistinctBoundsStrategy,
    NoNegativeValuesStrategy,
)

logger = logging.getLogger(__name__)


def _outcome_label(constraint: str, instance) -> str:
    """Name one outcome of a constraint that reports one row per instance.

    An instance that already names its constraint (``NoNegativeValues for
    amount``) is the label. A bare one (a column) becomes
    ``GroupedDistinctBounds[id]``. The legacy instance strings end in the
    engine's own ``None`` placeholder when no source was configured; that
    stays in the evidence's details and is dropped from the label.
    """
    text = re.sub(r"\s+None$", "", str(instance).strip())
    return text if text.startswith(constraint) else f"{constraint}[{text}]"


class BusinessrulesEngine(DQEngine):
    """Engine applying named business-rule constraint strategies.

    Supports four built-in constraint types, addressed by canonical name:

    * ``GroupedDistinctBounds`` -- validates distinct counts within groups
    * ``ConsecutivePercentChange`` -- bounds the change between consecutive rows
    * ``ColumnNamesInReferenceTable`` -- checks column names against a reference table
    * ``NoNegativeValues`` -- finds negative values across all columns

    The constraint set is frozen (ADR-003): rule packs depend on these names
    and on the exact metric/verification shapes, so the engine is a permanent
    first-class adapter rather than a migration stop. The pre-rename names
    (``DistinctnessByGroup``, ``RateOfChange``,
    ``LookupBasedOnColumnNameList``, ``WideTablesNegativeValuesCheck``)
    remain accepted aliases and keep emitting their historical instance
    strings, so existing rule packs are unaffected. Each constraint's Spark
    planning lives in one strategy object; every strategy decides through
    distributed aggregations and the driver only receives bounded check
    summaries.
    """

    _STRATEGIES = {
        "GroupedDistinctBounds": GroupedDistinctBoundsStrategy(),
        "ConsecutivePercentChange": ConsecutivePercentChangeStrategy(),
        "ColumnNamesInReferenceTable": ColumnNamesInReferenceTableStrategy(),
        "NoNegativeValues": NoNegativeValuesStrategy(),
    }

    _LEGACY_ALIASES = {
        "DistinctnessByGroup": "GroupedDistinctBounds",
        "RateOfChange": "ConsecutivePercentChange",
        "LookupBasedOnColumnNameList": "ColumnNamesInReferenceTable",
        "WideTablesNegativeValuesCheck": "NoNegativeValues",
    }

    def __init__(self, config, dqts: Optional[int] = None):
        self._config = config
        self._sparkSession = None
        self._last_artifact_frames = None
        super().__init__(config, dqts)

    def _prepare(self, dataframe) -> None:
        """Bind the run's Spark session for native frame building."""
        self._sparkSession = dataframe.sparkSession
        return None

    def _execute(self, dataframe, prepared) -> List[Dict[str, Any]]:
        """Apply business-rule constraint strategies to the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            prepared: Ignored (the strategy loop reads configuration).

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        rule_checks = self._config.get("checks", {})
        _metrics_results = []
        _labels = []
        _verification_results = []
        for check_config in rule_checks:
            constraint = check_config.get("constraint", None)
            strategy = self._STRATEGIES.get(constraint)
            if strategy is not None:
                canonical = constraint
            else:
                canonical = self._LEGACY_ALIASES.get(constraint)
                if canonical is None:
                    raise ValueError(
                        f"Unsupported businessrules constraint: {constraint}"
                    )
                strategy = self._STRATEGIES[canonical]
                logger.warning(
                    "constraint %r was renamed to %r; update the configuration",
                    constraint,
                    canonical,
                )
            # The configured name is the display name: legacy aliases keep
            # their historical instance and verification strings verbatim.
            params = {"name": constraint}
            params.update(
                dq_dimension=check_config.get("dq_dimension", "Compliance"),
                # "Error" keeps check_level a StringType column; a None
                # default would infer NullType and break sink persistence
                # for rule packs that omit level.
                level=check_config.get("level", "Error"),
            )
            if canonical == "GroupedDistinctBounds":
                params.update(
                    columns=check_config.get("columns", None),
                    group_by=check_config.get("group_by", None),
                    threshold_min=check_config.get("min", None),
                    threshold_max=check_config.get("max", None),
                )
            elif canonical == "ConsecutivePercentChange":
                params.update(
                    columns=check_config.get("columns", None),
                    group_by=check_config.get("group_by", None),
                    sort_by=check_config.get("sort_by", None),
                    threshold_min=check_config.get("min", None),
                    threshold_max=check_config.get("max", None),
                )
            elif canonical == "ColumnNamesInReferenceTable":
                params.update(
                    ref_table=check_config.get("ref_table", None),
                    ref_columns=check_config.get("ref_columns", None),
                    ignore_columns=check_config.get("ignore_columns", None),
                    source=check_config.get("source", None),
                )
            elif canonical == "NoNegativeValues":
                params.update(
                    ignore_columns=check_config.get("ignore_columns", None),
                    source=check_config.get("source", None),
                )
            _results, _check_verification = strategy.apply(dataframe, **params)
            _metrics_results += _results
            _verification_results += _check_verification
            # Evidence is attributed by label. Each outcome is named for its
            # constraint, and for the instance it measured when the
            # constraint reports several (one per column).
            base = str(check_config.get("constraint_name", None) or constraint)
            for row in _results:
                _labels.append(
                    _outcome_label(base, row[1]) if len(_results) > 1 else base
                )

        df_metrics_results = self._sparkSession.createDataFrame(
            _metrics_results, ["entity", "instance", "name", "value"]
        )
        df_check_verification_results = self._sparkSession.createDataFrame(
            _verification_results,
            [
                "check",
                "check_level",
                "check_status",
                "constraint",
                "constraint_status",
                "constraint_message",
            ],
        )
        # Native verification frames stashed for _persist_outcomes: the
        # businessrules artifact schema is frozen by ADR-003 and downstream
        # consumers join on these exact columns.
        self._last_artifact_frames = (
            df_metrics_results,
            df_check_verification_results,
        )
        checkspec.require_unique_labels(_labels)
        summarymetrics = [
            build_result(
                label,
                row[3] == 1,
                {
                    "entity": row[0],
                    "instance": row[1],
                    "name": row[2],
                    "value": row[3],
                },
            )
            for label, row in zip(_labels, _metrics_results)
        ]

        return summarymetrics

    def _persist_outcomes(self, repository, metrics, dataframe) -> None:
        """Persist native businessrules artifact frames (ADR-003 schema)."""
        if not repository or self._last_artifact_frames is None:
            return
        self._persist(repository, *self._last_artifact_frames)
