# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional, List, Dict, Any

from dq.engine.dq_engine import DQEngine
from pyhocon import ConfigTree
from pyspark.sql import DataFrame
from dq.engine.greatexpectations.greatexpectations_check import GreatexpectationsCheck
from dq.utils import constants

logger = logging.getLogger(__name__)


class GreatexpectationsEngine(DQEngine):
    """Engine that validates DataFrames using Great Expectations.

    Wraps a GE ``SparkDFDataset`` and dynamically applies expectations
    defined in the HOCON configuration.
    """

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        super().__init__(config, dqts)

    def apply(self, df: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply Great Expectations checks to the given DataFrame.

        Args:
            df: Spark DataFrame to validate.
            repository: Optional repository config (unused).

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        try:
            import great_expectations as ge
        except ImportError as exc:
            raise ImportError(
                "The Great Expectations engine requires the optional "
                "great_expectations package."
            ) from exc

        rule_name = self._config.get(constants.DQ_RULE_NAME, "Unknown")
        engine_name = self._config.get(constants.DQ_ENGINE_NAME, "Unknown")
        logger.info("Processing %s with %s Engine", rule_name, engine_name)

        ge_df = ge.dataset.SparkDFDataset(df)
        ge_checks = GreatexpectationsCheck(self._config.get("expectations", []))

        ge_checks.apply_checks(ge_df)

        validation_output = ge_df.validate()
        metrics = self._extract_metrics_from_validation_output(validation_output)
        return metrics

    def _extract_metrics_from_validation_output(
        self, validation_output
    ) -> List[Dict[str, Any]]:
        """Extract metric dictionaries from GE validation output."""
        summarymetrics = []
        for exp_result in validation_output.results:
            summarymetrics.append(
                {
                    "check": exp_result.expectation_config.expectation_type,
                    "success": exp_result.success,
                    "details": exp_result.result,
                }
            )
        return summarymetrics
