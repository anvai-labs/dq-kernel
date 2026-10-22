# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from pyhocon import ConfigTree
from pyspark.sql import DataFrame

from dq.utils import constants


class DQEngine(ABC):
    """Abstract base class for all data quality engines.

    Engines implement validation logic by overriding the ``apply`` method.
    They are loaded dynamically by ``EngineLoader`` based on the engine name
    in the HOCON configuration.
    """

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        self._config = config
        self._dqts = dqts

    def _log_processing_start(self) -> None:
        """Log the rule/engine preamble shared by engine ``apply`` methods."""
        rule_name = self._config.get(constants.DQ_RULE_NAME, "Unknown")
        engine_name = self._config.get(constants.DQ_ENGINE_NAME, "Unknown")
        # Log under the concrete engine's module channel so per-engine
        # log routing keeps working after this shared-helper extraction.
        logging.getLogger(type(self).__module__).info(
            "Processing %s with %s Engine", rule_name, engine_name
        )

    def _persist(self, repository, successMetrics, checkVerifications) -> None:
        """Persist metrics and verification DataFrames under one result key.

        Args:
            repository: Optional ``OutcomeSink``; nothing is persisted when falsy.
            successMetrics: Metrics DataFrame to save under ``DQ_REPOSITORY_METRICS``.
            checkVerifications: Verification DataFrame to save under
                ``DQ_REPOSITORY_VERIFICATIONS``.
        """
        if repository:
            # Imported lazily: pydeequ is an optional extra and this base class
            # stays importable in pyspark-only installations.
            from pydeequ.repository import ResultKey

            current_milli_time = ResultKey.current_milli_time()
            repository.save(
                successMetrics,
                constants.DQ_REPOSITORY_METRICS,
                current_milli_time,
            )
            repository.save(
                checkVerifications,
                constants.DQ_REPOSITORY_VERIFICATIONS,
                current_milli_time,
            )

    @abstractmethod
    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply data quality checks to the given DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository configuration for persisting results.

        Returns:
            List of metric dictionaries with ``check``, ``success``,
            and ``details`` keys.
        """
        raise NotImplementedError("Subclasses must implement this method")
