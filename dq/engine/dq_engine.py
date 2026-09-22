# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from pyhocon import ConfigTree
from pyspark.sql import DataFrame


class DQEngine(ABC):
    """Abstract base class for all data quality engines.

    Engines implement validation logic by overriding the ``apply`` method.
    They are loaded dynamically by ``EngineLoader`` based on the engine name
    in the HOCON configuration.
    """

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        self._config = config
        self._dqts = dqts

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
