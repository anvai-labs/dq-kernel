# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Optional, List, Dict

from dq.engine.dq_engine import DQEngine
from dq.engine.deequ.verification import (
    metrics_from_check_results,
    run_verification,
)
from pyhocon import ConfigTree
from pyspark.sql import DataFrame

from dq.utils import keys
from dq.engine.schemavalidation.schemavalidation_check import SchemavalidationCheck

logger = logging.getLogger(__name__)


class SchemavalidationEngine(DQEngine):
    """Engine that validates DataFrame schemas against expected definitions.

    Checks datatype, nullable, unique, and foreign-key constraints using
    Spark schema introspection.  Supports Unity Catalog, Glue, Hive, and
    default Spark catalog types.

    Belongs to the deequ engine family (shared pydeequ scaffold and raw
    vendor artifact shapes) without inheriting the deequ engine itself:
    composition through ``dq.engine.deequ.verification`` keeps the two
    engines independently evolvable.
    """

    #: Severity is native (pydeequ CheckLevel); no row scoping or counts.
    CAPABILITIES = {
        "criticality": "native",
        "row_filter": False,
        "failed_rows": False,
        "persistence": True,
    }

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        self._sparkSession = None
        self._last_verification_artifacts = None
        self._last_schema_check = None
        super().__init__(config, dqts)

    def _prepare(self, dataframe) -> None:
        """Bind the run's Spark session."""
        self._sparkSession = dataframe.sparkSession
        return None

    def _execute(self, dataframe, prepared) -> List[Dict[str, Any]]:
        """Run schema validation and shape check-result rows."""
        successMetrics, checkVerifications = self.validate_dataframe(dataframe)
        # Raw pydeequ frames stashed for _persist_outcomes (deequ-family
        # artifact schema, deliberately unchanged).
        self._last_verification_artifacts = (successMetrics, checkVerifications)
        return metrics_from_check_results(
            checkVerifications.collect(), self._last_schema_check.labels_by_check()
        )

    def validate_dataframe(self, df: DataFrame):
        """Validate the DataFrame against schema and additional constraints.

        Args:
            df: Spark DataFrame to validate.

        Returns:
            Tuple of (successMetrics DataFrame, checkVerifications DataFrame).
        """
        catalog_type = self._config.get(
            f"{keys.SCHEMA_VALIDATION_SCHEMA}.catalog_type", None
        )
        single_check_mode = self._config.get(keys.DQ_SINGLE_CHECK_MODE, True)
        schema_validation_check = self._last_schema_check = SchemavalidationCheck(
            schema_config=self._config.get(keys.SCHEMA_VALIDATION_SCHEMA, {}),
            single_check_mode=single_check_mode,
        )

        checks = schema_validation_check.apply_checks(df)
        joined_df = (
            schema_validation_check.get_joined_dataframe_from_foreign_key_constraints(
                df
            )
        )

        def _add_checks(builder):
            if single_check_mode:
                return builder.addCheck(checks)
            for check in checks:
                builder = builder.addCheck(check)
            return builder

        return run_verification(self._sparkSession, joined_df, _add_checks)

    def _persist_outcomes(self, repository, metrics, dataframe) -> None:
        """Persist raw pydeequ artifact frames (deequ-family schema)."""
        if not repository or self._last_verification_artifacts is None:
            return
        self._persist(repository, *self._last_verification_artifacts)
