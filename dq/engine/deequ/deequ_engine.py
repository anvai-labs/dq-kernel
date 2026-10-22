# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional, List, Dict, Any

from pyhocon import ConfigTree
from pyspark.sql import DataFrame
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.repository import FileSystemMetricsRepository

from dq.engine.dq_engine import DQEngine
from dq.engine.deequ.deequ_check import DeequCheck
from dq.utils import constants

logger = logging.getLogger(__name__)


class DeequEngine(DQEngine):
    """Engine that validates DataFrames using Amazon Deequ.

    Wraps PyDeequ's ``VerificationSuite`` to run constraint checks
    defined in the HOCON configuration.  Supports both single-check
    mode (all constraints in one ``Check``) and multi-check mode
    (one ``Check`` per constraint).

    ``apply`` is a template method: subclasses customize verification by
    overriding ``_run_verification`` while sharing logging, persistence,
    and summary conversion.
    """

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        self._sparkSession = None
        super().__init__(config, dqts)

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Run Deequ verification checks against the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository config for persisting metrics.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        self._log_processing_start()
        self._sparkSession = dataframe.sparkSession
        successMetrics, checkVerifications = self._run_verification(dataframe)
        self._persist(repository, successMetrics, checkVerifications)

        summarymetrics = []
        for check in checkVerifications.collect():
            summarymetrics.append(
                {
                    "check": check["check"],
                    "success": check["check_status"] == "Success",
                    "details": check.asDict(recursive=True),
                }
            )

        return summarymetrics

    def _run_verification(self, dataframe: DataFrame):
        """Build and run the Deequ ``VerificationSuite`` for configured checks.

        Args:
            dataframe: Spark DataFrame to validate.

        Returns:
            Tuple of (successMetrics DataFrame, checkVerifications DataFrame).
        """
        deequ_check = DeequCheck(
            checks_config=self._config.get("checks", []),
            single_check_mode=self._config.get(constants.DQ_SINGLE_CHECK_MODE, False),
        )

        verification_run_builder = VerificationSuite(
            spark_session=self._sparkSession
        ).onData(df=dataframe)

        verification_run_builder = deequ_check.apply_checks(
            verification_run_builder, self._sparkSession
        )

        verification_result = verification_run_builder.run()

        if verification_result.status == "SUCCESS":
            logger.info("Data quality checks passed successfully.")
        else:
            logger.warning("Data quality checks failed.")

        successMetrics = VerificationResult.successMetricsAsDataFrame(
            spark_session=self._sparkSession,
            verificationResult=verification_result,
        )
        checkVerifications = VerificationResult.checkResultsAsDataFrame(
            spark_session=self._sparkSession,
            verificationResult=verification_result,
        )
        return successMetrics, checkVerifications
