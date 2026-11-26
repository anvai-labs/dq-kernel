# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Optional, List, Dict

from pyhocon import ConfigTree

from dq.engine import checkspec
from dq.engine.dq_engine import DQEngine
from dq.engine.deequ.deequ_check import DeequCheck
from dq.engine.deequ.verification import (
    PYDEEQU_CHECK_STATUS_SUCCESS,
    metrics_from_check_results,
    run_verification,
)
from dq.utils import keys

logger = logging.getLogger(__name__)


class DeequEngine(DQEngine):
    """Engine that validates DataFrames using Amazon Deequ.

    Wraps PyDeequ's ``VerificationSuite`` to run constraint checks
    defined in the HOCON configuration.  Supports both single-check
    mode (all constraints in one ``Check``) and multi-check mode
    (one ``Check`` per constraint).

    Subclasses customize verification by overriding ``_run_verification``
    while sharing the artifact contract (raw pydeequ frames, unchanged
    shapes -- downstream consumers join on those columns).
    """

    #: Deequ carries per-rule severity natively (CheckLevel) and exposes it
    #: in check results; it has no per-constraint row scoping or row counts.
    CAPABILITIES = {
        "criticality": "native",
        "row_filter": False,
        "failed_rows": False,
        "persistence": True,
    }

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        self._sparkSession = None
        self._last_verification_artifacts = None
        super().__init__(config, dqts)

    def _prepare(self, dataframe) -> DeequCheck:
        """Bind the run's Spark session and build the configured checks."""
        self._sparkSession = dataframe.sparkSession
        return DeequCheck(
            checks_config=self._config.get("checks", []),
            single_check_mode=self._config.get(keys.DQ_SINGLE_CHECK_MODE, False),
        )

    def _execute(self, dataframe, prepared: DeequCheck) -> List[Dict[str, Any]]:
        """Run verification and shape check-result rows into metric dicts."""
        successMetrics, checkVerifications = self._run_verification(dataframe, prepared)
        # Raw pydeequ frames stashed for _persist_outcomes: the deequ-family
        # artifact schema (metrics/verifications vendor columns) is
        # downstream-consumed and is deliberately not reshaped here.
        self._last_verification_artifacts = (successMetrics, checkVerifications)
        return metrics_from_check_results(checkVerifications.collect())

    def _run_verification(self, dataframe, prepared: DeequCheck):
        """Build and run the Deequ ``VerificationSuite`` for configured checks.

        Args:
            dataframe: Spark DataFrame to validate.
            prepared: The ``DeequCheck`` built by ``_prepare``.

        Returns:
            Tuple of (successMetrics DataFrame, checkVerifications DataFrame).
        """
        return run_verification(
            self._sparkSession,
            dataframe,
            lambda builder: prepared.apply_checks(builder, self._sparkSession),
        )

    def _persist_outcomes(self, repository, metrics, dataframe) -> None:
        """Persist raw pydeequ artifact frames (deequ-family schema)."""
        if not repository or self._last_verification_artifacts is None:
            return
        self._persist(repository, *self._last_verification_artifacts)
