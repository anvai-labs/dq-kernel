# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared pydeequ VerificationSuite scaffold for the deequ engine family.

Deequ and schemavalidation both build ``VerificationSuite.onData(...)``,
add checks, run, and convert results to the (successMetrics,
checkVerifications) frame pair. One scaffold, one place: the two engines
cannot drift apart on suite construction, status logging, or conversion.

Vendor status spellings are named, not unified: pydeequ reports
``VerificationResult.status`` as ``"SUCCESS"`` while check-result rows
carry ``check_status == "Success"``. The row spelling is load-bearing
(metric shaping compares it), the result-status spelling guards a log
branch only -- collapsing them without a JVM-verified test would silently
break evidence shaping.
"""

import logging
from typing import Any, Callable, Dict, List, Tuple

from dq.engine import checkspec

logger = logging.getLogger(__name__)

#: ``checkResultsAsDataFrame`` rows compare ``check_status`` against this.
PYDEEQU_CHECK_STATUS_SUCCESS = "Success"

#: ``VerificationResult.status`` comparison (log branch only).
PYDEEQU_RESULT_STATUS_SUCCESS = "SUCCESS"


def run_verification(
    spark_session,
    dataframe,
    add_checks: Callable[[Any], Any],
) -> Tuple[Any, Any]:
    """Run one pydeequ verification round and convert results to frames.

    Args:
        spark_session: SparkSession the verification runs against.
        dataframe: DataFrame to verify (may be a derived/joined frame).
        add_checks: Callable receiving the ``VerificationSuite`` builder and
            returning it with checks applied (chained ``Check`` or several
            ``addCheck`` calls).

    Returns:
        ``(successMetrics, checkVerifications)`` as pydeequ DataFrames.
    """
    from pydeequ.verification import VerificationResult, VerificationSuite

    verification_run_builder = VerificationSuite(spark_session=spark_session).onData(
        df=dataframe
    )

    verification_run_builder = add_checks(verification_run_builder)

    verification_result = verification_run_builder.run()

    if verification_result.status == PYDEEQU_RESULT_STATUS_SUCCESS:
        logger.info("Data quality checks passed successfully.")
    else:
        logger.warning("Data quality checks failed.")

    successMetrics = VerificationResult.successMetricsAsDataFrame(
        spark_session=spark_session,
        verificationResult=verification_result,
    )
    checkVerifications = VerificationResult.checkResultsAsDataFrame(
        spark_session=spark_session,
        verificationResult=verification_result,
    )
    return successMetrics, checkVerifications


def metrics_from_check_results(check_rows: List[Any]) -> List[Dict[str, Any]]:
    """Shape pydeequ check-result rows into legacy outcome dicts.

    Shared by the deequ engine family (deequ, schemavalidation): every
    row carries the vendor ``check`` description, ``check_status``, and
    ``check_level`` -- severity normalizes through the shared seam.

    Args:
        check_rows: Rows collected from ``checkResultsAsDataFrame``.

    Returns:
        Metric dicts built with :func:`dq.engine.checkspec.build_result`.
    """
    summarymetrics = []
    for check in check_rows:
        details = check.asDict(recursive=True)
        summarymetrics.append(
            checkspec.build_result(
                check["check"],
                check["check_status"] == PYDEEQU_CHECK_STATUS_SUCCESS,
                details,
                criticality=details.get("check_level"),
            )
        )
    return summarymetrics
