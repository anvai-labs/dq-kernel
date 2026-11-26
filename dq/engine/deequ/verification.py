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
import re
from collections.abc import Mapping, MutableMapping
from typing import Any, Callable, Dict, List, Tuple

from dq.engine import checkspec
from dq.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

#: ``checkResultsAsDataFrame`` rows: the parent Check's status, and each
#: constraint's own status. A constraint's success is its own.
PYDEEQU_CHECK_STATUS_SUCCESS = "Success"
PYDEEQU_CONSTRAINT_STATUS_SUCCESS = "Success"

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


def metrics_from_check_results(
    check_rows: List[Any], labels: Mapping[str, Any]
) -> List[Dict[str, Any]]:
    """Shape pydeequ check-result rows into legacy outcome dicts.

    Each row is the result of one *constraint*. Its own status is
    ``constraint_status``. ``check_status`` is the status of the parent
    Check, so wherever several constraints share a Check -- single-check
    mode -- reading it would report every passing constraint as failed as
    soon as one of them failed.

    Args:
        check_rows: Rows collected from ``checkResultsAsDataFrame``.
        labels: ``{Check description: [label, ...]}`` with each Check's
            constraint labels in definition order (``DeequCheck.labels_by_check``).

    Returns:
        Metric dicts built with :func:`dq.engine.checkspec.build_result`.

    Raises:
        ConfigurationError: If a result row cannot be matched to a
            constraint's label, or a labelled constraint produced no row.
            Evidence is never attributed to a label by guesswork.
    """
    if not isinstance(labels, Mapping):
        raise ConfigurationError(
            "check results need labels by Check description; build them with "
            "DeequCheck(...).labels_by_check()"
        )
    summarymetrics: List[Dict[str, Any]] = []
    seen: Dict[str, int] = {}
    for check in check_rows:
        details = check.asDict(recursive=True)
        description = details.get("check")
        index = seen.get(description, 0)
        seen[description] = index + 1
        try:
            label = labels[description][index]
        except (KeyError, IndexError, TypeError):
            raise ConfigurationError(
                f"a check result (constraint {details.get('constraint')!r}) "
                f"does not match any configured check of {description!r}; "
                "its evidence cannot be attributed"
            ) from None
        summarymetrics.append(
            checkspec.build_result(
                label,
                details.get("constraint_status") == PYDEEQU_CONSTRAINT_STATUS_SUCCESS,
                details,
                criticality=details.get("check_level"),
            )
        )
    for description, expected in labels.items():
        if seen.get(description, 0) != len(expected):
            raise ConfigurationError(
                f"{len(expected)} check(s) are labelled under {description!r} "
                f"but {seen.get(description, 0)} result row(s) came back; "
                "their evidence cannot be attributed"
            )
    return summarymetrics


# ------------------------------------------------------ labelled constraints


_COLUMN_IN_NAME = re.compile(r"\b(?:column|field)\[([^\]]+)\]")


def _columns_of(args, kwargs):
    """Return the columns a constraint call names, or ``None``."""
    columns = kwargs.get("column", None) or kwargs.get("columns", None)
    if columns is None:
        for argument in args:
            if isinstance(argument, str) or (
                isinstance(argument, (list, tuple))
                and argument
                and all(isinstance(item, str) for item in argument)
            ):
                columns = argument
                break
    if columns is None or isinstance(columns, str):
        return [columns] if columns else None
    return [str(column) for column in columns]


def constraint_label(method: str, args, kwargs, taken: List[str]) -> str:
    """Return a unique label for one constraint added to a Check.

    The constraint's method and the column it targets: ``isComplete_id``,
    ``hasUniqueness_order_id_line_id``. A column named only inside a
    constraint's own text is read from there; failing that, the text
    itself is the label; failing that, the constraint's position.

    Args:
        method: The Check method called (``isComplete``, ``satisfies``...).
        args: Its positional arguments.
        kwargs: Its keyword arguments.
        taken: Labels already used by this rule's checks.
    """
    columns = _columns_of(args, kwargs)
    if columns is None:
        named = kwargs.get("constraintName", None)
        found = _COLUMN_IN_NAME.search(str(named)) if named else None
        if found:
            columns = [found.group(1)]
        elif named:
            label = str(named)
            return label if label not in taken else f"{label}_{len(taken) + 1}"
    label = (
        "_".join([method, *columns])
        if columns
        else checkspec.derive_name(method, len(taken))
    )
    return label if label not in taken else f"{label}_{len(taken) + 1}"


class RecordingCheck:
    """A pydeequ Check that remembers a label for each constraint added.

    Every constraint-adding method returns a new Check. Wrapping that return
    value too means a chain of constraints records one label each, in
    definition order -- the order in which Deequ reports a Check's
    constraint results, so each result row can be matched to its label.

    Unwrap with :meth:`unwrap` before handing the Check to pydeequ.
    """

    def __init__(self, check, labels: List[str]):
        object.__setattr__(self, "_check", check)
        object.__setattr__(self, "_labels", labels)

    def unwrap(self):
        """Return the underlying pydeequ Check."""
        return self._check

    def __getattr__(self, name):
        attribute = getattr(self._check, name)
        if name.startswith("_") or not callable(attribute):
            return attribute

        def method(*args, **kwargs):
            result = attribute(*args, **kwargs)
            if isinstance(result, type(self._check)):
                self._labels.append(constraint_label(name, args, kwargs, self._labels))
                return RecordingCheck(result, self._labels)
            return result

        return method


def recording_check(check, store: MutableMapping[str, List[str]]) -> RecordingCheck:
    """Start recording the constraints of ``check`` under its description.

    Args:
        check: A freshly built pydeequ Check.
        store: The rule's ``{Check description: [label, ...]}`` map, which
            the labels are appended to.
    """
    return RecordingCheck(check, store.setdefault(check.description, []))
