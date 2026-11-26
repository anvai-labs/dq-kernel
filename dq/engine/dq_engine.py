# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import time
import warnings
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from pyhocon import ConfigTree

from dq.exceptions import ConfigurationError
from dq.utils import keys

if TYPE_CHECKING:  # light-core: the engine contract imports with no extras installed
    from pyspark.sql import DataFrame

#: How an engine can report per-rule severity.
CRITICALITY_LEVELS = ("native", "derived", "absent")

#: Boolean capability flags.
FLAG_CAPABILITIES = ("row_filter", "failed_rows", "persistence")

#: Every key a capability manifest may carry.
CAPABILITY_KEYS = ("criticality",) + FLAG_CAPABILITIES


def capability_problems(capabilities) -> list:
    """Return what is wrong with a capability manifest (empty when valid).

    One definition of a valid manifest, shared by the loader (which refuses
    an engine whose manifest is invalid) and the conformance suite.

    Args:
        capabilities: The engine's ``CAPABILITIES`` attribute.

    Returns:
        Messages naming the offending key and value.
    """
    if not isinstance(capabilities, Mapping):
        return [
            "CAPABILITIES must be a mapping with the keys "
            f"{CAPABILITY_KEYS}, got {type(capabilities).__name__}"
        ]
    problems = []
    for key in sorted(capabilities):
        if key not in CAPABILITY_KEYS:
            problems.append(f"unknown capability {key!r}")
    criticality = capabilities.get("criticality", None)
    if criticality not in CRITICALITY_LEVELS:
        problems.append(
            f"criticality must be one of {CRITICALITY_LEVELS}, got {criticality!r}"
        )
    for key in FLAG_CAPABILITIES:
        value = capabilities.get(key, None)
        if type(value) is not bool:
            problems.append(f"{key} must be a boolean, got {value!r}")
    return problems


def outcome_frames(spark, outcomes):
    """Return the ``(metrics, verifications)`` frames outcomes persist as.

    The metrics frame is ``(check, success)``; the verifications frame adds
    ``details`` as JSON text. The schema is stated, never inferred, and that
    is the point. Outcome dictionaries differ in the shape of their
    ``details`` from one check to the next, and ``spark.createDataFrame``
    given a list of such dictionaries infers a map of strings and writes
    every nested value as its ``str()`` -- no error, the structure gone.
    An engine's own outcomes and an imported source's (``dq.harvest``) are
    framed here alike, so they reach the same sinks in the same shape.

    Args:
        spark: The Spark session the frames belong to.
        outcomes: Legacy outcome dictionaries: ``check``, ``success`` and
            ``details``, as :func:`dq.engine.checkspec.build_result` and
            every harvester return them.

    Returns:
        ``(metrics, verifications)`` DataFrames.
    """
    from pyspark.sql.types import BooleanType, StringType, StructField, StructType

    metrics_df = spark.createDataFrame(
        [(outcome["check"], outcome["success"]) for outcome in outcomes],
        StructType(
            [
                StructField("check", StringType()),
                StructField("success", BooleanType()),
            ]
        ),
    )
    verifications_df = spark.createDataFrame(
        [
            (
                outcome["check"],
                outcome["success"],
                json.dumps(outcome["details"], sort_keys=True),
            )
            for outcome in outcomes
        ],
        StructType(
            [
                StructField("check", StringType()),
                StructField("success", BooleanType()),
                StructField("details", StringType()),
            ]
        ),
    )
    return metrics_df, verifications_df


class DQEngine(ABC):
    """Contract for all data quality engines (ADR-012).

    Engines subclass this ABC and implement the ``_execute`` hook; the
    ``apply`` template method standardizes the run lifecycle so every
    engine gets the same log preamble, capability guard, prepare-once
    caching seam, and persistence path. Engines loaded dynamically by
    ``EngineLoader`` based on the engine name in the HOCON configuration.

    The ``dataframe`` argument is the engine-native frame object (a
    pyspark DataFrame for Spark engines, duck-typed at this boundary --
    the ABC itself does not import pyspark, so non-Spark engines may
    subclass it and receive their own frame type).
    """

    #: Capability manifest: what this engine natively supports. The
    #: template and consumers branch on capabilities, never on engine
    #: names, so new engines plug in by declaring what they can do. A
    #: manifest is checked by :func:`capability_problems` when the engine
    #: is loaded; see the "Writing an engine" guide.
    #: * ``criticality`` -- ``native`` (the engine has per-rule severity),
    #:   ``derived`` (the kernel derives it), or ``absent``. An engine that
    #:   is not ``absent`` reports a canonical ``details.criticality`` on
    #:   every result.
    #: * ``row_filter`` / ``failed_rows`` -- boolean support flags.
    #: * ``persistence`` -- whether the engine persists evidence to a
    #:   repository (``False`` engines refuse one).
    CAPABILITIES = {
        "criticality": "absent",
        "row_filter": False,
        "failed_rows": False,
        "persistence": True,
    }

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # A subclass states only what differs; the rest is inherited from
        # its parent, so a capability added here later reaches every engine.
        # Merging over the parent (not the defaults) keeps a second-level
        # subclass from silently resetting what its parent declared.
        declared = cls.__dict__.get("CAPABILITIES", {})
        if isinstance(declared, Mapping):
            parent = cls.__mro__[1].CAPABILITIES
            cls.CAPABILITIES = {**parent, **declared}
        # Fail at the definition, not at first use: a manifest nobody
        # validated would make every capability-based decision a guess.
        problems = capability_problems(cls.CAPABILITIES)
        if problems:
            raise ConfigurationError(
                f"{cls.__name__} declares an invalid CAPABILITIES manifest: "
                + "; ".join(problems)
            )
        if "apply" in cls.__dict__:
            warnings.warn(
                f"{cls.__name__} overrides DQEngine.apply; engines implement "
                "_prepare/_execute hooks instead. apply() overrides are "
                "deprecated since 0.3 and rejected by the engine contract "
                "test.",
                DeprecationWarning,
                stacklevel=2,
            )

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        self._config = config
        self._dqts = dqts

    def apply(self, dataframe, repository=None) -> List[Dict[str, Any]]:
        """Template method: guard, log, prepare, execute, persist.

        Args:
            dataframe: Engine-native frame to validate.
            repository: Optional ``OutcomeSink``; nothing is persisted
                when falsy.

        Returns:
            List of metric dictionaries with ``check``, ``success``,
            and ``details`` keys.

        Raises:
            ConfigurationError: When the engine declares
                ``persistence=False`` and a repository is configured.
        """
        if repository and not self.CAPABILITIES.get("persistence", True):
            raise ConfigurationError(
                f"{type(self).__name__} declares persistence=False; engine "
                "cannot persist evidence. Remove the repository "
                "configuration or pick a persistence-capable engine."
            )
        self._log_processing_start()
        prepared = self._prepare(dataframe)
        metrics = self._execute(dataframe, prepared)
        self._persist_outcomes(repository, metrics, dataframe)
        return metrics

    def _log_processing_start(self) -> None:
        """Log the rule/engine preamble shared by engine ``apply`` methods."""
        rule_name = self._config.get(keys.DQ_RULE_NAME, "Unknown")
        engine_name = self._config.get(keys.DQ_ENGINE_NAME, "Unknown")
        # Log under the concrete engine's module channel so per-engine
        # log routing keeps working after this shared-helper extraction.
        logging.getLogger(type(self).__module__).info(
            "Processing %s with %s Engine", rule_name, engine_name
        )

    def _prepare(self, dataframe) -> Any:
        """Optional one-time vendor plumbing before the first execution.

        Engines may cache construction on ``self`` (lazy vendor imports,
        suite/definition building) -- the framework reuses engine
        instances across apply calls, so once-per-instance caching is the
        convention. The returned object is passed to ``_execute``.

        Args:
            dataframe: The engine-native frame for this run.

        Returns:
            Anything ``_execute`` needs; ``None`` by default.
        """
        return None

    @abstractmethod
    def _execute(self, dataframe, prepared) -> List[Dict[str, Any]]:
        """Run the engine's checks against the frame.

        Args:
            dataframe: The engine-native frame for this run.
            prepared: Whatever ``_prepare`` returned.

        Returns:
            Metric dicts built with :func:`dq.engine.checkspec.build_result`.
        """

    def _persist_outcomes(self, repository, metrics, dataframe) -> None:
        """Persist engine evidence under the run key.

        Default path: the standard (check, success, details-JSON) artifact
        frames via :meth:`_artifact_frames`. Engines with non-standard
        artifact shapes (raw vendor frames, native verification columns)
        override this method to keep their own -- artifact table schemas
        are downstream-consumed and are never silently reshaped.

        Args:
            repository: Optional ``OutcomeSink`` (falsy skips persistence).
            metrics: The metric dicts ``_execute`` returned.
            dataframe: The engine-native frame for this run.
        """
        if not repository:
            return
        metrics_df, verifications_df = self._artifact_frames(metrics, dataframe)
        self._persist(repository, metrics_df, verifications_df)

    def _persist(self, repository, successMetrics, checkVerifications) -> None:
        """Persist metrics and verification DataFrames under one result key.

        Args:
            repository: Optional ``OutcomeSink``; nothing is persisted when falsy.
            successMetrics: Metrics DataFrame to save under ``DQ_REPOSITORY_METRICS``.
            checkVerifications: Verification DataFrame to save under
                ``DQ_REPOSITORY_VERIFICATIONS``.
        """
        if repository:
            # One run key per run: the framework stamps every engine with the
            # run timestamp (dqts), so artifacts land under the same key as
            # run_scope(...) and the ts on each metric dict. Engines built
            # directly without a timestamp fall back to epoch millis.
            current_milli_time = (
                self._dqts if self._dqts is not None else int(time.time() * 1000)
            )
            repository.save(
                successMetrics,
                keys.DQ_REPOSITORY_METRICS,
                current_milli_time,
            )
            repository.save(
                checkVerifications,
                keys.DQ_REPOSITORY_VERIFICATIONS,
                current_milli_time,
            )

    @staticmethod
    def _artifact_frames(metrics, dataframe):
        """Build the run's metrics and verifications artifact DataFrames.

        The standard (check, success) and (check, success, details-JSON)
        pair of frames; see :func:`outcome_frames`.
        """
        return outcome_frames(dataframe.sparkSession, metrics)
