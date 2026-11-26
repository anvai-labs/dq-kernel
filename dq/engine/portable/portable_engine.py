# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Portable-kernel engine: run a portable plan through the plugin path.

The engine-neutral kernel (``dq.plan`` + ``dq.portable_config`` + Spark
adapter, ADR-002) historically ran only as a call-it-yourself module API;
this engine routes the same kernel through ``EngineLoader`` so portable
plans execute inside ``DQFramework.run()`` like any other engine --
one orchestration path, two rule styles (ADR-010: new compute engines
land behind the kernel's semantic versions, not new vendor vocabularies).

The payload is the legacy constraint subset the fail-closed translator
already accepts; unsupported constraints are rejected with the
translator's own error, never translated (ADR-002: no automatic
translation). The caller certifies the dataset snapshot digest --
plans fingerprint data by SHA-256, and legacy HOCON carries none, so
the digest is a required configuration key and is never auto-derived.
"""

import logging
from typing import Any, List, Dict, Optional

from pyhocon import ConfigTree

from dq.engine.dq_engine import DQEngine
from dq.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class PortableEngine(DQEngine):
    """Engine executing portable kernel plans over Spark."""

    #: Severity is derived (kernel ``Severity`` normalized to criticality);
    #: plans carry no row filters or per-row failure counts.
    CAPABILITIES = {
        "criticality": "derived",
        "row_filter": False,
        "failed_rows": False,
        "persistence": True,
    }

    def _prepare(self, dataframe) -> Any:
        """Build the portable plan from the rule payload (fail closed).

        Required keys: ``dataset`` (logical name), ``snapshot_sha256``
        (64-hex digest the caller certifies), and a non-empty ``plan``
        list of legacy constraint dicts.
        """
        from dq.plan import DatasetRef
        from dq.portable_config import translate_plan

        dataset = self._config.get("dataset", None)
        sha = self._config.get("snapshot_sha256", None)
        if (
            not isinstance(dataset, str)
            or not dataset.strip()
            or not isinstance(sha, str)
            or not sha.strip()
        ):
            raise ConfigurationError(
                "portable engine requires 'dataset' and a caller-certified "
                "'snapshot_sha256' (64 hex chars) alongside 'plan'; the "
                "digest is never auto-derived"
            )
        checks = self._config.get("plan", None)
        if not isinstance(checks, list) or not checks:
            raise ConfigurationError(
                "portable engine requires a non-empty 'plan' list of "
                "constraint dicts"
            )
        self._dataset_name = dataset
        return translate_plan(checks, DatasetRef(dataset, sha))

    def _execute(self, dataframe, prepared) -> List[Dict[str, Any]]:
        """Execute the plan on the Spark adapter and flatten to legacy dicts."""
        from dq.spark_adapter import execute_plan

        outcomes = execute_plan(prepared, {self._dataset_name: dataframe})
        legacy = [outcome.to_legacy() for outcome in outcomes]
        logger.info(
            "portable plan %s produced %d outcomes",
            getattr(prepared, "plan_sha256", "unknown"),
            len(legacy),
        )
        return legacy
