# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The pandera engine: a pandas or a Polars frame against a pandera schema.

The schema is the rule's ``schema`` document, in pandera's own serialized
form (SPEC-017). The engine is called with a frame in hand. It is the first
engine whose frame is not Spark's, so it declares no persistence: the
framework's artifact path takes a Spark session from the frame.

pandera is imported when the engine is first used, never here.
"""

import importlib
import logging
from typing import List

from dq.engine.dq_engine import DQEngine
from dq.engine.pandera.pandera_check import BACKENDS, PanderaSchema
from dq.exceptions import ConfigurationError
from dq.utils.imports import require_extra

logger = logging.getLogger(__name__)

_PIN = "pandera>=0.36,<0.37"


def _backend(kind: str):
    """Return the pandera module that reads a document of ``kind``."""
    try:
        return importlib.import_module(BACKENDS[kind])
    except ImportError as error:
        raise ConfigurationError(
            f"pandera's {kind} schema cannot be used here: {error}. The frame's "
            "library comes with the frames: install the one this schema is for"
        ) from error


class PanderaEngine(DQEngine):
    """Validates a pandas or a Polars DataFrame against a pandera schema."""

    # What this engine can really do. A check pandera is told to treat as a
    # warning is reported with its own severity, and rows are counted where
    # pandera counts them. No row is filtered, and nothing is persisted.
    CAPABILITIES = {
        "criticality": "derived",
        "row_filter": False,
        "failed_rows": True,
        "persistence": False,
    }

    def _prepare(self, dataframe) -> PanderaSchema:
        """Read the rule's schema once, importing pandera for its backend."""
        require_extra("pandera", "pandera", "pandera", pin=_PIN)
        return PanderaSchema(self._config.get("schema", None), _backend)

    def _execute(self, dataframe, prepared: PanderaSchema) -> List[dict]:
        """Validate the frame, and return one result per outcome."""
        return prepared.validate(dataframe)
