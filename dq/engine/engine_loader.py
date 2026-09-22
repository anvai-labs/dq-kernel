# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env python
"""Dynamic engine loader for the Data Quality Framework."""

import importlib
import re
import logging

from dq.engine.dq_engine import DQEngine

logger = logging.getLogger(__name__)

# Engine name must be lowercase alphanumeric (no dots, slashes, etc.)
_ENGINE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


class EngineLoader:
    """Loads engine classes dynamically using Python's importlib.

    Engine modules are expected at ``dq.engine.{name}.{name}_engine``
    with a class named ``{Name}Engine`` (first letter capitalised).
    """

    def load_engine(self, engine_name: str, *args, **kwargs) -> DQEngine:
        """Dynamically load and instantiate a DQEngine subclass.

        Args:
            engine_name: The engine identifier (e.g., ``"deequ"``).
            *args: Positional arguments forwarded to the engine constructor.
            **kwargs: Keyword arguments forwarded to the engine constructor.

        Returns:
            An instantiated engine object.

        Raises:
            ImportError: If the engine module cannot be found.
            AttributeError: If the engine class cannot be found.
            ValueError: If the engine name contains invalid characters.
        """
        name = engine_name.lower()

        if not _ENGINE_NAME_PATTERN.match(name):
            raise ValueError(
                f"Invalid engine name '{engine_name}': must be lowercase "
                "alphanumeric with optional underscores."
            )

        try:
            module = importlib.import_module(f"dq.engine.{name}.{name}_engine")
            class_name = f"{name.capitalize()}Engine"
            engine_class = getattr(module, class_name)

            logger.debug("Loaded engine %s from %s", class_name, module.__name__)
            return engine_class(*args, **kwargs)

        except ModuleNotFoundError as e:
            raise ImportError(f"Engine module '{name}_engine' not found: {e}") from e

        except AttributeError as e:
            raise AttributeError(
                f"Engine class '{class_name}' not found in module "
                f"'{name}_engine': {e}"
            ) from e
