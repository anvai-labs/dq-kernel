# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env python
"""Dynamic engine loader for the Data Quality Framework."""

import importlib
import re
import logging

from dq.engine.registry import find_entry_point, find_spec
from dq.engine.dq_engine import DQEngine, capability_problems
from dq.exceptions import ConfigurationError, EngineNotFoundError

logger = logging.getLogger(__name__)

# Engine name must be lowercase alphanumeric (no dots, slashes, etc.)
_ENGINE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


class EngineLoader:
    """Loads engine classes dynamically.

    Resolution order: the declarative built-in registry
    (``dq.engine.registry``), then third-party engines registered in the
    ``dq_kernel.engines`` entry-point group, then the historical naming
    convention (``dq.engine.{name}.{name}_engine`` with class
    ``{Name}Engine``) as a fallback for engines dropped in without a
    registry entry.
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
            EngineNotFoundError: If the engine module or class cannot be
                found (also an ``ImportError`` for existing callers).
            ValueError: If the engine name contains invalid characters.
        """
        name = engine_name.lower()

        if not _ENGINE_NAME_PATTERN.match(name):
            raise ValueError(
                f"Invalid engine name '{engine_name}': must be lowercase "
                "alphanumeric with optional underscores."
            )

        spec = find_spec(name)
        if spec is not None:
            module_path, class_name = spec.module, spec.class_name
        else:
            entry_point_target = find_entry_point(name)
            if entry_point_target is not None:
                module_path, class_name = entry_point_target
                logger.debug("Loaded engine %s via entry point", name)
            else:
                # Historical convention fallback: dq.engine.{name}.{name}_engine
                module_path = f"dq.engine.{name}.{name}_engine"
                class_name = f"{name.capitalize()}Engine"
                logger.debug("Resolved engine %s via naming convention", name)

        try:
            module = importlib.import_module(module_path)
            engine_class = getattr(module, class_name, None)
            if engine_class is None:
                raise AttributeError(
                    f"Engine class '{class_name}' not found in module "
                    f"'{module_path}': {module_path}"
                )
            problems = capability_problems(getattr(engine_class, "CAPABILITIES", None))
            if problems:
                # Refuse the engine before it runs: an invalid manifest
                # would make every capability-based decision downstream a
                # guess.
                raise ConfigurationError(
                    f"Engine '{name}' declares an invalid CAPABILITIES manifest: "
                    + "; ".join(problems)
                )
            logger.debug("Loaded engine %s from %s", class_name, module_path)
            return engine_class(*args, **kwargs)

        except ModuleNotFoundError as e:
            raise EngineNotFoundError(
                f"Engine module '{module_path}' not found: {e}"
            ) from e

        except AttributeError as e:
            raise EngineNotFoundError(str(e)) from e
