# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env python
"""Dynamic engine loader for the Data Quality Framework."""

import importlib
import re
import logging
from collections.abc import Mapping

from dq.engine.checkspec import column_name_problems
from dq.engine.dq_engine import DQEngine, capability_problems
from dq.engine.registry import (
    cannot_run_here,
    find_entry_point,
    find_spec,
    install_hint,
    module_itself_is_missing,
    refusal,
)
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
            EngineNotFoundError: If the engine cannot be loaded, saying
                why: a library it needs is not installed (with the extras
                that install it, judged before anything is imported), its
                module or class cannot be found, or its module cannot be
                imported here. Also an ``ImportError``, for existing
                callers.
            ValueError: If the engine name contains invalid characters.
            ConfigurationError: If the rule names a column by anything but
                a plain identifier where the engine's payload names one --
                refused before the engine is imported (SPEC-025) -- or if
                the engine declares an invalid ``CAPABILITIES`` manifest.
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

        # A rule asks for a column by a plain name. One Spark would not read
        # as a name is refused here, before the engine is imported and
        # before any data is read (SPEC-025).
        if (
            spec is not None
            and spec.column_keys
            and args
            and isinstance(args[0], Mapping)
        ):
            problems = column_name_problems(spec, args[0], f"Engine '{name}': ")
            if problems:
                raise ConfigurationError("; ".join(problems))

        # What the engine needs is judged before anything is imported. A
        # library that is not installed is said so, with the extras that
        # install it -- not as an engine module that cannot be found.
        if spec is not None:
            unmet = cannot_run_here(spec)
            if unmet is not None:
                raise EngineNotFoundError(unmet)

        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError as e:
            if module_itself_is_missing(module_path, e):
                raise EngineNotFoundError(
                    f"Engine module '{module_path}' not found: {e}"
                ) from e
            # The engine's own module is here; a library it imports is not.
            # A built-in engine declares what it needs, so this is one the
            # declaration missed, and an engine from elsewhere.
            raise EngineNotFoundError(
                refusal(
                    name,
                    (e.name or str(e),),
                    install_hint(spec) if spec is not None else None,
                )
            ) from e
        except Exception as e:
            # Installed, and refusing to be imported: a vendor library
            # checks its own environment on import (pydeequ asks for
            # SPARK_VERSION). The reason is the library's own words.
            raise EngineNotFoundError(
                f"Engine '{name}' cannot be imported here: {type(e).__name__}: {e}"
            ) from e

        engine_class = getattr(module, class_name, None)
        if engine_class is None:
            raise EngineNotFoundError(
                f"Engine class '{class_name}' not found in module '{module_path}'"
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
