# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Declarative engine registry: the single place an engine is declared.

Historically an engine's identity lived in three stringly-typed places at
once (the loader's naming convention, ``dq.validate``'s hardcoded payload
table, and the engine directory itself), so adding an engine meant editing
parallel tables that could drift silently. The registry replaces that:
name, import target, required payload key, providing extra, and deprecated
aliases are declared once. A drift-guard contract test pins every engine
directory to a registry entry.

Third-party engines register through the ``dq_kernel.engines`` entry-point
group and need no code inside this repository. Resolution never imports an
engine module -- the registry is import-light (stdlib only) so config
validation and the control plane (ADR-009) can resolve engines without
any vendor dependency installed.
"""

import logging
from dataclasses import dataclass
import importlib.metadata
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

#: importlib.metadata group third-party engines register under.
ENTRY_POINT_GROUP = "dq_kernel.engines"


@dataclass(frozen=True)
class EngineSpec:
    """One engine's declaration.

    Attributes:
        name: The HOCON ``engine =`` value.
        module: Dotted module path holding the engine class.
        class_name: Engine class name inside ``module``.
        payload_key: Required non-empty config payload key; ``None`` means
            the engine declares no built-in payload check.
        payload_type: ``"list"`` or ``"string"`` (dqdl's ``ruleset``).
        extra: Poetry/pip extra providing the vendor dependency, for
            messages; ``None`` for extra-free engines.
        renamed_from: Deprecated aliases that must be migrated to ``name``.
        vendor_imports: Modules that must be importable for the engine to
            run. Engines import their vendor library lazily, so a missing
            one shows at run time, not at import; this lets a caller tell
            in advance whether an engine can run here.
    """

    name: str
    module: str
    class_name: str
    payload_key: Optional[str]
    payload_type: str = "list"
    extra: Optional[str] = None
    renamed_from: Tuple[str, ...] = ()
    vendor_imports: Tuple[str, ...] = ()


BUILT_IN_ENGINES: Dict[str, EngineSpec] = {
    spec.name: spec
    for spec in (
        EngineSpec(
            "businessrules",
            "dq.engine.businessrules.businessrules_engine",
            "BusinessrulesEngine",
            "checks",
            renamed_from=("custom",),
        ),
        EngineSpec(
            "deequ",
            "dq.engine.deequ.deequ_engine",
            "DeequEngine",
            "checks",
            extra="deequ",
            vendor_imports=("pydeequ",),
        ),
        EngineSpec(
            "dqdl",
            "dq.engine.dqdl.dqdl_engine",
            "DqdlEngine",
            "ruleset",
            payload_type="string",
            extra="deequ",
            vendor_imports=("pydeequ.dqdl",),
        ),
        EngineSpec(
            "dqx",
            "dq.engine.dqx.dqx_engine",
            "DqxEngine",
            "checks",
            extra="dqx",
            vendor_imports=("databricks.labs.dqx",),
        ),
        EngineSpec(
            "portable",
            "dq.engine.portable.portable_engine",
            "PortableEngine",
            "plan",
            extra="spark",
        ),
        EngineSpec(
            "greatexpectations",
            "dq.engine.greatexpectations.greatexpectations_engine",
            "GreatexpectationsEngine",
            "expectations",
            extra="greatexpectations",
            vendor_imports=("great_expectations",),
        ),
        EngineSpec(
            "schemavalidation",
            "dq.engine.schemavalidation.schemavalidation_engine",
            "SchemavalidationEngine",
            "schema",
            extra="deequ",
        ),
    )
}

_RENAMED_TO: Dict[str, str] = {
    alias: spec.name
    for spec in BUILT_IN_ENGINES.values()
    for alias in spec.renamed_from
}


def importable(module: str) -> bool:
    """Return whether ``module`` can be imported, without importing it.

    ``find_spec`` raises when a parent package is missing
    (``databricks.labs.dqx`` without ``databricks``), which also means the
    module cannot be imported.
    """
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):  # noqa: BLE001 - a missing parent package
        return False


def missing_vendor_imports(spec: EngineSpec) -> Tuple[str, ...]:
    """Return the vendor modules ``spec`` needs that are not importable."""
    return tuple(module for module in spec.vendor_imports if not importable(module))


def find_spec(name: str) -> Optional[EngineSpec]:
    """Return the built-in spec for ``name`` (exact match), or ``None``."""
    return BUILT_IN_ENGINES.get(name.lower())


def entry_point_engines() -> List[Tuple[str, str, str]]:
    """Return every third-party engine registered in the entry-point group.

    An entry point's value is ``module:Class`` (or ``module.Class``). A
    group no distribution registers is not an error, and neither is a
    metadata backend that cannot be enumerated: both yield no engines.

    Returns:
        ``(name, module, class_name)`` triples, by name.
    """
    try:
        group = importlib.metadata.entry_points().select(group=ENTRY_POINT_GROUP)
    except Exception:  # noqa: BLE001 - metadata must never block engine use
        logger.debug("entry-point group %s unavailable", ENTRY_POINT_GROUP)
        return []
    found: Dict[str, Tuple[str, str, str]] = {}
    for entry_point in group:
        target = entry_point.value
        module, _, class_name = target.partition(":")
        if not class_name:
            module, _, class_name = module.rpartition(".")
        if module and class_name:
            found[entry_point.name.lower()] = (
                entry_point.name.lower(),
                module,
                class_name,
            )
    return [found[name] for name in sorted(found)]


def find_entry_point(name: str) -> Optional[Tuple[str, str]]:
    """Resolve ``name`` from the third-party entry-point group.

    Args:
        name: Lowercased engine name.

    Returns:
        ``(module, class_name)`` for the registered engine, or ``None``.
    """
    for registered, module, class_name in entry_point_engines():
        if registered == name.lower():
            return module, class_name
    return None


def payload_key_for(name: str) -> Optional[str]:
    """Required payload key for a built-in engine (``None`` if unknown)."""
    spec = find_spec(name)
    return spec.payload_key if spec else None


def payload_type_for(name: str) -> str:
    """Payload shape for a built-in engine (``"list"`` or ``"string"``)."""
    spec = find_spec(name)
    return spec.payload_type if spec else "list"


def renamed_target(name: str) -> Optional[str]:
    """New engine name when ``name`` is a deprecated alias, else ``None``."""
    return _RENAMED_TO.get(name.lower())
