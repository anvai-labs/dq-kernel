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
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
import importlib.machinery
import importlib.metadata
import importlib.util
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
        payload_type: ``"list"`` (the default), ``"string"`` (dqdl's
            ``ruleset``), or ``"document"``: a mapping, or the text of one
            (pandera's ``schema``).
        extras: Every extra that provides what the engine needs, in the
            order to install them; empty for an engine the package's own
            dependencies carry. An engine on Spark names ``spark`` beside
            its own: no extra of this package pulls in another, so
            ``deequ`` alone installs no Spark. An engine that can run on
            more than one backend names them all and declares no
            ``vendor_imports``; the backend asked for says what it needs.
        renamed_from: Deprecated aliases that must be migrated to ``name``.
        vendor_imports: Modules that must be importable for the engine to
            run -- its vendor library and whatever else the package does
            not already install, ``pyspark`` for an engine on Spark.
            Engines import their vendor library lazily, so a missing one
            showed at run time, not at import; this lets a caller tell in
            advance whether an engine can run here, and say what to
            install when it cannot. A test holds each declaration to what
            the engine's code imports and to the extras it names.
        column_keys: Where the payload names a column that the engine hands
            to Spark by name. Each is a path into one entry of the payload,
            or into the payload itself where it is one mapping; at a path a
            value is one name, a list of names, or a list of those. A name
            there must be a plain identifier: Spark reads any other string
            as an expression -- ``a.b`` is the field ``b`` of a struct
            ``a``, ``x-y`` is a subtraction -- and the engine would measure
            something else. The kernel refuses such a name before the
            engine is imported (SPEC-025). Empty where the engine judges
            its own (none does today), where the author writes the
            expression as text (dqdl's ``ruleset``), or where a column is a
            dataframe label that never reaches Spark (pandera).
    """

    name: str
    module: str
    class_name: str
    payload_key: Optional[str]
    payload_type: str = "list"
    extras: Tuple[str, ...] = ()
    renamed_from: Tuple[str, ...] = ()
    vendor_imports: Tuple[str, ...] = ()
    column_keys: Tuple[str, ...] = ()


#: Every parameter of DQX's own check functions that names a column, read
#: from its registry on DQX 0.17.2: a test holds this list to the registry.
_DQX_COLUMN_ARGUMENTS = (
    "column",
    "columns",
    "ref_columns",
    "exclude_columns",
    "column1",
    "column2",
    "country_column",
    "merge_columns",
    "condition_column",
    "time_column",
    "ref_column",
    "null_safe_column_value_matching",
)


BUILT_IN_ENGINES: Dict[str, EngineSpec] = {
    spec.name: spec
    for spec in (
        EngineSpec(
            "businessrules",
            "dq.engine.businessrules.businessrules_engine",
            "BusinessrulesEngine",
            "checks",
            extras=("spark",),
            renamed_from=("custom",),
            vendor_imports=("pyspark",),
            column_keys=("columns", "group_by", "sort_by"),
        ),
        EngineSpec(
            "deequ",
            "dq.engine.deequ.deequ_engine",
            "DeequEngine",
            "checks",
            extras=("spark", "deequ"),
            vendor_imports=("pyspark", "pydeequ"),
            column_keys=("column", "columns"),
        ),
        EngineSpec(
            "dqdl",
            "dq.engine.dqdl.dqdl_engine",
            "DqdlEngine",
            "ruleset",
            payload_type="string",
            extras=("spark", "deequ"),
            vendor_imports=("pyspark", "pydeequ.dqdl"),
        ),
        EngineSpec(
            "dqx",
            "dq.engine.dqx.dqx_engine",
            "DqxEngine",
            "checks",
            extras=("spark", "dqx"),
            vendor_imports=("pyspark", "databricks.labs.dqx"),
            column_keys=(
                "check.for_each_column",
                *("check.arguments." + name for name in _DQX_COLUMN_ARGUMENTS),
            ),
        ),
        EngineSpec(
            "portable",
            "dq.engine.portable.portable_engine",
            "PortableEngine",
            "plan",
            # One engine, three backends: Arrow over files, SQL over a
            # connection, Spark. Each says what it needs when it is asked.
            extras=("arrow", "sqlalchemy", "spark"),
            column_keys=("column", "columns", "group_by"),
        ),
        EngineSpec(
            "greatexpectations",
            "dq.engine.greatexpectations.greatexpectations_engine",
            "GreatexpectationsEngine",
            "expectations",
            extras=("spark", "greatexpectations"),
            vendor_imports=("pyspark", "great_expectations"),
            # Every argument of GX 1.25's expectations that names a column,
            # but ``column_index``, which is a position: a test holds this
            # list to the library.
            column_keys=tuple(
                prefix + name
                for prefix in ("", "kwargs.")
                for name in (
                    "column",
                    "column_A",
                    "column_B",
                    "column_list",
                    "column_set",
                )
            ),
        ),
        EngineSpec(
            "schemavalidation",
            "dq.engine.schemavalidation.schemavalidation_engine",
            "SchemavalidationEngine",
            "schema",
            extras=("spark", "deequ"),
            vendor_imports=("pyspark", "pydeequ"),
            column_keys=(
                "not_null_columns",
                "unique_constraints",
                "overrides.column",
                "foreign_key_constraints.src_column",
            ),
        ),
        EngineSpec(
            "pandera",
            "dq.engine.pandera.pandera_engine",
            "PanderaEngine",
            "schema",
            payload_type="document",
            extras=("pandera",),
            vendor_imports=("pandera",),
        ),
    )
}

_RENAMED_TO: Dict[str, str] = {
    alias: spec.name
    for spec in BUILT_IN_ENGINES.values()
    for alias in spec.renamed_from
}


_NOT_HELD = object()


def importable(module: str) -> bool:
    """Return whether ``module`` is installed here, importing nothing.

    A module is looked up and never run, and neither is the package it is
    in. ``importlib.util.find_spec`` imports the parent of a dotted name to
    learn where its submodules live, and a parent may do anything when it
    is imported -- pydeequ refuses without ``SPARK_VERSION`` set -- so
    "can this engine run here" cannot be asked through it: asked of the
    dqdl engine on a machine with pydeequ and no ``SPARK_VERSION``, it
    raised, and took ``dq-engines`` down with it.

    A module already imported counts as installed. So does one whose
    import was refused (``None`` in ``sys.modules``): that refusal stands.
    """
    try:
        path = None
        prefix = ""
        for part in module.split("."):
            prefix = part if not prefix else f"{prefix}.{part}"
            held = sys.modules.get(prefix, _NOT_HELD)
            if held is not _NOT_HELD:
                if held is None:
                    return False
                path = getattr(held, "__path__", None)
                continue
            found = (
                importlib.util.find_spec(prefix)
                if path is None
                else importlib.machinery.PathFinder.find_spec(part, path)
            )
            if found is None:
                return False
            path = found.submodule_search_locations
    except (ImportError, ValueError):  # noqa: BLE001 - nothing here can be imported
        return False
    return True


def missing_vendor_imports(spec: EngineSpec) -> Tuple[str, ...]:
    """Return the modules ``spec`` needs that are not installed here."""
    return tuple(module for module in spec.vendor_imports if not importable(module))


def module_itself_is_missing(module: str, error: ModuleNotFoundError) -> bool:
    """Return whether ``error`` is about ``module`` itself, or a package it is in.

    Anything else a failed import reports is a library the module imports:
    the two are said differently, since only the second is installed by an
    extra.
    """
    if error.name is None:
        return False
    return module == error.name or module.startswith(f"{error.name}.")


def install_hint(spec: EngineSpec) -> Optional[str]:
    """Return how to install what an engine needs, or ``None`` if no extra does.

    One spelling for every caller: the loader, ``dq-engines`` and the
    bare-install proof all say this, so a reader is never sent to two
    different lines.
    """
    if not spec.extras:
        return None
    pip = ",".join(spec.extras)
    poetry = " ".join(f"-E {extra}" for extra in spec.extras)
    return f"pip install 'dq-kernel[{pip}]'  (or: poetry install {poetry})"


def refusal(name: str, modules: Sequence[str], hint: Optional[str] = None) -> str:
    """Return why an engine cannot run here: what is missing, and how to install it.

    Args:
        name: The engine, as a rule names it.
        modules: What is not installed, in the order the engine declared it.
        hint: What :func:`install_hint` returns for the engine, if it has
            an extra to name.

    Returns:
        The refusal. With nothing to install by, it says only what is
        missing: the engine's own documentation says the rest.
    """
    said = f"Engine '{name}' cannot run here: {_not_installed(modules)}."
    if hint is None:
        return said
    return f"{said} Install {'it' if len(modules) == 1 else 'them'} with: {hint}."


def _not_installed(modules: Sequence[str]) -> str:
    """Return which modules are not installed."""
    named = [repr(module) for module in modules]
    if len(named) == 1:
        return f"{named[0]} is not installed"
    return f"{', '.join(named[:-1])} and {named[-1]} are not installed"


def cannot_run_here(spec: EngineSpec) -> Optional[str]:
    """Return why ``spec`` cannot run here, or ``None`` when nothing installed stands in its way.

    Only what is installed is judged -- nothing is imported -- so the
    question costs nothing and can be asked of every engine at once. What
    it does not judge is whether the engine's library works once imported:
    that shows when the engine is loaded, and is reported then.
    """
    missing = missing_vendor_imports(spec)
    if not missing:
        return None
    return refusal(spec.name, missing, install_hint(spec))


def engines_that_cannot_run(names: Iterable[str]) -> List[str]:
    """Return why each engine in ``names`` cannot run here, in the order named.

    An engine named twice is judged once. One registered by entry point
    declares nothing, so nothing is judged for it here: the loader says
    what it meets when the engine is imported.
    """
    unmet: List[str] = []
    for name in dict.fromkeys(str(said).lower() for said in names):
        spec = find_spec(name)
        if spec is None:
            continue
        refusal_ = cannot_run_here(spec)
        if refusal_ is not None:
            unmet.append(refusal_)
    return unmet


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
