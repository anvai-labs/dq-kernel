# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The `dq-engines` command: list engines and what they can do (SPEC-011).

Answers "which engines can I use here, and what is each capable of" without
starting Spark. An engine's vendor library is imported only to read its
capability manifest, and only when that library is installed. The command
answers on any machine, the one that can use no engine included: an engine
it cannot use is reported, with the reason, never raised.
"""

import argparse
import importlib
import json
import sys
from typing import Any, Dict, List

from dq.engine.registry import (
    BUILT_IN_ENGINES,
    ENTRY_POINT_GROUP,
    EngineSpec,
    entry_point_engines,
    install_hint,
    missing_vendor_imports,
    module_itself_is_missing,
)


def describe(spec: EngineSpec, source: str = "built-in") -> Dict[str, Any]:
    """Describe one engine for humans and for JSON.

    Args:
        spec: The engine's declaration.
        source: ``"built-in"`` or ``"entry-point"``.

    Returns:
        Name, source, import target, payload key and the extras that provide
        what the engine needs. ``installed`` says whether the engine can be
        used here: every module it declares is installed, and its module
        was imported to read its capability manifest. When one is not,
        ``missing`` names it and ``install_hint`` says how to install it.
        When something else stood in the way -- a library that refuses to
        be imported on this machine -- ``problem`` carries the reason in
        that library's own words. The manifest is ``None`` whenever the
        engine could not be imported.
    """
    missing: List[str] = []
    problem = None
    capabilities = None
    try:
        missing = list(missing_vendor_imports(spec))
        if not missing:
            module = importlib.import_module(spec.module)
            engine_class = getattr(module, spec.class_name, None)
            if engine_class is None:
                problem = f"Engine class '{spec.class_name}' is not in '{spec.module}'"
            else:
                capabilities = dict(getattr(engine_class, "CAPABILITIES", {}))
    except ModuleNotFoundError as error:
        if module_itself_is_missing(spec.module, error):
            problem = f"{type(error).__name__}: {error}"
        else:
            # A library the engine imports is not installed, and one its
            # declaration did not name: said as the loader says it.
            missing = [error.name or str(error)]
    except Exception as error:  # noqa: BLE001 - reported, never raised
        problem = f"{type(error).__name__}: {error}"
    return {
        "name": spec.name,
        "source": source,
        "module": spec.module,
        "class": spec.class_name,
        "payload_key": spec.payload_key,
        "extras": list(spec.extras),
        "installed": not missing and problem is None,
        "missing": missing,
        "problem": problem,
        "install_hint": install_hint(spec) if missing else None,
        "renamed_from": list(spec.renamed_from),
        "capabilities": capabilities,
    }


def known_engines() -> List[Dict[str, Any]]:
    """Describe every engine this installation can resolve, built-ins first.

    A third-party engine registered twice, or under a built-in's name, is
    skipped: the built-in wins, exactly as it does at load time.
    """
    engines = [describe(spec) for spec in BUILT_IN_ENGINES.values()]
    seen = {engine["name"] for engine in engines}
    for name, module, class_name in entry_point_engines():
        if name in seen:
            continue
        spec = EngineSpec(name, module, class_name, None)
        engines.append(describe(spec, source="entry-point"))
    return sorted(engines, key=lambda engine: engine["name"])


_COLUMNS = (
    "name",
    "payload_key",
    "extras",
    "installed",
    "criticality",
    "row_filter",
    "failed_rows",
    "persistence",
)


def _named(engine: Dict[str, Any]) -> str:
    """Return an engine's name as the table spells it."""
    suffix = "" if engine["source"] == "built-in" else " (entry point)"
    return engine["name"] + suffix


def _cell(engine: Dict[str, Any], column: str) -> str:
    capabilities = engine["capabilities"] or {}
    if column == "name":
        former = ", ".join(engine["renamed_from"])
        return _named(engine) + (f" (was {former})" if former else "")
    if column == "extras":
        return ",".join(engine["extras"]) or "-"
    if column == "payload_key":
        return engine["payload_key"] or "-"
    if column == "installed":
        return "yes" if engine["installed"] else "no"
    if engine["capabilities"] is None:
        return "-"
    value = capabilities.get(column, None)
    if column == "criticality":
        return str(value or "-")
    return "yes" if value else "no"


def render_text(engines: List[Dict[str, Any]]) -> str:
    """Return the human-readable table of engines, and why one cannot be used."""
    headers = [column.replace("_", " ").capitalize() for column in _COLUMNS]
    rows = [[_cell(engine, column) for column in _COLUMNS] for engine in engines]
    widths = [
        (
            max(len(headers[index]), *(len(row[index]) for row in rows))
            if rows
            else len(headers[index])
        )
        for index in range(len(_COLUMNS))
    ]
    lines = [
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)),
        "  ".join("-" * width for width in widths),
    ]
    lines += [
        "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row))
        for row in rows
    ]
    lines = [line.rstrip() for line in lines]
    unmet = [engine for engine in engines if engine["missing"]]
    if unmet:
        lines += ["", "Not installed here:"]
        lines += [
            f"  {_named(engine)}: {', '.join(engine['missing'])}"
            + (f" -- {engine['install_hint']}" if engine["install_hint"] else "")
            for engine in unmet
        ]
    unusable = [engine for engine in engines if engine["problem"]]
    if unusable:
        lines += ["", "Cannot be used here:"]
        lines += [f"  {_named(engine)}: {engine['problem']}" for engine in unusable]
    lines += [
        "",
        f"Third-party engines register in the '{ENTRY_POINT_GROUP}' entry-point group.",
    ]
    return "\n".join(lines)


def main(argv=None) -> int:
    """Entry point of the `dq-engines` command."""
    parser = argparse.ArgumentParser(
        prog="dq-engines",
        description="List the data-quality engines this installation can use",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    args = parser.parse_args(argv)
    engines = known_engines()
    if args.format == "json":
        print(
            json.dumps(
                {"engines": engines, "entry_point_group": ENTRY_POINT_GROUP}, indent=2
            )
        )
    else:
        print(render_text(engines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
