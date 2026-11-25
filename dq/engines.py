# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The `dq-engines` command: list engines and what they can do (SPEC-011).

Answers "which engines can I use here, and what is each capable of" without
starting Spark. An engine's vendor library is imported only to read its
capability manifest, and only when that library is installed.
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
    missing_vendor_imports,
)


def install_hint(spec: EngineSpec) -> str:
    """Return how to install an engine's optional dependency, or ``None``."""
    if not spec.extra:
        return None
    return (
        f"pip install 'dq-kernel[{spec.extra}]'  (or: poetry install -E {spec.extra})"
    )


def describe(spec: EngineSpec, source: str = "built-in") -> Dict[str, Any]:
    """Describe one engine for humans and for JSON.

    Args:
        spec: The engine's declaration.
        source: ``"built-in"`` or ``"entry-point"``.

    Returns:
        Name, source, import target, payload key, extra, whether its vendor
        library is installed (and how to install it), and its capability
        manifest -- ``None`` when the engine cannot be imported here.
    """
    missing = list(missing_vendor_imports(spec))
    capabilities = None
    if not missing:
        try:
            module = importlib.import_module(spec.module)
            capabilities = dict(
                getattr(getattr(module, spec.class_name), "CAPABILITIES", {})
            )
        except (
            Exception
        ):  # noqa: BLE001 - an engine that cannot load is reported, not raised
            missing = [spec.module]
    return {
        "name": spec.name,
        "source": source,
        "module": spec.module,
        "class": spec.class_name,
        "payload_key": spec.payload_key,
        "extra": spec.extra,
        "installed": not missing,
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
    "extra",
    "installed",
    "criticality",
    "row_filter",
    "failed_rows",
    "persistence",
)


def _cell(engine: Dict[str, Any], column: str) -> str:
    capabilities = engine["capabilities"] or {}
    if column == "name":
        suffix = "" if engine["source"] == "built-in" else " (entry point)"
        former = ", ".join(engine["renamed_from"])
        return engine["name"] + suffix + (f" (was {former})" if former else "")
    if column == "extra":
        return engine["extra"] or "-"
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
    """Return the human-readable table of engines."""
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
    hints = [
        f"  {engine['name']}: {engine['install_hint']}"
        for engine in engines
        if engine["install_hint"]
    ]
    if hints:
        lines += ["", "Not installed here:", *hints]
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
