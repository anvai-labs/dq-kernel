#!/usr/bin/env python3
# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Prove that the published wheel needs no extra.

Usage: python scripts/check_core_install.py dist/dq_kernel-<version>.whl

Run it with the interpreter of a fresh virtual environment into which the
wheel, and nothing else, has been installed, from the root of a checkout:
its ``examples/`` are what the commands are asked to read.

The script reads the wheel's own metadata for which dependencies are
optional, and refuses an environment where any of them is installed: there
the proof would be void. With that established, it does what the package
says works with no extra installed.

- Every module the wheel ships imports, except the ones declared in
  ``NEEDS_AN_EXTRA_TO_IMPORT``; each of those must stop at a missing
  package, so the list cannot go stale.
- Every command the wheel installs answers ``--help``, the validator
  accepts an example, and the suggester turns a profile into a draft that
  validates.
- Each entry point that needs an extra, asked to work, names it: a library
  by the error it raises, a command by exiting with the code its help
  promises and saying what to install.
- A command judges its configuration before it looks for an extra.

Why CI needs this: the development group carries the tests' own tools, and
some of those bring an extra's package with them. A lane that installs the
development group therefore holds pyarrow, SQLAlchemy and the OpenTelemetry
API without any extra named, and cannot show that the package manages
without them. No test lane can reach a missing Spark either: every one has
it installed.

Nothing is imported from the checkout. The script's directory is its only
entry on the path, and each command runs in an isolated interpreter, so
``dq`` resolves to the installed wheel.
"""

from __future__ import annotations

import importlib
import importlib.metadata as metadata
import re
import subprocess
import sys
import tempfile
import zipfile
from email.parser import Parser
from pathlib import Path

DISTRIBUTION = "dq-kernel"
PACKAGE = "dq"

#: What every command's help promises for a missing dependency.
EXIT_MISSING_DEPENDENCY = 2

#: The only modules that may need an extra *to import*. Each is a vendor's
#: own engine code, a sink built on SQLAlchemy, or the Spark expression
#: helper: these import their library at the top, which the engine guide
#: asks a new engine not to do. Every other module in the wheel -- kernel,
#: plan, run core, runner, adapters, commands, harvesters, catalogs -- must
#: import with nothing installed. A vendor library belongs inside a
#: function, behind ``dq.utils.imports.require_extra``.
NEEDS_AN_EXTRA_TO_IMPORT = frozenset(
    {
        "dq.engine.businessrules.businessrules_engine",
        "dq.engine.businessrules.strategies",
        "dq.engine.databricks.databricks_check",
        "dq.engine.databricks.pipeline_expectations",
        "dq.engine.deequ.deequ_check",
        "dq.engine.deequ.deequ_constraints_builder",
        "dq.engine.deequ.deequ_engine",
        "dq.engine.deequ.expression",
        "dq.engine.dqdl.dqdl_engine",
        "dq.engine.dqx.dqx_engine",
        "dq.engine.schemavalidation.datatypes",
        "dq.engine.schemavalidation.schemavalidation_check",
        "dq.engine.schemavalidation.schemavalidation_engine",
        "dq.postgres_sink",
        "dq.sqlalchemy_sink",
        "dq.utils.spark_expr",
    }
)

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
#: A rule pack the validator accepts, for an engine that runs on Spark.
ON_SPARK = EXAMPLES / "basic_deequ_validation.conf"
#: A rule pack for the runner over files: every rule names the Arrow adapter.
OVER_FILES = EXAMPLES / "arrow_rule.conf"

#: ``(module, what to call on it, the extra the refusal must name)``. With
#: nothing installed, each call must stop at the missing package and say
#: which extra provides it: never fail some other way, and never run.
ASKED_TO_WORK = (
    ("dq.arrow_adapter", lambda m: m.table_of(object()), "-E arrow"),
    (
        "dq.runner",
        lambda m: m.read_dataset("orders", Path("unread.parquet")),
        "-E arrow",
    ),
    (
        "dq.sql_adapter",
        lambda m: m.engine_from_env("NOWHERE_URL", {}),
        "-E sqlalchemy",
    ),
    (
        "dq.sinks",
        lambda m: m.sink_from_config({"type": "postgres"}),
        "-E sqlalchemy",
    ),
    (
        "dq.observability",
        lambda m: m.observer_from_config([{"type": "otel"}]),
        "-E otel",
    ),
    # A profiler takes the library's own frame: it stops at the missing
    # package before it looks at what it was given.
    ("dq.profile.arrow", lambda m: m.profile_table(object(), "orders"), "-E arrow"),
    ("dq.profile.spark", lambda m: m.profile_frame(object(), "orders"), "-E spark"),
)


def over_files(scratch: Path) -> list:
    """Return ``dq-run``'s arguments over a dataset that is there to be read."""
    dataset = scratch / "orders.parquet"
    dataset.write_bytes(b"the runner must stop before it reads this")
    return [
        str(OVER_FILES),
        "--dataset",
        f"orders={dataset}",
        "--report",
        str(scratch / "report.json"),
    ]


def over_a_dataset(scratch: Path) -> list:
    """Return ``dq-profile``'s arguments over a dataset that is there to be read."""
    dataset = scratch / "orders.parquet"
    dataset.write_bytes(b"the profiler must stop before it reads this")
    return ["--dataset", f"orders={dataset}", "--profile", str(scratch / "p.json")]


#: ``(command, its arguments given a scratch directory, what its refusal must
#: say)``. Each must exit with ``EXIT_MISSING_DEPENDENCY``.
ASKED_TO_RUN = (
    ("dq-kernel", lambda scratch: [str(ON_SPARK)], "pip install dq-kernel[spark]"),
    (
        # The configuration is judged before Spark is looked for.
        "dq-kernel",
        lambda scratch: [str(scratch / "absent.conf")],
        "Configuration validation failed",
    ),
    ("dq-run", over_files, "-E arrow"),
    ("dq-profile", over_a_dataset, "-E arrow"),
)

#: A profile as ``dq-profile`` writes one: an integer column, over three
#: rows, that is complete, exactly distinct and never below zero.
A_PROFILE = """{
  "schema_version": "dq-profile/v1",
  "dataset": {"name": "orders", "snapshot_sha256": null},
  "source": "arrow",
  "rows": 3,
  "columns": [
    {"name": "id", "type": "integer", "nulls": 0, "distinct": 3,
     "distinct_exact": true, "min": 1, "max": 3, "min_length": null,
     "max_length": null, "precision": null, "scale": null,
     "not_a_number": null}
  ]
}
"""

#: What a draft written for Deequ from that profile says three times over.
DRAFT_WORDS = ("isComplete", "isUnique", "isNonNegative")


def spelled(name: str) -> str:
    """Return a distribution name in one spelling."""
    return name.lower().replace("_", "-").replace(".", "-")


ALWAYS = "always"
EXTRA = "extra"
PLATFORM = "platform"


def requirement_line(line: str) -> tuple:
    """Return ``(name, kind)`` for one ``Requires-Dist`` line.

    A line is ``name (version) ; marker``. A dependency with no marker is
    needed always; one whose marker names an extra, such as
    ``extra == "arrow"``, only with that extra; one with any other marker,
    such as ``sys_platform == 'win32'``, only on that platform.
    """
    head, _, marker = line.partition(";")
    name = re.split(r"[\s(\[]", head.strip(), maxsplit=1)[0]
    if not marker.strip():
        return spelled(name), ALWAYS
    return spelled(name), EXTRA if "extra ==" in marker else PLATFORM


def requirements(wheel: Path) -> list:
    """Return the wheel's ``(name, kind)`` requirements, from its metadata."""
    with zipfile.ZipFile(wheel) as archive:
        metadata_path = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        document = Parser().parsestr(archive.read(metadata_path).decode("utf-8"))
    return [requirement_line(line) for line in document.get_all("Requires-Dist") or []]


def installed() -> set:
    """Return every distribution installed in this interpreter's environment."""
    return {
        spelled(dist.metadata["Name"])
        for dist in metadata.distributions()
        if dist.metadata["Name"]
    }


def problems(required: list, present: set) -> list:
    """Return what is wrong with an environment for this proof, or nothing.

    Args:
        required: The wheel's ``(name, kind)`` requirements.
        present: The distributions installed.
    """
    found = []
    optional = sorted(name for name, kind in required if kind == EXTRA)
    always = sorted(name for name, kind in required if kind == ALWAYS)
    present_optional = sorted(set(optional) & present)
    if present_optional:
        found.append(
            "this environment holds optional dependencies, so it proves nothing: "
            + ", ".join(present_optional)
        )
    missing = sorted(set(always) - present)
    if missing:
        found.append("core dependencies are not installed: " + ", ".join(missing))
    return found


def from_installation(location: Path) -> bool:
    """Return whether ``location`` is inside this interpreter's own environment."""
    return location.is_relative_to(Path(sys.prefix).resolve())


# ------------------------------------------------------------- the modules


def modules_of(package) -> list:
    """Return every module the installed ``package`` ships.

    The package is all the wheel ships: its tests and its notebooks are
    left out of the build, so there is nothing here that needs another
    library to import.
    """
    root = Path(package.__file__).resolve().parent
    names = []
    for path in sorted(root.rglob("*.py")):
        parts = path.relative_to(root).with_suffix("").parts
        if parts[-1] == "__init__":
            parts = parts[:-1]
        names.append(".".join((package.__name__, *parts)))
    return names


def import_problems(names: list) -> list:
    """Import each module in ``names``; return what is wrong, printing the rest."""
    imported, declared, found = [], [], []
    for name in names:
        declared_to_need_one = name in NEEDS_AN_EXTRA_TO_IMPORT
        try:
            importlib.import_module(name)
        except ModuleNotFoundError as error:
            of_its_own = (error.name or "").split(".")[0] == PACKAGE
            if not declared_to_need_one:
                found.append(f"{name} does not import without an extra: {error}")
            elif of_its_own:
                found.append(
                    f"{name}, declared to need an extra, stops at a module "
                    f"of its own: {error}"
                )
            else:
                declared.append(f"{name} ({error.name})")
        except Exception as error:  # noqa: BLE001 - any other failure is the finding
            found.append(
                f"{name} fails to import another way: {type(error).__name__}: {error}"
            )
        else:
            if declared_to_need_one:
                found.append(
                    f"{name} is declared to need an extra and imports without one: "
                    "remove it from NEEDS_AN_EXTRA_TO_IMPORT"
                )
            else:
                imported.append(name)
    print(f"imported with no extra: {len(imported)} modules")
    print(f"declared to need an extra to import, and do: {len(declared)} modules")
    for line in declared:
        print(f"  {line}")
    return found


# ------------------------------------------------------------ the commands


def commands() -> dict:
    """Return the commands the wheel installs: ``{name: (module, function)}``."""
    own = {}
    for entry_point in metadata.entry_points(group="console_scripts"):
        module, _, function = entry_point.value.partition(":")
        if module.split(".")[0] == PACKAGE:
            own[entry_point.name] = (module, function)
    return own


def run(target: tuple, arguments: list) -> tuple:
    """Run one of the wheel's commands in an isolated interpreter."""
    module, function = target
    start = f"import sys; from {module} import {function}; sys.exit({function}() or 0)"
    done = subprocess.run(
        [sys.executable, "-I", "-c", start, *arguments],
        capture_output=True,
        text=True,
        timeout=600,
    )
    return done.returncode, done.stdout + done.stderr


def last_line(text: str) -> str:
    """Return the last line that says anything, for a problem's sake."""
    lines = [line for line in text.splitlines() if line.strip()]
    return lines[-1] if lines else "(no output)"


def command_problems(scratch: Path) -> list:
    """Return what is wrong with the wheel's commands, printing what passed.

    Args:
        scratch: An empty directory the commands may write into.
    """
    found = []
    available = commands()
    for name in sorted(available):
        code, output = run(available[name], ["--help"])
        if code == 0 and "usage:" in output:
            print(f"{name} answers --help")
        else:
            found.append(f"{name} --help exits {code}: {last_line(output)}")

    if "dq-validate" not in available:
        found.append("the wheel installs no command named dq-validate")
    else:
        code, output = run(available["dq-validate"], [str(ON_SPARK)])
        if code == 0:
            print(f"dq-validate accepts {ON_SPARK.name}")
        else:
            found.append(
                f"dq-validate {ON_SPARK.name} exits {code}: {last_line(output)}"
            )

    for name, arguments, expected in ASKED_TO_RUN:
        if name not in available:
            found.append(f"the wheel installs no command named {name}")
            continue
        given = arguments(scratch)
        code, output = run(available[name], given)
        if code == EXIT_MISSING_DEPENDENCY and expected in output:
            print(f"{name}, asked to run, exits {code} and says: {expected}")
        else:
            found.append(
                f"{name}, asked to run, exits {code} and does not say "
                f"{expected!r}: {last_line(output)}"
            )
    found += suggests_a_draft(available, scratch)
    return found


def suggests_a_draft(available, scratch: Path) -> list:
    """Return what is wrong with ``dq-suggest`` over a profile, printing what passed.

    The command needs no extra, so it is asked to do its whole work: turn a
    profile into a draft, written under ``scratch``, that validates.
    """
    if "dq-suggest" not in available:
        return ["the wheel installs no command named dq-suggest"]
    profile, draft = scratch / "orders.profile.json", scratch / "draft.conf"
    profile.write_text(A_PROFILE, encoding="utf-8")
    code, output = run(
        available["dq-suggest"],
        [str(profile), "--engine", "deequ", "--output", str(draft)],
    )
    written = draft.read_text(encoding="utf-8") if draft.is_file() else ""
    if code == 0 and all(word in written for word in DRAFT_WORDS):
        print("dq-suggest turns a profile into a draft that validates")
        return []
    return [
        f"dq-suggest over a profile exits {code} and does not write a draft: "
        f"{last_line(output)}"
    ]


# ------------------------------------------------------- asked to work


def refuses_naming_the_extra(module, call, extra: str) -> str:
    """Return a message if calling ``call`` did not stop for the missing extra."""
    try:
        call(module)
    except ImportError as error:
        if extra not in str(error):
            return f"it named no extra: {error}"
        return ""
    except Exception as error:  # noqa: BLE001 - any other failure is the finding
        return f"it failed another way: {type(error).__name__}: {error}"
    return "it ran: an extra is installed"


def asked_to_work_problems() -> list:
    """Return what is wrong when the library is asked to work, printing the rest."""
    found = []
    for name, call, extra in ASKED_TO_WORK:
        message = refuses_naming_the_extra(importlib.import_module(name), call, extra)
        if message:
            found.append(f"{name}, asked to work, {message}")
        else:
            print(f"{name}, asked to work, names its extra: {extra}")
    return found


def main(argv=None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1 or not Path(args[0]).is_file() or not EXAMPLES.is_dir():
        print(
            "usage: check_core_install.py <wheel>, from the root of a checkout",
            file=sys.stderr,
        )
        return 2

    present = installed()
    found = problems(requirements(Path(args[0])), present)
    if spelled(DISTRIBUTION) not in present:
        found.append(f"{DISTRIBUTION} itself is not installed")
    if not found:
        # Only once the environment is fit: an import proves nothing in one
        # that holds an extra.
        package = importlib.import_module(PACKAGE)
        location = Path(package.__file__ or ".").resolve()
        if not from_installation(location):
            found.append(
                f"{PACKAGE} was imported from {location}, not from {sys.prefix}"
            )
    for problem in found:
        print("problem: " + problem)
    if found:
        return 1
    print(f"installed ({len(present)}): {', '.join(sorted(present))}")
    print(f"imported from {location}")

    found = import_problems(modules_of(package))
    found += asked_to_work_problems()
    with tempfile.TemporaryDirectory() as directory:
        found += command_problems(Path(directory))
    for problem in found:
        print("problem: " + problem)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
