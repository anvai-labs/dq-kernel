# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the engine scaffold (SPEC-011 G4).

The scaffold is only useful if what it produces is green as generated: it
must pass the formatter, its own unit tests, and the repository's guards
(the registry drift guard, the registry snapshot, the error catalog, and
example validation) without a manual edit.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from dq.sdk import scaffold as scaffold_module
from dq.sdk.scaffold import ScaffoldError, class_name, engine_module_path, scaffold

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
GUARD_TESTS = [
    "dq/tests/test_registry.py",
    "dq/tests/test_error_catalog.py",
    "dq/tests/test_engine_loader.py",
    "dq/tests/test_validate.py",
]


@pytest.fixture
def scratch(tmp_path):
    """A copy of the repository with only what the scaffold touches."""
    root = tmp_path / "repo"
    root.mkdir()
    for folder in ("dq", "examples"):
        shutil.copytree(
            REPOSITORY_ROOT / folder,
            root / folder,
            ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache", "*.pyc"),
        )
    shutil.copy2(REPOSITORY_ROOT / "pyproject.toml", root / "pyproject.toml")
    (root / "docs").mkdir()
    shutil.copy2(
        REPOSITORY_ROOT / "docs" / "error-catalog.adoc",
        root / "docs" / "error-catalog.adoc",
    )
    return root


def python_files(touched):
    return [str(path) for path in touched if path.suffix == ".py"]


def unmeasured_environment():
    """The current environment without what makes coverage follow a child.

    Under ``--cov``, pytest-cov arms every child process through its
    environment (``COV_CORE_*``, or coverage's own ``COVERAGE_PROCESS_START``).
    The child here runs a *copy* of the tree, so its measurements are not this
    repository's: merged into the parent's report they show up as a second,
    untested ``dq/`` and move the totals without any code having changed.
    """
    return {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("COV_CORE_", "COVERAGE_"))
    }


def run_python(arguments, cwd, **extra_environment):
    environment = {
        **unmeasured_environment(),
        "PYTHONPATH": str(cwd),
        "PYTHONDONTWRITEBYTECODE": "1",
        **extra_environment,
    }
    return subprocess.run(
        [sys.executable, *arguments],
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )


def test_the_generated_engine_is_green_as_generated(scratch):
    touched = scaffold(scratch, "soda")
    assert [path.relative_to(scratch).as_posix() for path in touched] == [
        "dq/engine/soda/__init__.py",
        "dq/engine/soda/soda_engine.py",
        "dq/engine/soda/soda_check.py",
        "dq/tests/test_soda_unit.py",
        "examples/soda_validation.conf",
        "dq/engine/registry.py",
        "dq/tests/test_registry.py",
        "docs/error-catalog.adoc",
    ]

    lint = run_python(
        ["-m", "black", "--check", "--quiet", *python_files(touched)], scratch
    )
    assert lint.returncode == 0, lint.stdout + lint.stderr

    tests = run_python(
        [
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "-o",
            "addopts=",
            "dq/tests/test_soda_unit.py",
            *GUARD_TESTS,
        ],
        scratch,
        SPARK_VERSION="3.5",
    )
    assert tests.returncode == 0, tests.stdout[-4000:] + tests.stderr[-2000:]
    assert "passed" in tests.stdout and "failed" not in tests.stdout.split("passed")[-1]


def test_a_child_in_the_scratch_copy_is_not_measured_by_the_parent_s_coverage(
    scratch, monkeypatch
):
    """What pytest-cov 6 and coverage itself each export to arm a child."""
    for key in (
        "COV_CORE_SOURCE",
        "COV_CORE_CONFIG",
        "COV_CORE_DATAFILE",
        "COVERAGE_PROCESS_START",
    ):
        monkeypatch.setenv(key, "armed")
    probe = (
        "import os; print(sorted(k for k in os.environ"
        " if k.startswith(('COV_CORE', 'COVERAGE'))))"
    )
    seen = run_python(["-c", probe], scratch)
    assert seen.returncode == 0, seen.stderr
    assert seen.stdout.strip() == "[]"
    # Everything else a child may rely on still reaches it.
    assert unmeasured_environment()["PATH"] == os.environ["PATH"]


def test_the_generated_engine_is_registered_and_loadable(scratch):
    scaffold(scratch, "soda")
    script = (
        "import logging; logging.disable(logging.CRITICAL); "
        "from dq.engine.registry import BUILT_IN_ENGINES, find_spec; "
        "assert 'soda' in BUILT_IN_ENGINES; "
        "spec = BUILT_IN_ENGINES['soda']; "
        "assert spec.payload_key == 'checks'; "
        "from dq.engine.engine_loader import EngineLoader; "
        "from pyhocon import ConfigFactory; "
        "config = ConfigFactory.from_dict({'name': 's', 'checks': [{'check': 'x', 'column': 'c'}]}); "
        "engine = EngineLoader().load_engine('soda', config, 7); "
        "assert type(engine).__name__ == 'SodaEngine'; "
        "assert engine.CAPABILITIES['persistence'] is False; "
        "print('loads')"
    )
    result = run_python(["-c", script], scratch, SPARK_VERSION="3.5")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "loads" in result.stdout


@pytest.mark.parametrize("name", ["", "Soda", "soda-db", "2soda", "soda.engine", None])
def test_a_name_that_cannot_be_an_engine_is_refused(name):
    with pytest.raises(ScaffoldError, match="lowercase identifier"):
        class_name(name) if False else scaffold(REPOSITORY_ROOT, name)


def test_an_existing_engine_is_not_overwritten_without_force(scratch):
    scaffold(scratch, "soda")
    engine_file = scratch / "dq" / "engine" / "soda" / "soda_engine.py"
    engine_file.write_text("# my work\n", encoding="utf-8")
    with pytest.raises(ScaffoldError, match="already exists"):
        scaffold(scratch, "soda")
    assert engine_file.read_text(encoding="utf-8") == "# my work\n"
    scaffold(scratch, "soda", force=True)
    assert engine_file.read_text(encoding="utf-8") != "# my work\n"


def test_a_refused_scaffold_leaves_the_tree_untouched(scratch):
    # An unrecognised registry stops the scaffold after its checks, and
    # before any write.
    registry = scratch / "dq" / "engine" / "registry.py"
    original = registry.read_text(encoding="utf-8")
    registry.write_text(
        original.replace("BUILT_IN_ENGINES: Dict[str, EngineSpec] = {", "ENGINES = {"),
        encoding="utf-8",
    )
    with pytest.raises(ScaffoldError, match="no BUILT_IN_ENGINES literal"):
        scaffold(scratch, "soda")
    assert not (scratch / "dq" / "engine" / "soda").exists()
    assert not (scratch / "examples" / "soda_validation.conf").exists()


def test_an_unrecognised_registry_or_catalog_is_reported_not_mangled():
    with pytest.raises(ScaffoldError, match="was not recognised"):
        scaffold_module._insert_registry_spec(
            "BUILT_IN_ENGINES: Dict[str, EngineSpec] = { oops", "x", ""
        )
    with pytest.raises(ScaffoldError, match="does not state its raise-site total"):
        scaffold_module._extend_error_catalog("no total here", "x", 1)
    with pytest.raises(ScaffoldError, match="no snapshot test"):
        scaffold_module._insert_registry_test_name("nothing here", "x")
    with pytest.raises(ScaffoldError, match="expected set was not recognised"):
        scaffold_module._insert_registry_test_name(
            "def test_built_in_registry_covers_the_known_engines():\n    pass\n", "x"
        )


def test_a_root_that_is_not_the_repository_is_refused_before_any_write(tmp_path):
    wrong_root = tmp_path / "docs"
    wrong_root.mkdir()
    with pytest.raises(ScaffoldError, match="is --root the repository root"):
        scaffold(wrong_root, "soda")
    assert list(wrong_root.iterdir()) == []


def test_generated_code_is_left_as_is_when_the_formatter_is_absent(monkeypatch):
    """Formatting is a convenience: without black the scaffold still works."""
    monkeypatch.setitem(sys.modules, "black", None)
    assert scaffold_module._format("x=1\n") == "x=1\n"


def test_the_scaffold_inserts_a_spec_at_the_end_of_the_literal_and_formats_it():
    source = (
        "BUILT_IN_ENGINES: Dict[str, EngineSpec] = {\n"
        "    spec.name: spec\n"
        "    for spec in (\n"
        "        EngineSpec('a', 'm', 'A', None),\n"
        "    )\n"
        "}\n"
    )
    spec = "        EngineSpec(\n            'b',\n            'n',\n            'B',\n            'checks',\n        ),\n"
    edited = scaffold_module._insert_registry_spec(source, "b", spec)
    assert edited.endswith("EngineSpec('a', 'm', 'A', None),\n" + spec + "    )\n}\n")
    compile(edited, "registry.py", "exec")


def test_names_follow_the_loader_s_convention():
    assert class_name("soda") == "SodaEngine"
    assert class_name("great_expectations") == "Great_expectationsEngine"
    assert engine_module_path("soda") == "dq.engine.soda.soda_engine"
