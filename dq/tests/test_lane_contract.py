# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract: the test lanes mean what their documentation says.

``pytest -m "not spark"`` is documented to run without a JVM. That holds
only if every test that uses the session ``spark`` fixture carries the
``spark`` marker; an unmarked one starts a JVM in the unit lane and is
deselected by the spark lane.
"""

import ast
from pathlib import Path

TESTS = Path(__file__).resolve().parent


def _module_markers(tree):
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "pytestmark"
            for target in node.targets
        ):
            return ast.unparse(node.value)
    return ""


def _carries_marker(decorators, marker):
    return any(marker in ast.unparse(decorator) for decorator in decorators)


def test_every_test_using_the_spark_fixture_is_marked_spark():
    offenders = []
    for path in sorted(TESTS.glob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if "pytest.mark.spark" in _module_markers(tree):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef) or not node.name.startswith(
                "test_"
            ):
                continue
            arguments = [arg.arg for arg in (*node.args.args, *node.args.kwonlyargs)]
            if "spark" in arguments and not _carries_marker(
                node.decorator_list, "spark"
            ):
                offenders.append(f"{path.name}::{node.name}")
    assert offenders == [], (
        "these tests take the `spark` fixture without the `spark` marker, so they "
        f"start a JVM in the unit lane: {offenders}"
    )


def test_every_test_module_declares_a_lane():
    """A module is either spark-marked or wholly unit; none is ambiguous."""
    stragglers = []
    for path in sorted(TESTS.glob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        has_tests = any(
            isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
            for node in ast.walk(tree)
        )
        if not has_tests:
            stragglers.append(path.name)
    # Helper modules (helpers, *_support) live in non-test files by convention.
    assert stragglers == []
