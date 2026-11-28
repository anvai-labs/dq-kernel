# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-end rule-pack formats: HOCON, YAML and JSON run identically (SPEC-010)."""

import json
from pathlib import Path

import pytest
import yaml

from dq.dq_framework import DQFramework

pytestmark = pytest.mark.spark

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = REPOSITORY_ROOT / "examples"
# The same rule pack in the three supported formats.
TWINS = (
    EXAMPLES / "basic_deequ_validation.conf",
    EXAMPLES / "basic_deequ_validation.yaml",
    EXAMPLES / "basic_deequ_validation.json",
)


@pytest.fixture
def frame(spark):
    return spark.createDataFrame(
        [(1, "ann", 30), (2, "bob", 45), (2, None, -1)], ["id", "name", "age"]
    )


def run(reference, frame):
    framework = DQFramework(frame.sparkSession, reference, default_dataframe=frame)
    return framework.run()


@pytest.mark.parametrize("reference", TWINS, ids=lambda p: p.suffix)
def test_each_format_runs_through_the_framework(reference, frame):
    results = run(str(reference), frame)
    # (1, "ann", 30), (2, "bob", 45), (2, None, -1): the id is never null,
    # it repeats, one name is null, and one age is negative. A result is
    # labelled with the constraint name the rule pack configures.
    assert [(result["check"], result["success"]) for result in results] == [
        ("id_complete", True),
        ("id_unique", False),
        ("name_complete", False),
        ("age_non_negative", False),
    ]
    # The rule pack's constraint kind is what ran, on the column it names.
    ran = [result["details"]["constraint"] for result in results]
    assert [constraint.split("(", 1)[0] for constraint in ran] == [
        "CompletenessConstraint",
        "UniquenessConstraint",
        "CompletenessConstraint",
        "ComplianceConstraint",
    ]
    assert ran[0].startswith("CompletenessConstraint(Completeness(id")
    assert ran[1].startswith("UniquenessConstraint(Uniqueness(List(id)")
    assert ran[2].startswith("CompletenessConstraint(Completeness(name")
    # Severity is reported on every result, from the rule pack's own spelling.
    assert [result["details"]["criticality"] for result in results] == [
        "error",
        "error",
        "warn",
        "warn",
    ]


def test_the_three_formats_produce_identical_run_outcomes(frame):
    outcomes = []
    for reference in TWINS:
        results = run(str(reference), frame)
        outcomes.append(
            [
                {
                    key: value
                    for key, value in result.items()
                    if key not in ("ts", "jobid")
                }
                for result in results
            ]
        )
    assert outcomes[0] == outcomes[1] == outcomes[2]


def test_a_bare_relative_path_and_a_path_object_load(frame, monkeypatch):
    monkeypatch.chdir(REPOSITORY_ROOT)
    relative = run("examples/basic_deequ_validation.conf", frame)
    from_object = run(EXAMPLES / "basic_deequ_validation.yaml", frame)
    assert [r["success"] for r in relative] == [r["success"] for r in from_object]


def test_a_yaml_rule_pack_with_a_portable_plan(frame, tmp_path):
    """YAML is not limited to vendor engines: the kernel path reads it too."""
    sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    pack = tmp_path / "orders.yaml"
    pack.write_text(
        "dqframework:\n"
        "  dqrules:\n"
        "    - name: portable_yaml\n"
        "      engine: portable\n"
        f"      dataset: orders\n"
        f"      snapshot_sha256: '{sha}'\n"
        "      plan:\n"
        "        - { constraint: isComplete, constraint_name: id_complete, column: id, level: Error }\n",
        encoding="utf-8",
    )
    results = run(str(pack), frame)
    assert [result["check"] for result in results] == ["id_complete"]
    assert results[0]["success"] is True


# --------------------------------------------- a logical name with a dot in it


DOTTED = {
    "dqframework": {
        "dataframes": {"main.sales.orders": "orders_view"},
        "dqrules": [
            {
                "name": "orders",
                "engine": "deequ",
                "dataframes": ["main.sales.orders"],
                "checks": [
                    {
                        "constraint_name": "id_complete",
                        "constraint": "isComplete",
                        "column": "id",
                        "level": "Error",
                    }
                ],
            }
        ],
    }
}
DOTTED_AS_HOCON = """
dqframework {
  dataframes { "main.sales.orders" = "orders_view" }
  dqrules = [{
    name = "orders"
    engine = "deequ"
    dataframes = ["main.sales.orders"]
    checks = [{ constraint_name = "id_complete", constraint = "isComplete", column = "id", level = "Error" }]
  }]
}
"""


@pytest.mark.parametrize("fmt", ["conf", "yaml", "json"])
def test_a_dataframe_is_found_by_a_logical_name_with_a_dot_in_it(frame, tmp_path, fmt):
    """pyhocon keeps a quoted key in quotes; the rule asks for the name as
    it was written."""
    frame.createOrReplaceTempView("orders_view")
    text = {
        "json": json.dumps(DOTTED),
        "yaml": yaml.safe_dump(DOTTED, sort_keys=False),
        "conf": DOTTED_AS_HOCON,
    }[fmt]
    path = tmp_path / f"dotted.{fmt}"
    path.write_text(text)

    framework = DQFramework(frame.sparkSession, str(path))

    assert framework.get_dataframe("main.sales.orders") is not None
    assert [(result["check"], result["success"]) for result in framework.run()] == [
        ("id_complete", True)
    ]
