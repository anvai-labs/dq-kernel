# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Every built-in engine passes the conformance suite (SPEC-011 G1).

The suite is the contract an engine author outside this repository runs.
The engines inside it are held to nothing less. Each engine gets a frame
that makes it report one passing and one failing check, and is checked
with and without a sink.
"""

import importlib
import importlib.util

import pytest
from pyhocon import ConfigFactory

from dq.engine.registry import BUILT_IN_ENGINES, missing_vendor_imports
from dq.sdk.conformance import assert_engine_conforms, conformance_problems
from dq.sinks import InMemorySink

pytestmark = pytest.mark.spark

ROWS = [(1, "east", "ann"), (2, "east", None), (3, "west", "cy"), (4, "west", "di")]
COLUMNS = ["id", "region", "name"]
SNAPSHOT = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

RULES = {
    "deequ": {
        "checks": [
            {
                "constraint_name": "id_complete",
                "constraint": "isComplete",
                "column": "id",
                "level": "Error",
            },
            {
                "constraint_name": "name_complete",
                "constraint": "isComplete",
                "column": "name",
                "level": "Warning",
            },
        ]
    },
    "greatexpectations": {
        "expectations": [
            {"type": "expect_column_values_to_not_be_null", "column": "id"},
            {"type": "expect_column_values_to_not_be_null", "column": "name"},
        ]
    },
    "dqx": {
        "checks": [
            {"check": {"function": "is_not_null", "arguments": {"column": "id"}}},
            {
                "name": "names_present",
                "criticality": "warn",
                "check": {"function": "is_not_null", "arguments": {"column": "name"}},
            },
        ]
    },
    "dqdl": {"ruleset": 'Rules=[RowCount >= 4, IsComplete "name"]'},
    "businessrules": {
        "checks": [
            {"constraint": "NoNegativeValues", "level": "Error"},
            {
                "constraint": "GroupedDistinctBounds",
                "columns": ["id"],
                "group_by": ["region"],
                "min": 3,
                "level": "Error",
            },
        ]
    },
    "portable": {
        "dataset": "conformance",
        "snapshot_sha256": SNAPSHOT,
        "plan": [
            {
                "constraint": "isComplete",
                "constraint_name": "id_complete",
                "column": "id",
                "level": "Error",
            },
            {
                "constraint": "hasSize",
                "constraint_name": "size_at_least_five",
                "assertion": "lambda x: x >= 5",
                "level": "Warning",
            },
        ],
    },
    "schemavalidation": {
        "schema": {"catalog_type": "spark", "table": "conformance_view"}
    },
}


def frame_for(spark):
    frame = spark.createDataFrame(ROWS, COLUMNS)
    frame.createOrReplaceTempView("conformance_view")
    return frame


def engine_factory(name, frame):
    spec = BUILT_IN_ENGINES[name]
    module = importlib.import_module(spec.module)
    engine_class = getattr(module, spec.class_name)
    config = ConfigFactory.from_dict(
        {"name": f"{name}-conformance", "engine": name, **RULES[name]}
    )
    return lambda dqts=None: engine_class(config, dqts=dqts)


def built_in_engines():
    """Every registry engine, skipping those whose vendor library is absent.

    Importing an engine module is not the test: engines import their vendor
    library lazily, so a module imports fine and then fails at run time.
    """
    names = []
    for name, spec in BUILT_IN_ENGINES.items():
        missing = list(missing_vendor_imports(spec))
        if missing:
            names.append(
                pytest.param(
                    name, marks=pytest.mark.skip(reason=f"{name} needs {missing}")
                )
            )
        else:
            names.append(name)
    return names


@pytest.mark.parametrize("name", built_in_engines())
def test_every_built_in_engine_conforms(spark, name):
    frame = frame_for(spark)
    report = assert_engine_conforms(engine_factory(name, frame), frame, InMemorySink)
    assert "artifacts_carry_the_injected_run_key" in report.checks or name == "dqdl"
    if name != "schemavalidation":
        # Schema validation checks the frame against its own schema, so it
        # can only pass. Every other engine here reports one of each.
        assert (
            report.notes == ()
        ), f"{name} should report both a passing and a failing check"


@pytest.mark.parametrize("name", built_in_engines())
def test_built_in_engines_conform_without_a_sink_too(spark, name):
    frame = frame_for(spark)
    report = conformance_problems(engine_factory(name, frame), frame)
    assert report.conforms, report.summary()


def test_the_builtin_roster_is_complete():
    """Every engine the registry declares is exercised above."""
    assert set(RULES) == set(BUILT_IN_ENGINES)
