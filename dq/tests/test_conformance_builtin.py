# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Every built-in engine passes the conformance suite (SPEC-011 G1).

The suite is the contract an engine author outside this repository runs.
The engines inside it are held to nothing less. Each engine gets a frame
that makes it report one passing and one failing check, and is checked
with and without a sink, and -- where its checks are a list -- with the
same checks in another order, so each one is shown to keep its own
outcome. The great expectations rule is written in an order GX does not
return its results in.
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
    # The uniqueness expectation is defined third; GX groups expectations
    # by column and sends it back second, after the other one on ``id``.
    "greatexpectations": {
        "expectations": [
            {"type": "expect_column_values_to_not_be_null", "column": "id"},
            {"type": "expect_column_values_to_not_be_null", "column": "name"},
            {"type": "expect_column_values_to_be_unique", "column": "id"},
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


#: The same checks in another order, for every engine configured by a list
#: of them. Schema validation takes a schema, not checks, so it has none.
REORDERED = {
    "deequ": {"checks": list(reversed(RULES["deequ"]["checks"]))},
    "greatexpectations": {
        "expectations": list(reversed(RULES["greatexpectations"]["expectations"]))
    },
    "dqx": {"checks": list(reversed(RULES["dqx"]["checks"]))},
    "dqdl": {"ruleset": 'Rules=[IsComplete "name", RowCount >= 4]'},
    "businessrules": {"checks": list(reversed(RULES["businessrules"]["checks"]))},
    # The portable plan travels with the dataset it certifies.
    "portable": dict(RULES["portable"], plan=list(reversed(RULES["portable"]["plan"]))),
}


def frame_for(spark):
    frame = spark.createDataFrame(ROWS, COLUMNS)
    frame.createOrReplaceTempView("conformance_view")
    return frame


def engine_factory(name, frame):
    """Build fresh engines configured with the rule's checks."""
    return _factory(name, RULES[name])


def reordered_factory(name, frame):
    """Build fresh engines with the same checks in another order, or None."""
    if name not in REORDERED:
        return None
    return _factory(name, REORDERED[name])


def _factory(name, rules):
    spec = BUILT_IN_ENGINES[name]
    module = importlib.import_module(spec.module)
    engine_class = getattr(module, spec.class_name)
    config = ConfigFactory.from_dict(
        {"name": f"{name}-conformance", "engine": name, **rules}
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


NOT_REORDERED = (
    "attribution under reordering was not checked: no reordered_factory was given"
)


@pytest.mark.parametrize("name", built_in_engines())
def test_every_built_in_engine_conforms(spark, name):
    frame = frame_for(spark)
    report = assert_engine_conforms(
        engine_factory(name, frame), frame, InMemorySink, reordered_factory(name, frame)
    )
    assert "artifacts_carry_the_injected_run_key" in report.checks or name == "dqdl"
    if name == "schemavalidation":
        # It checks the frame against its own schema, so it can only pass,
        # and it takes a schema, not checks: there is no other order to try.
        assert report.notes == (
            "every outcome passed; a frame that makes the engine report both "
            "gives the suite more to check",
            NOT_REORDERED,
        )
        return
    # Every other engine here reports one of each, and keeps every check
    # with its own outcome when its checks are configured in another order.
    assert (
        report.notes == ()
    ), f"{name} should report both a passing and a failing check"
    assert "attribution_survives_reordering" in report.checks


@pytest.mark.parametrize("name", built_in_engines())
def test_built_in_engines_conform_without_a_sink_too(spark, name):
    frame = frame_for(spark)
    report = conformance_problems(
        engine_factory(name, frame), frame, None, reordered_factory(name, frame)
    )
    assert report.conforms, report.summary()


def test_the_builtin_roster_is_complete():
    """Every engine the registry declares is exercised above."""
    assert set(RULES) == set(BUILT_IN_ENGINES)
    assert set(REORDERED) == set(BUILT_IN_ENGINES) - {"schemavalidation"}
