# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""One rule per vocabulary, for a column of any name (SPEC-025).

The measurement the spec rests on was made on one frame and one check per
engine: a not-null check, on a column whose name Spark does not read as a
name. The frame and the rules are here so every test that needs them sees
the same ones.
"""

from dq.engine.registry import BUILT_IN_ENGINES

#: Names Spark does not read as a name. Each is a column of the frame
#: below, and each holds a null in one of its two rows.
NOT_A_NAME = ("s.t", "order id", "x-y", "a`b")

#: Two rows. Beside one another: a column literally called `s.t` and a
#: struct `s` whose field `t` is never null; a column called `x-y` beside
#: columns `x` and `y`; a column called `order id`; one called ``a`b``; and
#: `plain`, which is the control. ``{null}`` and ``{value}`` say whether
#: the first row holds a null in each of them.
TRAP_FRAME = """
    SELECT
        CAST({null} AS INT) AS `s.t`, struct(CAST(1 AS INT) AS t) AS s,
        CAST({value} AS STRING) AS `order id`,
        CAST({null} AS INT) AS `x-y`, CAST(1 AS INT) AS `x`, CAST(1 AS INT) AS `y`,
        CAST({null} AS INT) AS `a``b`, CAST({null} AS INT) AS `plain`
    UNION ALL
    SELECT
        CAST(3 AS INT), struct(CAST(2 AS INT) AS t), CAST(NULL AS STRING),
        CAST(3 AS INT), CAST(3 AS INT), CAST(1 AS INT), CAST(3 AS INT), CAST(3 AS INT)
"""

#: What each engine's payload holds: one check that asks for ``{column}``
#: and must fail while that column holds a null.
CHECKS = {
    "portable": {
        "plan": [
            {"constraint": "isComplete", "constraint_name": "c", "column": "{column}"}
        ]
    },
    "deequ": {
        "checks": [
            {"constraint_name": "c", "constraint": "isComplete", "column": "{column}"}
        ]
    },
    "dqx": {
        "checks": [
            {
                "name": "c",
                "criticality": "error",
                "check": {
                    "function": "is_not_null",
                    "arguments": {"column": "{column}"},
                },
            }
        ]
    },
    "greatexpectations": {
        "expectations": [
            {
                "name": "c",
                "type": "expect_column_values_to_not_be_null",
                "column": "{column}",
                "criticality": "error",
            }
        ]
    },
    "businessrules": {
        "checks": [
            {
                "constraint_name": "c",
                "constraint": "GroupedDistinctBounds",
                "columns": ["{column}"],
                "group_by": ["{column}"],
                "min": 1,
            }
        ]
    },
    "schemavalidation": {"schema": {"not_null_columns": ["{column}"]}},
}


def trap_frame(spark, *, with_nulls=True):
    """Return the frame of the measurement, each target holding a null in
    one row, or none in either."""
    said = (
        {"null": "NULL", "value": "'x'"}
        if with_nulls
        else {"null": "2", "value": "'x'"}
    )
    return spark.sql(TRAP_FRAME.format(**said))


def rule_for(engine: str, column: str) -> dict:
    """Return a rule in ``engine``'s own vocabulary, its one check asking
    for ``column``."""
    rule = {
        "name": "r",
        "engine": engine,
        "dataframes": ["default"],
        **_spelled(CHECKS[engine], column),
    }
    if engine == "portable":
        rule.update(dataset="d", snapshot_sha256="a" * 64)
    return rule


def _spelled(value, column: str):
    """Return ``value`` with every ``{column}`` replaced by the column's name."""
    if value == "{column}":
        return column
    if isinstance(value, dict):
        return {key: _spelled(item, column) for key, item in value.items()}
    if isinstance(value, list):
        return [_spelled(item, column) for item in value]
    return value


def engines_that_name_a_column() -> list:
    """Return every built-in engine whose payload names a column, in the
    registry's order."""
    return [name for name, spec in BUILT_IN_ENGINES.items() if spec.column_keys]
