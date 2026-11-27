# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The SQL adapter against the shared semantic corpus, on DuckDB (SPEC-016).

Two layers are checked, so a divergence is located precisely: the metrics
the adapter computes (``check_metrics``) and the decisions it reaches
(``check_case``), the fail-closed cases included. Then every corpus plan is
compiled for the PostgreSQL dialect and held to a snapshot: the statement is
deterministic, and the one reviewed is the one that runs.
"""

import sys
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from dq import sql_adapter
from dq.exceptions import ConfigurationError
from dq.sdk.fixtures import (
    DATASET_NAME,
    build_plan,
    check_case,
    check_metrics,
    load_cases,
)
from dq.sql_adapter import SqlDataset, execute_plan, validated_bindings
from dq.tests.sql_support import load

pytest.importorskip("sqlalchemy", reason="the SQL adapter needs the sqlalchemy extra")
pytest.importorskip("duckdb", reason="the certified lane needs duckdb")

EVERY_CASE = load_cases()
DECIDABLE = [case for case in EVERY_CASE if not case.expect_error]
FAIL_CLOSED = [case for case in EVERY_CASE if case.expect_error]
SNAPSHOT = Path(__file__).with_name("fixtures") / "sql-postgresql.snap"

#: What a fail-closed case makes each adapter say, to the word.
REFUSALS = {
    "ranges-infinite-bound-fails-closed": (
        "column 'v' in dataset 'ds' has an infinite bound; ranges/v1 bounds "
        "must be finite"
    ),
    "ranges-text-bounds-fail-closed": (
        "column 's' in dataset 'ds' has an unsupported type for ranges/v1 bounds: "
        "VARCHAR. rule 'min_at_least_zero' needs column_min of it, and bounds "
        "are declared absent for a text column"
    ),
}


def engine():
    """A database of the certified lane, empty, and of this call's own."""
    return sa.create_engine("duckdb:///:memory:")


def bound(case, bind=None, name=DATASET_NAME):
    """A corpus case loaded into a database, bound as the plan names it."""
    bind = bind or engine()
    return bind, {name: SqlDataset(load(case, bind, name), bind)}


def compute(plan, datasets):
    return sql_adapter.compute_plan_values(plan, validated_bindings(plan, datasets))


# ------------------------------------------------------------- metrics


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_sql_computes_the_documented_metrics(case):
    _, datasets = bound(case)
    assert check_metrics(case, compute, datasets) == []


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_sql_decides_every_case_as_the_corpus_documents(case):
    _, datasets = bound(case)
    assert check_case(case, execute_plan, datasets) == []


@pytest.mark.parametrize("case", FAIL_CLOSED, ids=lambda case: case.id)
def test_sql_fails_closed_in_the_words_the_other_adapters_use(case):
    _, datasets = bound(case)
    assert check_case(case, execute_plan, datasets) == []
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(build_plan(case), datasets)
    assert str(raised.value) == REFUSALS[case.id]


def test_every_fail_closed_case_has_its_refusal_pinned():
    assert {case.id for case in FAIL_CLOSED} == set(REFUSALS)


def test_an_exactly_stored_decimal_comes_back_exact():
    """Through a typed expression this dialect hands back a decimal rounded
    to a double; the corpus case about two decimals a double cannot tell
    apart is what showed it."""
    case = next(c for c in DECIDABLE if c.id == "ranges-decimal-bounds-are-exact")
    bind, datasets = bound(case)
    with bind.connect() as connection:
        stored = connection.execute(sa.text("SELECT max(amount) FROM ds")).scalar()
    assert str(stored) == "0.10000000000000000001"
    from fractions import Fraction

    values = compute(build_plan(case), datasets)
    high = next(
        value for metric, value in values.items() if metric.kind.value == "column_max"
    )
    assert high == Fraction(10000000000000000001, 10**20)


# ------------------------------------------------------------- statements


def postgresql_text(case):
    """A corpus plan as the PostgreSQL dialect spells it, or its refusal."""
    bind, datasets = bound(case)
    try:
        return sql_adapter.compile_plan(
            build_plan(case), datasets, dialect=postgresql.dialect()
        )[DATASET_NAME]
    except ConfigurationError as error:
        return f"REFUSED: {error}"


def snapshot_text():
    lines = [
        "-- SPEC-016, G2: every corpus plan as the PostgreSQL dialect spells it.",
        "-- Reviewed by eye once; held by dq/tests/test_corpus_sql.py.",
        "-- To regenerate, after reviewing the change: python dq/tests/test_corpus_sql.py",
        "",
    ]
    for case in EVERY_CASE:
        lines += [f"-- case {case.id}", postgresql_text(case), ""]
    return "\n".join(lines)


@pytest.mark.parametrize("case", EVERY_CASE, ids=lambda case: case.id)
def test_g2_every_corpus_plan_compiles_to_its_postgresql_statement(case):
    """One case per test, so a difference names the case in its id."""
    expected = next(
        block.split("\n", 1)[1].rstrip("\n")
        for block in SNAPSHOT.read_text(encoding="utf-8").split("\n-- ")[1:]
        if block.startswith("case " + case.id + "\n")
    )
    assert postgresql_text(case) == expected


def test_the_snapshot_holds_every_corpus_case_and_nothing_else():
    held = {
        block.split("\n", 1)[0]
        for block in SNAPSHOT.read_text(encoding="utf-8").split("\n-- case ")[1:]
    }
    assert held == {case.id for case in EVERY_CASE}
    assert SNAPSHOT.read_text(encoding="utf-8") == snapshot_text() + "\n"


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_a_plan_always_compiles_to_the_same_text(case):
    bind, datasets = bound(case)
    for dialect in (bind.dialect, postgresql.dialect()):
        first = sql_adapter.compile_plan(build_plan(case), datasets, dialect=dialect)
        again = sql_adapter.compile_plan(build_plan(case), datasets, dialect=dialect)
        assert first == again


@pytest.mark.parametrize("case", DECIDABLE, ids=lambda case: case.id)
def test_no_statement_carries_a_parameter_of_its_own(case):
    """It is run as text, as it is: there is nothing to bind."""
    bind, datasets = bound(case)
    for dialect in (bind.dialect, postgresql.dialect()):
        text = sql_adapter.compile_plan(build_plan(case), datasets, dialect=dialect)[
            DATASET_NAME
        ]
        assert "%(" not in text, text
        assert ";" not in text, text  # one statement, not two


def test_the_text_that_is_compiled_is_the_text_that_is_run():
    class Recording:
        def __init__(self, bind):
            self.bind, self.run = bind, []

        def __getattr__(self, name):
            return getattr(self.bind, name)

        def execute(self, statement):
            self.run.append(str(statement))
            return self.bind.execute(statement)

    case = next(c for c in DECIDABLE if c.id == "groups-two-column-key")
    bind = sa.create_engine("duckdb:///:memory:")
    table = load(case, bind)
    with bind.connect() as connection:
        recording = Recording(connection)
        execute_plan(build_plan(case), {DATASET_NAME: SqlDataset(table, recording)})
        compiled = sql_adapter.compile_plan(
            build_plan(case), {DATASET_NAME: SqlDataset(table, connection)}
        )[DATASET_NAME]
    assert recording.run == [compiled]


if __name__ == "__main__":
    SNAPSHOT.write_text(snapshot_text() + "\n", encoding="utf-8")
    print(f"wrote {SNAPSHOT.relative_to(Path.cwd())} ({len(EVERY_CASE)} cases)")
    sys.exit(0)
