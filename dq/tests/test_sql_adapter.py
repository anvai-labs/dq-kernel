# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contracts of the SQL adapter beyond the corpus (SPEC-016).

The corpus says the adapter computes the documented values on DuckDB. These
say the rest: what is refused and *when*. Everything checkable is refused
before a query, so every refusal test hands the adapter a connection that
records, and asserts it recorded nothing. And each column type is measured
through real plans on the certified lane, or refused.
"""

import os
import subprocess
import sys
import uuid
from datetime import date, datetime, time
from decimal import Decimal
from fractions import Fraction
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql, postgresql, sqlite

from dq import sql_adapter
from dq.compute_support import ADAPTER_MODULES, adapter_named
from dq.envelope import RulesetEnvelope
from dq.exceptions import ConfigurationError
from dq.outcomes import CheckOutcome
from dq.plan import (
    CapabilitySet,
    ColumnRef,
    Comparison,
    DatasetRef,
    ExecutionPlan,
    MetricKind,
    Predicate,
    RuleKind,
    RuleSpec,
    Severity,
    SEMANTIC_GROUPS_VERSION,
    SEMANTIC_RANGES_VERSION,
    SEMANTICS_VERSION,
)
from dq.sql_adapter import SqlDataset, engine_from_env, execute_plan, validated_bindings
from dq.tests.sql_support import insert

pytest.importorskip("sqlalchemy", reason="the SQL adapter needs the sqlalchemy extra")
pytest.importorskip("duckdb", reason="the certified lane needs duckdb")

GE, LE, EQ = Comparison.GE, Comparison.LE, Comparison.EQ
MIN, MAX, GROUPS = (
    MetricKind.GROUP_MIN_DISTINCT,
    MetricKind.GROUP_MAX_DISTINCT,
    MetricKind.GROUP_COUNT,
)
COUNT, LOW, HIGH, PRESENT = (
    MetricKind.COLUMN_COUNT,
    MetricKind.COLUMN_MIN,
    MetricKind.COLUMN_MAX,
    MetricKind.PRESENT_COUNT,
)
DATASET = DatasetRef("orders", "a" * 64)
OTHER = DatasetRef("returns", "b" * 64)
EXTRAS = {
    "pyspark",
    "pydeequ",
    "great_expectations",
    "datafusion",
    "databricks",
    "delta",
    "boto3",
    "pyarrow",
    "sqlalchemy",
    "opentelemetry",
    "polars",
    "pandas",
    "duckdb",
}

ORDERS = sa.Table(
    "orders",
    sa.MetaData(),
    sa.Column("id", sa.String),
    sa.Column("n", sa.BigInteger),
    sa.Column("v", sa.Double),
    sa.Column("amount", sa.Numeric(10, 2)),
    sa.Column("flag", sa.Boolean),
    sa.Column("region", sa.String),
    sa.Column("day", sa.Date),
)
ROWS = [
    dict(
        id="a",
        n=1,
        v=0.0,
        amount=Decimal("1.25"),
        flag=True,
        region="east",
        day=date(2026, 1, 1),
    ),
    dict(id=None, n=None, v=-0.0, amount=None, flag=None, region="east", day=None),
    dict(
        id="c",
        n=3,
        v=float("nan"),
        amount=Decimal("3.50"),
        flag=False,
        region=None,
        day=date(2026, 1, 2),
    ),
    dict(
        id="d",
        n=3,
        v=1.0,
        amount=Decimal("-0.50"),
        flag=True,
        region="west",
        day=date(2026, 1, 2),
    ),
]


def rule(
    identifier,
    kind,
    *,
    target=None,
    column=None,
    group_by=(),
    dataset=DATASET,
    threshold="0",
    operator=GE,
):
    return RuleSpec(
        identifier,
        dataset,
        kind,
        Predicate(operator, Decimal(threshold)),
        ColumnRef(column) if column else None,
        Severity.ERROR,
        tuple(ColumnRef(name) for name in group_by),
        target,
    )


def plan_of(*rules):
    return ExecutionPlan(rules)


#: What a one-rule plan of each kind needs beyond its column.
DEFAULTS = {
    RuleKind.SIZE: dict(),
    RuleKind.COMPLETENESS: dict(operator=EQ, threshold="1"),
    RuleKind.GROUPED_DISTINCT: dict(target=MIN, group_by=("region",)),
    RuleKind.VALUE_RANGE: dict(target=LOW, operator=LE, threshold="1e30"),
}


def plan(kind=RuleKind.COMPLETENESS, column="id", **options):
    """A one-rule plan of ``kind`` over ``column``."""
    return plan_of(
        rule(
            "rule",
            kind,
            column=None if kind is RuleKind.SIZE else column,
            **{**DEFAULTS[kind], **options},
        )
    )


SIZED = rule("sized", RuleKind.SIZE)


class Recording:
    """A connection as the adapter needs it: a dialect, an execute, a record."""

    def __init__(self, dialect=None, row=None):
        self.dialect = dialect or postgresql.dialect()
        self.row = {} if row is None else row
        self.run = []

    def execute(self, statement):
        self.run.append(str(statement))
        return self

    def mappings(self):
        return self

    def one(self):
        return self.row


def refused(plan_, datasets, *match):
    """The plan is refused before any query, saying ``match``."""
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(plan_, datasets)
    for text in match:
        assert text in str(raised.value), (text, str(raised.value))
    for dataset in datasets.values():
        assert dataset.bind.run == [], "a query was sent before the refusal"


def engine_with(table=ORDERS, rows=ROWS):
    """A database of the certified lane with ``table`` and its rows in it."""
    engine = sa.create_engine("duckdb:///:memory:")
    table.metadata.create_all(engine)
    insert(table, rows, engine)
    return engine


def value_of(plan_, datasets, kind, column=None, grouping=()):
    values = sql_adapter.compute_plan_values(plan_, validated_bindings(plan_, datasets))
    return next(
        value
        for metric, value in values.items()
        if metric.kind is kind
        and (column is None or metric.column.name == column)
        and metric.group_by == tuple(ColumnRef(name) for name in grouping)
    )


def typed(declared, value):
    """A bound column of one declared type: the value twice, under two keys,
    and a null between them."""
    table = sa.Table(
        "t", sa.MetaData(), sa.Column("k", sa.String), sa.Column("x", declared)
    )
    rows = [dict(k="g", x=value), dict(k="g", x=None), dict(k="h", x=value)]
    return {"orders": SqlDataset(table, engine_with(table, rows))}


# ---------------------------------------------------------------- G3


def test_g3_a_bound_on_a_text_column_is_refused_before_any_query():
    recording = Recording()
    datasets = {"orders": SqlDataset(ORDERS, recording)}
    refused(
        plan(RuleKind.VALUE_RANGE, "id"),
        datasets,
        "column 'id' in dataset 'orders' has an unsupported type for ranges/v1 bounds: "
        "VARCHAR. rule 'rule' needs column_min of it, and bounds are declared absent "
        "for a text column",
    )


def test_a_column_counted_for_one_metric_is_still_asked_about_for_the_next():
    """A ranges/v1 rule asks two metrics of its column: how many values it
    holds, and a bound. The count of a date is allowed; the bound of that
    same column is still refused, before any query. Resolving a column once
    must not let its second metric through."""
    recording = Recording()
    datasets = {"orders": SqlDataset(ORDERS, recording)}
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(plan(RuleKind.VALUE_RANGE, "day"), datasets)
    assert "an unsupported type for ranges/v1 bounds: DATE" in str(raised.value)
    assert recording.run == []


@pytest.mark.parametrize(
    "declared, family",
    [
        (sa.Text(), "text"),
        (sa.Date(), "temporal"),
        (sa.DateTime(), "temporal"),
        (sa.Time(), "temporal"),
        (sa.LargeBinary(), "binary"),
        (sa.Uuid(), "uuid"),
    ],
)
def test_g3_each_family_without_bounds_is_refused_naming_it(declared, family):
    table = sa.Table("t", sa.MetaData(), sa.Column("x", declared))
    refused(
        plan(RuleKind.VALUE_RANGE, "x"),
        {"orders": SqlDataset(table, Recording())},
        "an unsupported type for ranges/v1 bounds",
        "rule 'rule' needs column_min",
        f"declared absent for a {family} column",
    )


@pytest.mark.parametrize("declared", [sa.JSON(), sa.ARRAY(sa.BigInteger)])
def test_g3_a_type_the_adapter_does_not_know_is_refused_for_everything(declared):
    table = sa.Table("t", sa.MetaData(), sa.Column("x", declared))
    refused(
        plan(RuleKind.COMPLETENESS, "x"),
        {"orders": SqlDataset(table, Recording())},
        f"is of type {declared}, which the SQL adapter cannot measure",
        "rule 'rule' needs present_count",
        "declared absent",
    )


@pytest.mark.parametrize("dialect", [sqlite, mysql])
def test_g3_a_dialect_the_adapter_does_not_know_is_asked_for_nothing(dialect):
    """Nothing is verified against the kernel on it, so even an integer's
    count is refused, before any query."""
    refused(
        plan(RuleKind.COMPLETENESS, "n"),
        {"orders": SqlDataset(ORDERS, Recording(dialect.dialect()))},
        "dataset binding 'orders' is bound to the "
        f"{dialect.dialect().name} dialect, which the SQL adapter does not "
        "know, so it can ask nothing of it. It knows: ['duckdb', 'postgresql']",
    )


def test_g3_a_float_on_a_dialect_that_cannot_keep_nan_out_is_refused(monkeypatch):
    """A dialect may be known and still have no way to say a value is NaN."""
    monkeypatch.setitem(
        sql_adapter.DIALECTS, "sqlite", sql_adapter._Dialect(None, True)
    )
    table = sa.Table("t", sa.MetaData(), sa.Column("x", sa.Double))
    datasets = {
        "orders": SqlDataset(table, Recording(sa.create_engine("sqlite://").dialect))
    }
    refused(
        plan(RuleKind.COMPLETENESS, "x"),
        datasets,
        "is a float column, and the sqlite dialect has no way to keep NaN out",
        "rule 'rule' needs present_count",
        "declared absent for a float on this dialect",
    )


def test_g3_no_statement_is_made_for_a_dialect_that_is_not_known():
    datasets = {"orders": SqlDataset(ORDERS, Recording(postgresql.dialect()))}
    with pytest.raises(
        ConfigurationError, match="no statement can be made for the mysql dialect"
    ):
        sql_adapter.compile_plan(plan(), datasets, dialect=mysql.dialect())


def test_g3_a_grouping_key_is_asked_about_too():
    table = sa.Table(
        "t", sa.MetaData(), sa.Column("x", sa.BigInteger), sa.Column("k", sa.JSON())
    )
    refused(
        plan(RuleKind.GROUPED_DISTINCT, "x", group_by=("k",)),
        {"orders": SqlDataset(table, Recording())},
        "column 'k' in dataset 'orders' is of type JSON, which the SQL adapter cannot measure",
    )


def test_a_statement_for_another_dialect_that_cannot_say_nan_is_refused(monkeypatch):
    """The bind's dialect may keep NaN out while the one asked for cannot."""
    monkeypatch.setitem(sql_adapter.DIALECTS, "mute", sql_adapter._Dialect(None, True))
    mute = sqlite.dialect()
    mute.name = "mute"
    datasets = {"orders": SqlDataset(ORDERS, Recording())}
    with pytest.raises(
        ConfigurationError, match="this dialect has no way to keep NaN out"
    ):
        sql_adapter.compile_plan(
            plan(RuleKind.COMPLETENESS, "v"), datasets, dialect=mute
        )


def test_a_parameter_whose_value_cannot_be_written_is_refused():
    asked = (
        sa.select(ORDERS.c.id)
        .where(ORDERS.c.n > sa.bindparam("least", value=lambda: 3))
        .subquery("orders")
    )
    refused(
        plan(),
        {"orders": SqlDataset(asked, Recording())},
        "holds a parameter whose value cannot be written into a statement",
    )


def test_the_postgresql_nan_predicate_is_what_postgresql_can_run():
    table = sa.Table("t", sa.MetaData(), sa.Column("x", sa.Double))
    text = sql_adapter.compile_plan(
        plan(RuleKind.COMPLETENESS, "x"),
        {"orders": SqlDataset(table, Recording(postgresql.dialect()))},
    )["orders"]
    assert "t.x != CAST('NaN' AS DOUBLE PRECISION)" in text
    assert "isnan" not in text


def test_a_boolean_is_bounded_through_an_integer_where_it_must_be():
    """PostgreSQL has no ``max(boolean)``: read by running every corpus
    statement on a real PostgreSQL 14. The bounds of a boolean are 0 and 1
    there too, taken of it cast once."""
    datasets = {"orders": SqlDataset(ORDERS, Recording(postgresql.dialect()))}
    ranged = plan_of(
        rule(
            "low",
            RuleKind.VALUE_RANGE,
            column="flag",
            target=LOW,
            operator=LE,
            threshold="1e30",
        ),
        rule(
            "high",
            RuleKind.VALUE_RANGE,
            column="flag",
            target=HIGH,
            operator=LE,
            threshold="1e30",
        ),
    )
    text = sql_adapter.compile_plan(ranged, datasets)["orders"]
    assert "max(CAST(orders.flag AS INTEGER))" in text
    assert "min(CAST(orders.flag AS INTEGER))" in text
    assert text.count("CAST(") == 2
    # DuckDB bounds a boolean directly.
    duckdb = sql_adapter.compile_plan(
        ranged, {"orders": SqlDataset(ORDERS, sa.create_engine("duckdb:///:memory:"))}
    )["orders"]
    assert "max(orders.flag)" in duckdb and "CAST(" not in duckdb


def test_a_family_is_read_from_the_type_a_decorator_stands_for():
    assert sql_adapter.family_of(sa.PickleType()) == "binary"
    assert sql_adapter.family_of(sa.Interval()) == "temporal"
    assert sql_adapter.family_of(sa.Enum("a", "b")) == "text"
    assert sql_adapter.family_of(sa.Float) == "float"  # a class, not an instance
    assert sql_adapter.family_of(sa.JSON()) == "unknown"


# ---------------------------------------------------------------- G4


def test_g4_one_query_per_dataset_for_a_multi_rule_plan():
    """Five rules over two datasets: each dataset's connection sees exactly
    one statement, whichever and how many rules ask of it."""
    multi = plan_of(
        rule("a", RuleKind.SIZE, dataset=OTHER),
        rule("b", RuleKind.COMPLETENESS, column="id"),
        rule("c", RuleKind.COMPLETENESS, column="n", operator=GE, threshold="0.5"),
        rule("d", RuleKind.SIZE),
        rule(
            "e",
            RuleKind.COMPLETENESS,
            column="flag",
            dataset=OTHER,
            operator=GE,
            threshold="0",
        ),
    )
    first, second = Recording(), Recording()
    returns = sa.Table("returns", sa.MetaData(), sa.Column("flag", sa.Boolean))
    datasets = {
        "orders": SqlDataset(ORDERS, first),
        "returns": SqlDataset(returns, second),
    }
    checked = validated_bindings(multi, datasets)
    for recording in (first, second):
        recording.row = dict.fromkeys(
            ("dq_row_count", "dq_present_1", "dq_present_2", "dq_present_3"), 0
        )
    values = sql_adapter.compute_plan_values(multi, checked)
    assert len(values) == 5  # two row counts, three present counts
    assert len(first.run) == 1 and len(second.run) == 1
    assert "orders" in first.run[0] and "returns" in second.run[0]
    assert all(text.count(";") == 0 for text in first.run + second.run)


@pytest.mark.parametrize("kind", list(RuleKind))
def test_g4_each_semantics_is_one_statement(kind):
    recording = Recording()
    datasets = {"orders": SqlDataset(ORDERS, recording)}
    with pytest.raises(KeyError):  # the recording holds no row to answer from
        sql_adapter.compute_plan_values(
            plan(kind, "n"), validated_bindings(plan(kind, "n"), datasets)
        )
    assert len(recording.run) == 1


def test_a_plain_table_is_one_statement():
    datasets = {"orders": SqlDataset(ORDERS, Recording())}
    assert sql_adapter.compile_plan(plan(RuleKind.SIZE), datasets)["orders"] == (
        "SELECT count(*) AS dq_row_count \nFROM orders"
    )


# -------------------------------------------------------- capabilities


@pytest.mark.parametrize(
    "declared, version, kinds",
    [
        (sql_adapter.CAPABILITIES, SEMANTICS_VERSION, {"row_count", "present_count"}),
        (
            sql_adapter.GROUP_CAPABILITIES,
            SEMANTIC_GROUPS_VERSION,
            {"group_count", "group_min_distinct", "group_max_distinct"},
        ),
        (
            sql_adapter.RANGES_CAPABILITIES,
            SEMANTIC_RANGES_VERSION,
            {"column_count", "column_min", "column_max"},
        ),
    ],
)
def test_capabilities_declare_every_metric_of_each_semantic_version(
    declared, version, kinds
):
    assert declared.adapter == sql_adapter.ADAPTER_NAME == "sql"
    assert declared.version == sql_adapter.ADAPTER_VERSION
    assert declared.semantic_version == version
    assert {kind.value for kind in declared.metrics} == kinds


def test_execute_plan_refuses_a_plan_the_declared_capabilities_cannot_hold():
    original = sql_adapter.CAPABILITIES
    sql_adapter.CAPABILITIES = CapabilitySet(
        "sql", "1", frozenset({MetricKind.ROW_COUNT})
    )
    try:
        with pytest.raises(ConfigurationError, match="adapter capabilities cannot"):
            execute_plan(plan(), {"orders": SqlDataset(ORDERS, Recording())})
    finally:
        sql_adapter.CAPABILITIES = original


# ------------------------------------------------------------ bindings


def test_bindings_must_cover_exactly_the_plan_datasets():
    datasets = {"orders": SqlDataset(ORDERS, Recording())}
    with pytest.raises(ConfigurationError, match="missing"):
        execute_plan(plan(), {})
    with pytest.raises(ConfigurationError, match="unknown"):
        execute_plan(plan(), {**datasets, "other": SqlDataset(ORDERS, Recording())})
    with pytest.raises(ConfigurationError, match="SqlDataset values"):
        execute_plan(plan(), [("orders", ORDERS)])


class HasExecute:
    def execute(self, statement):
        return None


@pytest.mark.parametrize(
    "dataset, problem",
    [
        (ORDERS, "got Table"),
        (None, "got NoneType"),
        ("orders", "got str"),
        (SqlDataset(None, Recording()), "its table has no declared columns"),
        (SqlDataset("orders", Recording()), "its table has no declared columns"),
        (
            SqlDataset(ORDERS, None),
            "its bind is neither an Engine nor something with an execute",
        ),
        (
            SqlDataset(ORDERS, object()),
            "its bind is neither an Engine nor something with an execute",
        ),
        (SqlDataset(ORDERS, HasExecute()), "its bind has no dialect"),
    ],
)
def test_a_binding_that_is_not_a_dataset_says_what_is_wrong_with_it(dataset, problem):
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(plan(), {"orders": dataset})
    message = str(raised.value)
    assert message.startswith(
        "dataset binding 'orders' must be a SqlDataset of a table"
    )
    assert message.endswith(problem)


def test_columns_are_resolved_by_exact_name_as_every_adapter_does():
    datasets = {"orders": SqlDataset(ORDERS, Recording())}
    with pytest.raises(ConfigurationError, match="exact case-sensitive"):
        execute_plan(plan(RuleKind.COMPLETENESS, "ID"), datasets)
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(plan(RuleKind.COMPLETENESS, "email"), datasets)
    assert "available columns: amount, day, flag, id, n, region, v" in str(raised.value)
    twins = sa.Table(
        "t",
        sa.MetaData(),
        sa.Column("value", sa.BigInteger),
        sa.Column("VALUE", sa.BigInteger),
    )
    with pytest.raises(ConfigurationError, match="ambiguous"):
        execute_plan(
            plan(RuleKind.COMPLETENESS, "value"),
            {"orders": SqlDataset(twins, Recording())},
        )


def test_a_parameter_of_the_bound_table_without_a_value_is_refused():
    """SQLAlchemy would write NULL where the value belongs and only warn, so
    the query would quietly answer about nothing."""
    asked = (
        sa.select(ORDERS.c.id)
        .where(ORDERS.c.n > sa.bindparam("least"))
        .subquery("orders")
    )
    refused(
        plan(),
        {"orders": SqlDataset(asked, Recording())},
        "its table holds a parameter, 'least', that has no value",
    )


# ------------------------------------------------- on the certified lane


MEASURABLE = [
    ("integer", sa.BigInteger(), 7, Fraction(7)),
    ("integer", sa.SmallInteger(), 7, Fraction(7)),
    ("float", sa.Float(24), 0.1, Fraction(Decimal("0.10000000149011612"))),
    ("float", sa.Double(), 0.1, Fraction(Decimal("0.1"))),
    ("numeric", sa.Numeric(10, 2), Decimal("1.25"), Fraction(5, 4)),
    ("boolean", sa.Boolean(), True, Fraction(1)),
]


@pytest.mark.parametrize("family, declared, value, bound", MEASURABLE)
def test_every_measurable_family_is_measured_exactly(family, declared, value, bound):
    datasets = typed(declared, value)
    grouped = plan(RuleKind.GROUPED_DISTINCT, "x", group_by=("k",))
    ranged = plan(RuleKind.VALUE_RANGE, "x")
    assert value_of(plan(RuleKind.COMPLETENESS, "x"), datasets, PRESENT, "x") == 2
    assert value_of(grouped, datasets, MIN, "x", ("k",)) == 1
    assert value_of(grouped, datasets, GROUPS, None, ("k",)) == 2
    assert value_of(ranged, datasets, COUNT, "x") == 2
    assert value_of(ranged, datasets, LOW, "x") == bound
    assert (
        value_of(plan(RuleKind.VALUE_RANGE, "x", target=HIGH), datasets, HIGH, "x")
        == bound
    )


COUNTABLE = [
    (sa.String(), "b"),
    (sa.Date(), date(2026, 1, 2)),
    (sa.DateTime(), datetime(2026, 1, 2, 3, 4, 5)),
    (sa.Time(), time(4, 5, 6)),
    (sa.LargeBinary(), b"ab"),
    (sa.Uuid(), uuid.UUID("00000000-0000-0000-0000-000000000001")),
]


@pytest.mark.parametrize("declared, value", COUNTABLE)
def test_a_family_without_bounds_is_counted_and_counted_distinct(declared, value):
    datasets = typed(declared, value)
    grouped = plan(RuleKind.GROUPED_DISTINCT, "x", group_by=("k",))
    assert value_of(plan(RuleKind.COMPLETENESS, "x"), datasets, PRESENT, "x") == 2
    assert value_of(grouped, datasets, MIN, "x", ("k",)) == 1
    assert value_of(grouped, datasets, GROUPS, None, ("k",)) == 2


def test_a_nan_is_absent_and_never_a_bound_or_a_group_of_signed_zeros():
    datasets = {"orders": SqlDataset(ORDERS, engine_with())}
    assert value_of(plan(RuleKind.COMPLETENESS, "v"), datasets, PRESENT, "v") == 3
    ranged = plan(RuleKind.VALUE_RANGE, "v")
    assert value_of(ranged, datasets, COUNT, "v") == 3
    assert value_of(
        plan(RuleKind.VALUE_RANGE, "v", target=HIGH), datasets, HIGH, "v"
    ) == Fraction(1)
    # East holds 0.0 and -0.0: one value, as Spark counts it.
    by_region = plan(RuleKind.GROUPED_DISTINCT, "v", group_by=("region",))
    assert value_of(by_region, datasets, MIN, "v", ("region",)) == 1
    # As a key: v holds 0.0, -0.0, NaN and 1.0, and the two zeros are one group.
    assert (
        value_of(
            plan(RuleKind.GROUPED_DISTINCT, "id", group_by=("v",)),
            datasets,
            GROUPS,
            None,
            ("v",),
        )
        == 3
    )


def test_an_engine_opens_its_own_connection_and_a_connection_is_used_as_it_is():
    engine = engine_with()
    through_engine = sql_adapter.compute_plan_values(
        plan(), validated_bindings(plan(), {"orders": SqlDataset(ORDERS, engine)})
    )
    with engine.connect() as connection:
        through_connection = sql_adapter.compute_plan_values(
            plan(),
            validated_bindings(plan(), {"orders": SqlDataset(ORDERS, connection)}),
        )
    assert through_engine == through_connection


def test_a_subquery_is_a_dataset_and_its_filter_is_written_into_the_text():
    engine = engine_with()
    asked = sa.select(ORDERS.c.id, ORDERS.c.n).where(ORDERS.c.n > 3).subquery("orders")
    datasets = {"orders": SqlDataset(asked, engine)}
    assert (
        "WHERE orders.n > 3) AS orders"
        in sql_adapter.compile_plan(plan(), datasets)["orders"]
    )
    assert value_of(plan(), datasets, PRESENT, "id") == 0  # no order has n > 3
    valued = (
        sa.select(ORDERS.c.id)
        .where(ORDERS.c.n > sa.bindparam("least", value=1))
        .subquery("orders")
    )
    assert value_of(plan(), {"orders": SqlDataset(valued, engine)}, PRESENT, "id") == 2


def test_two_groupings_are_one_statement_and_a_group_count_is_not_a_cross_join():
    engine = engine_with()
    two = plan_of(
        rule(
            "by_region",
            RuleKind.GROUPED_DISTINCT,
            column="id",
            target=MIN,
            group_by=("region",),
        ),
        rule(
            "by_day",
            RuleKind.GROUPED_DISTINCT,
            column="n",
            target=MAX,
            group_by=("day", "flag"),
        ),
    )
    datasets = {"orders": SqlDataset(ORDERS, engine)}
    assert len(sql_adapter.compile_plan(two, datasets)) == 1
    # The null key is a region of its own: east, west and null.
    assert value_of(two, datasets, GROUPS, None, ("region",)) == 3
    assert value_of(two, datasets, MIN, "id", ("region",)) == 1
    # Four day-and-flag keys: (Jan 1, true), (null, null), (Jan 2, false), (Jan 2, true).
    assert value_of(two, datasets, GROUPS, None, ("day", "flag")) == 4
    # Every day-and-flag key holds one row, so the most distinct n is one.
    assert value_of(two, datasets, MAX, "n", ("day", "flag")) == 1


def test_a_failed_statement_names_the_dataset_and_the_bind_it_ran_on():
    missing = sa.Table("nowhere", sa.MetaData(), sa.Column("id", sa.String))
    with pytest.raises(ConfigurationError) as raised:
        execute_plan(
            plan(),
            {"orders": SqlDataset(missing, sa.create_engine("duckdb:///:memory:"))},
        )
    message = str(raised.value)
    assert message.startswith("dataset 'orders': the statement failed on duckdb:")
    assert "caller's transaction" not in message
    engine = sa.create_engine("duckdb:///:memory:")
    with engine.connect() as connection:
        with pytest.raises(ConfigurationError) as raised:
            execute_plan(plan(), {"orders": SqlDataset(missing, connection)})
    assert (
        "It ran in the caller's transaction, which this engine may have left aborted"
        in str(raised.value)
    )


# ---------------------------------------------------------------- outcomes


def test_outcomes_are_bounded_immutable_snapshots_naming_the_adapter():
    outcome = execute_plan(plan(), {"orders": SqlDataset(ORDERS, engine_with())})[0]
    legacy = outcome.to_legacy()
    assert CheckOutcome.from_legacy(legacy).to_legacy() == legacy
    assert legacy["details"]["adapter"] == {"name": "sql", "version": "1"}
    assert legacy["success"] is False  # one id in four is null


def test_an_envelope_runs_every_semantics_and_refuses_a_bare_plan():
    datasets = {"orders": SqlDataset(ORDERS, engine_with())}
    envelope = RulesetEnvelope(
        (
            plan_of(SIZED),
            plan(),
            plan(RuleKind.GROUPED_DISTINCT, "id", group_by=("region",)),
            plan(RuleKind.VALUE_RANGE, "n"),
        )
    )
    outcomes = sql_adapter.execute_envelope(envelope, datasets)
    # The envelope's own order is not this adapter's business.
    assert sorted(outcome.to_legacy()["check"] for outcome in outcomes) == [
        "rule",
        "rule",
        "rule",
        "sized",
    ]
    with pytest.raises(
        ConfigurationError, match="execute_envelope requires a RulesetEnvelope"
    ):
        sql_adapter.execute_envelope(plan(), datasets)


# ---------------------------------------------------------------- G5


def test_g5_the_module_imports_with_sqlalchemy_absent_and_every_extra_blocked():
    code = f"""
import sys

BLOCKED = {sorted(EXTRAS)!r}


class Block:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in BLOCKED:
            raise AssertionError(fullname + " was imported")

sys.meta_path.insert(0, Block())
import dq.sql_adapter
import dq.compute_support

assert not [n for n in sys.modules if n.split(".")[0] in BLOCKED]
assert dq.sql_adapter.ADAPTER_NAME == "sql"
assert sorted(dq.compute_support.ADAPTER_MODULES) == ["arrow", "spark", "sql"]
print("ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        env={
            k: v
            for k, v in os.environ.items()
            if not k.startswith(("COV_CORE_", "COVERAGE_"))
        },
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_g5_adapter_sql_names_this_module(monkeypatch):
    assert ADAPTER_MODULES["sql"] == "dq.sql_adapter"
    assert adapter_named("sql") is sql_adapter
    monkeypatch.setitem(sys.modules, "sqlalchemy", None)
    with pytest.raises(ImportError) as raised:
        execute_plan(plan(), {"orders": SqlDataset(ORDERS, Recording())})
    message = str(raised.value)
    assert "The sqlalchemy adapter requires the optional sqlalchemy package" in message
    assert "poetry install -E sqlalchemy" in message


# ---------------------------------------------------------------- G6


def test_g6_a_bind_is_built_from_the_environment():
    engine = engine_from_env(
        "ORDERS_DATABASE_URL", {"ORDERS_DATABASE_URL": "duckdb:///:memory:"}
    )
    ORDERS.metadata.create_all(engine)
    insert(ORDERS, ROWS, engine)
    assert value_of(plan(), {"orders": SqlDataset(ORDERS, engine)}, PRESENT, "id") == 3


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"ORDERS_DATABASE_URL": ""},
        {"ORDERS_DATABASE_URL": "  "},
        {"ORDERS_DATABASE_URL": 7},
        None,
    ],
)
def test_g6_without_a_url_the_variable_is_named_and_nothing_is_built(env):
    with pytest.raises(ConfigurationError) as raised:
        engine_from_env("ORDERS_DATABASE_URL", env)
    assert str(raised.value) == (
        "ORDERS_DATABASE_URL does not hold a database URL. It must: a credential "
        "never enters a rule pack, so a bind is built from the environment"
    )


def test_a_portable_rule_on_the_sql_adapter_runs_through_the_engine():
    """``adapter = "sql"`` is a name the engine knows, and a SQL rule is run
    by handing the engine a dataset where a Spark rule gets a frame."""
    from pyhocon import ConfigFactory

    from dq.engine.portable.portable_engine import PortableEngine

    config = ConfigFactory.from_dict(
        {
            "name": "orders_complete",
            "engine": "portable",
            "dataset": "orders",
            "snapshot_sha256": "a" * 64,
            "adapter": "sql",
            "plan": [
                {
                    "constraint": "isComplete",
                    "constraint_name": "id_complete",
                    "column": "id",
                    "level": "Error",
                }
            ],
        }
    )
    (result,) = PortableEngine(config).apply(SqlDataset(ORDERS, engine_with()))
    assert result["check"] == "id_complete"
    assert result["success"] is False
    assert result["details"]["adapter"] == {"name": "sql", "version": "1"}
    assert result["details"]["criticality"] == "error"


def test_a_sql_rule_given_a_spark_frame_is_refused_by_the_binding():
    from pyhocon import ConfigFactory

    from dq.engine.portable.portable_engine import PortableEngine

    config = ConfigFactory.from_dict(
        {
            "engine": "portable",
            "dataset": "orders",
            "snapshot_sha256": "a" * 64,
            "adapter": "sql",
            "plan": [
                {"constraint": "isComplete", "constraint_name": "c", "column": "id"}
            ],
        }
    )
    with pytest.raises(ConfigurationError, match="got SimpleNamespace") as raised:
        PortableEngine(config).apply(SimpleNamespace())
    assert "got SimpleNamespace" in str(raised.value)


def test_g6_a_url_that_cannot_be_read_is_refused_without_repeating_it():
    with pytest.raises(ConfigurationError) as raised:
        engine_from_env(
            "ORDERS_DATABASE_URL", {"ORDERS_DATABASE_URL": "not a url secret"}
        )
    assert "ORDERS_DATABASE_URL does not hold a URL that SQLAlchemy can read" in str(
        raised.value
    )
    assert "secret" not in str(raised.value)
