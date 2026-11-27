# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""SQL compute adapter: portable plans where the data already lives (ADR-011).

The kernel owns the semantics (``dq.plan``). This adapter compiles a plan
into one aggregate ``SELECT`` per dataset, has the database run it, and
hands the row it gets back to the kernel's own evaluation, so a decision is
made in one place whichever engine computed. The data does not move.

A dataset is bound as a :class:`SqlDataset`: a SQLAlchemy table whose columns
declare their types, and the ``Engine`` or ``Connection`` that reaches it.
The types have to be declared, not looked up, because they decide the
statement: a float column must have NaN kept out of its present count and
its bounds, and nothing type-agnostic can find a NaN in DuckDB -- a NaN
equals itself there, and ``isnan`` is an error on a non-numeric column.

Everything that can be checked is checked when the bindings are validated,
and nothing is sent to the database until that is done: that each column
exists, exactly named; what its family may be asked for, for each metric
that asks; and whether the bind's dialect can keep NaN out at all. What
cannot be honoured is refused before any query runs, naming the rule that
asked for it. The order matters because a failed statement cannot be taken
back: DuckDB has no savepoint, and it leaves the transaction the statement
ran in aborted.

The statement is built with SQLAlchemy Core and rendered by a dialect: the
bind's own to run, or another to compile for -- PostgreSQL, for a snapshot
-- without a server. What is logged, snapshot-tested and executed is one
text. It is executed as text on purpose: through a typed expression the
DuckDB dialect hands back a decimal rounded to a double.

DuckDB is the certified lane.

This module imports with SQLAlchemy absent and with every other extra
blocked: SQLAlchemy is imported when a plan is validated or run, and
without it the adapter refuses, naming the extra.

    from sqlalchemy import Column, MetaData, String, Table, create_engine

    from dq.sql_adapter import SqlDataset, execute_plan

    orders = Table("orders", MetaData(), Column("id", String))
    engine = create_engine("duckdb:///orders.duckdb")
    outcomes = execute_plan(plan, {"orders": SqlDataset(orders, engine)})
"""

from __future__ import annotations

import dataclasses
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Callable, Optional

from dq.compute_support import (
    ComputeAdapter,
    as_exact_fraction,
    capability_sets,
    execute_envelope_with,
    execute_plan_with,
    metrics_by_grouping,
    metrics_of,
    require_exact_bindings,
    require_exact_column,
    values_by_dataset,
)
from dq.exceptions import ConfigurationError
from dq.outcomes import CheckOutcome
from dq.plan import (
    CapabilitySet,
    ExecutionPlan,
    MetricKind,
    SEMANTIC_GROUPS_VERSION,
    SEMANTIC_RANGES_VERSION,
    SEMANTICS_VERSION,
)
from dq.utils.imports import require_extra

logger = logging.getLogger(__name__)

ADAPTER_NAME = "sql"
ADAPTER_VERSION = "1"
_SETS = capability_sets(ADAPTER_NAME, ADAPTER_VERSION)
#: What this adapter can compute for each semantic version. These names are
#: what ``capabilities_for`` reads, so a caller can narrow one for a test.
CAPABILITIES = _SETS[SEMANTICS_VERSION]
GROUP_CAPABILITIES = _SETS[SEMANTIC_GROUPS_VERSION]
RANGES_CAPABILITIES = _SETS[SEMANTIC_RANGES_VERSION]


def capabilities_for(plan: ExecutionPlan) -> CapabilitySet:
    """The SQL adapter capabilities matching a plan's semantics."""
    if plan.semantic_version == SEMANTIC_GROUPS_VERSION:
        return GROUP_CAPABILITIES
    if plan.semantic_version == SEMANTIC_RANGES_VERSION:
        return RANGES_CAPABILITIES
    return CAPABILITIES


def _sqlalchemy():
    """Return SQLAlchemy, or name the extra that provides it."""
    return require_extra(
        "sqlalchemy",
        "sqlalchemy",
        "sqlalchemy",
        pin="sqlalchemy>=2.0,<2.2",
        kind="adapter",
    )


# ------------------------------------------------------------- what is bound


@dataclass(frozen=True)
class SqlDataset:
    """A dataset that lives in a database.

    Attributes:
        table: A SQLAlchemy ``Table``, or a subquery, whose columns declare
            their types. A declared type is what the adapter may assume
            about the values; it need not be how the database stores them.
        bind: An SQLAlchemy ``Engine`` or ``Connection``, or anything with
            a ``dialect`` and an ``execute`` that behaves as a connection's.
            Given an ``Engine``, the adapter opens and closes a connection
            of its own for each statement. Given a ``Connection``, it runs in
            the caller's transaction and cannot undo a failure there.
    """

    table: Any
    bind: Any


def engine_from_env(url_env: str, env: Mapping[str, str] = os.environ):
    """Build an ``Engine`` from a database URL the environment holds.

    A credential never enters a rule pack: a pack may name a variable, and
    the variable holds the URL.

    Args:
        url_env: The name of the environment variable holding the URL.
        env: Where to read it: the process environment.

    Returns:
        The ``Engine``.

    Raises:
        ConfigurationError: When the variable is not set, holds nothing
            usable, or holds what SQLAlchemy cannot read as a URL.
    """
    sqlalchemy = _sqlalchemy()
    url = env.get(url_env, None) if hasattr(env, "get") else None
    if not isinstance(url, str) or not url.strip():
        raise ConfigurationError(
            f"{url_env} does not hold a database URL. It must: a credential "
            "never enters a rule pack, so a bind is built from the environment"
        )
    try:
        return sqlalchemy.create_engine(url.strip())
    except sqlalchemy.exc.ArgumentError as error:
        # The URL is not repeated in the message: it may hold a password.
        raise ConfigurationError(
            f"{url_env} does not hold a URL that SQLAlchemy can read: {error}"
        ) from error


# ----------------------------------------------- what a column may be asked for


@dataclass(frozen=True)
class _Can:
    """What the family of a column's declared type may be asked for."""

    #: May be counted present (not null, and for a float not NaN).
    present: bool
    #: May be counted distinct, and grouped on.
    distinct: bool
    #: May give a least and a greatest value, as an exact fraction.
    bounds: bool
    #: Whether NaN has to be kept out, which takes a dialect's predicate.
    nan: bool


_INTEGER, _FLOAT, _NUMERIC, _BOOLEAN = "integer", "float", "numeric", "boolean"
_TEXT, _TEMPORAL, _BINARY, _UUID, _UNKNOWN = (
    "text",
    "temporal",
    "binary",
    "uuid",
    "unknown",
)

#: Family -> what it may be asked for. A family that is not here may be
#: asked for nothing: whether its values can be NaN, and how they compare,
#: is not known, so it is refused rather than approximated.
_FAMILIES = {
    _INTEGER: _Can(True, True, True, False),
    _FLOAT: _Can(True, True, True, True),
    _NUMERIC: _Can(True, True, True, False),
    _BOOLEAN: _Can(True, True, True, False),
    _TEXT: _Can(True, True, False, False),
    _TEMPORAL: _Can(True, True, False, False),
    _BINARY: _Can(True, True, False, False),
    _UUID: _Can(True, True, False, False),
}


def family_of(declared) -> str:
    """Return which family a declared SQLAlchemy type belongs to."""
    types = _sqlalchemy().types
    # A TypeDecorator stands for the type it wraps; that one decides.
    while isinstance(declared, types.TypeDecorator) or (
        isinstance(declared, type) and issubclass(declared, types.TypeDecorator)
    ):
        declared = types.to_instance(declared.impl)
    declared = types.to_instance(declared)
    # A Float is a Numeric, so it has to be asked first.
    for family, bases in (
        (_FLOAT, (types.Float,)),
        (_INTEGER, (types.Integer,)),
        (_NUMERIC, (types.Numeric,)),
        (_BOOLEAN, (types.Boolean,)),
        (_TEXT, (types.String, types.Text)),
        (_TEMPORAL, (types.Date, types.DateTime, types.Time)),
        (_BINARY, (types.LargeBinary,)),
        (_UUID, (types.Uuid,)),
    ):
        if isinstance(declared, bases):
            return family
    return _UNKNOWN


def _nan_is_duckdb(column):
    return _sqlalchemy().func.isnan(column)


def _nan_is_postgresql(column):
    from sqlalchemy.dialects import postgresql

    sqlalchemy = _sqlalchemy()
    # A literal column, not a bound literal: the statement is one text,
    # run as it is, and carries no parameter of its own.
    nan = sqlalchemy.cast(
        sqlalchemy.literal_column("'NaN'"), postgresql.DOUBLE_PRECISION()
    )
    return column == nan


@dataclass(frozen=True)
class _Dialect:
    """What a database dialect can be asked for."""

    #: How to say a column's value is NaN; ``None`` where it cannot be said,
    #: so a float column is declared absent there.
    nan: Optional[Callable]
    #: Whether ``min`` and ``max`` take a boolean. Where they do not, the
    #: bounds are taken of the boolean cast to an integer: the same two
    #: values, 0 and 1.
    boolean_bounds: bool


#: Dialect name -> what it can be asked for. A dialect that is not here is
#: asked for nothing: a float column is declared absent on it. ``func.isnan``
#: is not one answer for all: it renders as plain ``isnan(...)`` for
#: PostgreSQL too, where there is no such function. And PostgreSQL has no
#: ``max(boolean)`` -- read by running every corpus statement on a real
#: PostgreSQL 14, not from the compile alone.
DIALECTS = {
    "duckdb": _Dialect(_nan_is_duckdb, True),
    "postgresql": _Dialect(_nan_is_postgresql, False),
}

#: The certified lane: the one dialect whose statements are run in the tests.
CERTIFIED_DIALECT = "duckdb"


def _profile(dialect) -> Optional[_Dialect]:
    """Return what a dialect can be asked for, or ``None`` where it is unknown."""
    return DIALECTS.get(getattr(dialect, "name", None))


# ------------------------------------------------------------- validation


@dataclass(frozen=True)
class _Bound:
    """A dataset as the plan may use it: nothing else is looked up later."""

    name: str
    dataset: SqlDataset
    dialect: str
    #: What the bind's dialect can be asked for; ``None`` where NaN cannot
    #: be kept out on it.
    nan: Optional[Callable]
    #: Whether ``min`` and ``max`` take a boolean on the bind's dialect.
    boolean_bounds: bool
    #: ``{column name: (column, family)}`` for each column the plan names.
    columns: dict
    #: ``{metric: rule id}``: the plan's rule that needs each metric.
    asked_by: dict


def _binding_problem(dataset) -> Optional[str]:
    """Return what is wrong with a binding's shape, or ``None``.

    An ``Engine`` has no ``execute`` of its own in SQLAlchemy 2: the adapter
    opens a connection for it. Anything else must behave as a connection.
    """
    if not isinstance(dataset, SqlDataset):
        return f"got {type(dataset).__name__}"
    if not hasattr(dataset.table, "c"):
        return "its table has no declared columns"
    bind = dataset.bind
    if not isinstance(bind, _sqlalchemy().engine.Engine) and not callable(
        getattr(bind, "execute", None)
    ):
        return "its bind is neither an Engine nor something with an execute"
    if not hasattr(bind, "dialect"):
        return "its bind has no dialect"
    # A table's own filter may hold a parameter. Its value is written into
    # the statement's text; one that has none would be written as NULL, and
    # SQLAlchemy only warns -- the query would then quietly answer about
    # nothing. So it is refused here, with everything else, before a query.
    sqlalchemy = _sqlalchemy()
    for element in sqlalchemy.sql.visitors.iterate(dataset.table):
        if (
            isinstance(element, sqlalchemy.sql.elements.BindParameter)
            and element.value is None
            and element.callable is None
        ):
            return (
                f"its table holds a parameter, {element.key!r}, that has no value. "
                "Give it one, as a bindparam with a value, or write the value "
                "into the filter"
            )
    return None


def validated_bindings(plan: ExecutionPlan, datasets) -> dict:
    """Check the bindings against the plan, or refuse. Nothing is sent.

    Every column the plan names is resolved by its exact name, and every
    metric is then asked of it on its own: whether the column's family may
    be measured the way that metric needs, and -- for a float -- whether the
    bind's dialect has a NaN predicate. All of it is decided here, from the
    declared columns, before a query runs.

    Args:
        plan: The plan, already validated against the capabilities.
        datasets: ``{name: SqlDataset}``.

    Returns:
        ``{name: _Bound}``.

    Raises:
        ConfigurationError: For a binding that is not a dataset with declared
            columns and a bind, or for the first thing the plan asks of a
            column that cannot be honoured. The message names the rule.
    """
    if not isinstance(datasets, Mapping):
        raise ConfigurationError(
            "dataset bindings require a mapping of dataset names to SqlDataset values"
        )
    bindings = dict(datasets)
    require_exact_bindings((metric.dataset.name for metric in plan.metrics), bindings)
    asked_by = {}
    for rule in plan.rules:
        for metric in rule.required_metrics:
            asked_by.setdefault(metric, rule.rule_id)
    checked = {}
    for name, dataset in bindings.items():
        problem = _binding_problem(dataset)
        if problem:
            raise ConfigurationError(
                f"dataset binding {name!r} must be a SqlDataset of a table whose "
                "columns are declared and a bind (an Engine, a Connection, or "
                f"anything with a dialect and an execute); {problem}"
            )
        bind = dataset.bind
        profile = _profile(bind.dialect)
        if profile is None:
            # Nothing is verified against the kernel on a dialect that is
            # not known here, so nothing is asked of it.
            raise ConfigurationError(
                f"dataset binding {name!r} is bound to the {bind.dialect.name} "
                "dialect, which the SQL adapter does not know, so it can ask "
                f"nothing of it. It knows: {sorted(DIALECTS)}"
            )
        bound = _Bound(
            name,
            dataset,
            bind.dialect.name,
            profile.nan,
            profile.boolean_bounds,
            {},
            asked_by,
        )
        for metric in metrics_of(plan, name):
            if metric.column is not None:
                _ask_of(bound, metric.column.name, metric)
            for key in metric.group_by:
                _ask_of(bound, key.name, metric)
        checked[name] = bound
    return checked


def _ask_of(bound: _Bound, column: str, metric) -> None:
    """Ask whether one column may be measured the way one metric needs.

    A column is resolved once and asked about for every metric that names
    it: the same column may be counted, and still have no bound.
    """
    if column not in bound.columns:
        names = [str(field.name) for field in bound.dataset.table.c]
        fields = {str(field.name): field.type for field in bound.dataset.table.c}
        require_exact_column(bound.name, column, names, fields)
        field = next(item for item in bound.dataset.table.c if str(item.name) == column)
        bound.columns[column] = (field, family_of(field.type))
    field, family = bound.columns[column]
    kind = metric.kind.value
    rule = bound.asked_by.get(metric, "no rule")
    if family not in _FAMILIES:
        raise ConfigurationError(
            f"column {column!r} in dataset {bound.name!r} is of type {field.type}, "
            f"which the SQL adapter cannot measure: rule {rule!r} needs {kind} "
            "of it. A type whose values may be NaN, or how they compare, is "
            "not known to this adapter, and is declared absent"
        )
    can = _FAMILIES[family]
    if metric.kind in (MetricKind.COLUMN_MIN, MetricKind.COLUMN_MAX) and not can.bounds:
        raise ConfigurationError(
            f"column {column!r} in dataset {bound.name!r} has an unsupported type "
            f"for ranges/v1 bounds: {field.type}. rule {rule!r} needs {kind} of "
            f"it, and bounds are declared absent for a {family} column"
        )
    if can.nan and bound.nan is None:
        raise ConfigurationError(
            f"column {column!r} in dataset {bound.name!r} is a float column, and "
            f"the {bound.dialect} dialect has no way to keep NaN out of a "
            f"measurement. rule {rule!r} needs {kind} of it, which is declared "
            "absent for a float on this dialect"
        )


# ------------------------------------------------------------- the statement


def _counts(bound: _Bound, column: str, nan):
    """The condition under which a value counts: not null, and not NaN."""
    sqlalchemy = _sqlalchemy()
    field, family = bound.columns[column]
    where = field.is_not(None)
    if family == _FLOAT:
        if nan is None:
            raise ConfigurationError(
                f"column {column!r} in dataset {bound.name!r} is a float column, "
                "and this dialect has no way to keep NaN out of a measurement"
            )
        where = sqlalchemy.and_(where, sqlalchemy.not_(nan(field)))
    return where


def _counts_statement(bound: _Bound, metrics, nan) -> tuple:
    """One row: the table's row count, and a present count per column."""
    sqlalchemy = _sqlalchemy()
    labels = {}
    columns = []
    for index, metric in enumerate(metrics):
        if metric.kind is MetricKind.ROW_COUNT:
            labels[metric] = "dq_row_count"
            columns.append(sqlalchemy.func.count().label("dq_row_count"))
        else:
            # A counts/v1 metric is a row count or a present count.
            labels[metric] = f"dq_present_{index}"
            columns.append(
                sqlalchemy.func.count()
                .filter(_counts(bound, metric.column.name, nan))
                .label(labels[metric])
            )
    return sqlalchemy.select(*columns).select_from(bound.dataset.table), labels


def _ranges_statement(bound: _Bound, metrics, nan) -> tuple:
    """One row: per column, how many values it holds, and its least and greatest."""
    sqlalchemy = _sqlalchemy()
    labels = {}
    columns = []
    wanted = sorted({metric.column.name for metric in metrics if metric.column})
    for index, name in enumerate(wanted):
        where = _counts(bound, name, nan)
        field, family = bound.columns[name]
        if family == _BOOLEAN and not bound.boolean_bounds:
            # Once per column: a boolean is 0 or 1, exactly the bounds the
            # kernel takes of it, where the dialect has no min of a boolean.
            field = sqlalchemy.cast(field, sqlalchemy.Integer)
        for metric in metrics:
            if metric.column is None or metric.column.name != name:
                continue
            if metric.kind is MetricKind.COLUMN_COUNT:
                labels[metric] = f"dq_count_{index}"
                columns.append(
                    sqlalchemy.func.count().filter(where).label(labels[metric])
                )
            else:
                low = metric.kind is MetricKind.COLUMN_MIN
                labels[metric] = f"dq_{'min' if low else 'max'}_{index}"
                aggregate = (sqlalchemy.func.min if low else sqlalchemy.func.max)(field)
                columns.append(aggregate.filter(where).label(labels[metric]))
    return sqlalchemy.select(*columns).select_from(bound.dataset.table), labels


def _groups_statement(bound: _Bound, metrics, nan) -> tuple:
    """One row: per grouping, its group count and each column's least and
    greatest distinct count, each read from one grouped subquery.

    Every value is a scalar subquery of its own over a common table
    expression, so that the grouped rows are made once. A plain aggregate
    over two such subqueries would count the rows of their cross join.
    """
    sqlalchemy = _sqlalchemy()
    labels = {}
    columns = []
    for index, (grouping, group_metrics) in enumerate(
        metrics_by_grouping(metrics).items()
    ):
        keys = [bound.columns[key.name][0] for key in grouping]
        measured = sorted(
            {metric.column.name for metric in group_metrics if metric.column}
        )
        inner = [key.label(f"dq_key_{position}") for position, key in enumerate(keys)]
        inner += [
            sqlalchemy.func.count(sqlalchemy.distinct(bound.columns[name][0])).label(
                f"dq_distinct_{position}"
            )
            for position, name in enumerate(measured)
        ]
        grouped = sqlalchemy.select(*inner).group_by(*keys).cte(f"dq_grouped_{index}")
        for metric in group_metrics:
            if metric.kind is MetricKind.GROUP_COUNT:
                labels[metric] = f"dq_group_count_{index}"
                aggregate = sqlalchemy.func.count()
            else:
                position = measured.index(metric.column.name)
                low = metric.kind is MetricKind.GROUP_MIN_DISTINCT
                labels[metric] = (
                    f"dq_group_{'min' if low else 'max'}_{index}_{position}"
                )
                aggregate = (sqlalchemy.func.min if low else sqlalchemy.func.max)(
                    grouped.c[f"dq_distinct_{position}"]
                )
            columns.append(
                sqlalchemy.select(aggregate.label("dq_value"))
                .select_from(grouped)
                .scalar_subquery()
                .label(labels[metric])
            )
    return sqlalchemy.select(*columns), labels


def statement_for(plan: ExecutionPlan, bound: _Bound, dialect) -> tuple:
    """Return the one statement that answers a dataset's metrics, and its labels.

    Args:
        plan: The plan.
        bound: The dataset, as :func:`validated_bindings` returned it.
        dialect: The SQLAlchemy dialect the statement is for. It decides how
            NaN is recognised and whether a boolean is bounded directly, so
            a statement compiled for another dialect than the bind's is that
            dialect's own.

    Returns:
        ``(statement, {metric: label})``: a SQLAlchemy ``Select`` and, for
            each metric the plan asks of this dataset, the label its value
            comes back under.

    Raises:
        ConfigurationError: For a dialect the adapter does not know.
    """
    metrics = metrics_of(plan, bound.name)
    profile = _profile(dialect)
    if profile is None:
        raise ConfigurationError(
            f"dataset {bound.name!r}: no statement can be made for the "
            f"{getattr(dialect, 'name', dialect)} dialect, which the SQL adapter "
            f"does not know. It knows: {sorted(DIALECTS)}"
        )
    nan = profile.nan
    bound = dataclasses.replace(bound, boolean_bounds=profile.boolean_bounds)
    if plan.semantic_version == SEMANTIC_GROUPS_VERSION:
        return _groups_statement(bound, metrics, nan)
    if plan.semantic_version == SEMANTIC_RANGES_VERSION:
        return _ranges_statement(bound, metrics, nan)
    return _counts_statement(bound, metrics, nan)


def _text_of(plan: ExecutionPlan, bound: _Bound, dialect) -> tuple:
    """Return ``(text, labels)``: a dataset's statement, for a dialect.

    The text is the one that is logged, snapshot-tested and executed. It is
    executed as text, not as the expression it was built from, on purpose.
    Through a typed expression the DuckDB dialect hands back a decimal
    rounded to a double: it declares that the driver has no native decimal,
    so SQLAlchemy turns what the driver gave -- an exact ``Decimal`` -- into
    a ``Decimal`` again by way of a float. The value is still a ``Decimal``,
    so nothing downstream could tell. As text, the driver's own value comes
    back.

    The text is self-contained. The adapter's own SQL holds no value, but a
    bound subquery may filter on one, and such a value is written into the
    text rather than left as a parameter the text would then lack.

    """
    sqlalchemy = _sqlalchemy()
    statement, labels = statement_for(plan, bound, dialect)
    try:
        compiled = statement.compile(
            dialect=dialect, compile_kwargs={"literal_binds": True}
        )
    except sqlalchemy.exc.CompileError as error:
        raise ConfigurationError(
            f"dataset {bound.name!r}: its statement holds a parameter whose value "
            f"cannot be written into a statement: {error}"
        ) from error
    return sqlalchemy.text(str(compiled).strip()), labels


# ---------------------------------------------------------------- execution


def _run(bound: _Bound, text) -> Mapping:
    """Have the database answer one statement, and return its one row."""
    sqlalchemy = _sqlalchemy()
    bind = bound.dataset.bind
    logger.info("SQL adapter, dataset %r: %s", bound.name, str(text))
    try:
        if isinstance(bind, sqlalchemy.engine.Engine):
            with bind.connect() as connection:
                return connection.execute(text).mappings().one()
        return bind.execute(text).mappings().one()
    except sqlalchemy.exc.SQLAlchemyError as error:
        message = (
            f"dataset {bound.name!r}: the statement failed on {bound.dialect}: "
            f"{error.orig if hasattr(error, 'orig') else error}"
        )
        if not isinstance(bind, sqlalchemy.engine.Engine):
            message += (
                ". It ran in the caller's transaction, which this engine may "
                "have left aborted until it is rolled back"
            )
        raise ConfigurationError(message) from error


def _values_of(name: str, bound: _Bound, plan: ExecutionPlan, read) -> dict:
    """Run a dataset's one statement and read its row the way a semantics needs."""
    text, labels = _text_of(plan, bound, bound.dataset.bind.dialect)
    row = _run(bound, text)
    return {
        metric: read(metric, row[labels[metric]], name)
        for metric in metrics_of(plan, name)
    }


def _dataset_counts(name: str, bound: _Bound, plan: ExecutionPlan) -> dict:
    return _values_of(name, bound, plan, lambda metric, value, dataset: int(value))


def _dataset_range_metrics(name: str, bound: _Bound, plan: ExecutionPlan) -> dict:
    def read(metric, value, dataset):
        if metric.kind is MetricKind.COLUMN_COUNT:
            return int(value)
        exact = as_exact_fraction(value, dataset, metric.column.name)
        return Fraction(0) if exact is None else exact

    return _values_of(name, bound, plan, read)


def _dataset_group_metrics(name: str, bound: _Bound, plan: ExecutionPlan) -> dict:
    def read(metric, value, dataset):
        if metric.kind is MetricKind.GROUP_COUNT:
            return int(value)
        # With no group there is no bound to violate: the kernel reads 0.
        return 0 if value is None else int(value)

    return _values_of(name, bound, plan, read)


def compute_plan_values(plan: ExecutionPlan, bindings: dict) -> dict:
    """Compute every plan metric: one statement per dataset, each run once."""
    return values_by_dataset(
        plan,
        bindings,
        _dataset_counts,
        _dataset_group_metrics,
        _dataset_range_metrics,
    )


# ------------------------------------------------------------------ surface


def compile_plan(
    plan: ExecutionPlan, datasets: Mapping[str, SqlDataset], *, dialect=None
) -> dict:
    """Return ``{dataset name: SQL text}`` for a plan. Nothing is sent.

    Args:
        plan: The plan.
        datasets: ``{name: SqlDataset}``, validated here first.
        dialect: A SQLAlchemy dialect to compile for: each bind's own when
            ``None``.

    Returns:
        The statements' text, in dataset-name order: what would be logged
        and executed, or what the given dialect would run.
    """
    checked = validated_bindings(plan, datasets)
    targets = {
        name: dialect or bound.dataset.bind.dialect for name, bound in checked.items()
    }
    return {
        name: _text_of(plan, checked[name], targets[name])[0].text
        for name in sorted(checked)
    }


def execute_plan(
    plan: ExecutionPlan, datasets: Mapping[str, SqlDataset]
) -> tuple[CheckOutcome, ...]:
    """Compute the plan's metrics with SQL and evaluate exact outcomes."""
    return execute_plan_with(plan, datasets, _ADAPTER)


def execute_envelope(
    envelope, datasets: Mapping[str, SqlDataset]
) -> tuple[CheckOutcome, ...]:
    """Execute every subplan of a :class:`~dq.envelope.RulesetEnvelope` with SQL.

    Mixed-semantics rulesets get one entry point, as for every adapter
    (:func:`dq.compute_support.execute_envelope_with`).
    """
    return execute_envelope_with(envelope, datasets, _ADAPTER)


#: What this module offers a plan: its capabilities, its binding check, its computation.
_ADAPTER = ComputeAdapter(capabilities_for, validated_bindings, compute_plan_values)
