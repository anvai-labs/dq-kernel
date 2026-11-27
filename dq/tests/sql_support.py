# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""DuckDB tables for the shared semantic corpus, for the SQL adapter's tests.

Rows are loaded exactly. A decimal is bound as text and cast in the
statement: bound as a parameter, a ``Decimal`` reaches DuckDB already
rounded to a double, even when the SQL casts the parameter to the column's
type -- which would turn the corpus's case about two decimals a double
cannot tell apart into a case about one of them.
"""

import decimal

import sqlalchemy as sa

from dq.sdk.fixtures import SemanticCase, python_rows

TYPES = {
    "string": sa.String,
    "long": sa.BigInteger,
    "double": sa.Double,
    "boolean": sa.Boolean,
}


def column_type(kind: str):
    """Return the declared SQLAlchemy type a corpus column type names."""
    if kind in TYPES:
        return TYPES[kind]()
    precision, scale = kind[len("decimal(") : -1].split(",")
    return sa.Numeric(int(precision), int(scale))


def corpus_table(case: SemanticCase, metadata, name: str = "ds") -> sa.Table:
    """Declare a corpus case as a table whose columns carry their types."""
    return sa.Table(
        name,
        metadata,
        *[sa.Column(column, column_type(kind)) for column, kind in case.columns],
    )


def insert(table: sa.Table, rows, bind) -> None:
    """Insert ``rows`` into ``table`` exactly, through an ``Engine``."""
    if not rows:
        return
    dialect = bind.dialect
    quote = dialect.identifier_preparer.quote
    columns = list(table.c)

    def place(column):
        if isinstance(column.type, sa.Numeric) and not isinstance(
            column.type, sa.Float
        ):
            exact = column.type.compile(dialect=dialect)
            return f"CAST(:{column.key} AS {exact})"
        return f":{column.key}"

    statement = sa.text(
        f"INSERT INTO {quote(table.name)} VALUES ({', '.join(place(c) for c in columns)})"
    )
    values = [
        {
            column.key: (
                str(row[column.key])
                if isinstance(row[column.key], decimal.Decimal)
                else row[column.key]
            )
            for column in columns
        }
        for row in rows
    ]
    if isinstance(bind, sa.engine.Engine):
        with bind.begin() as connection:
            connection.execute(statement, values)
    else:
        bind.execute(statement, values)


def load(case: SemanticCase, engine, name: str = "ds") -> sa.Table:
    """Create a corpus case's table on ``engine`` with its rows in it."""
    metadata = sa.MetaData()
    table = corpus_table(case, metadata, name)
    metadata.create_all(engine)
    insert(
        table,
        [dict(zip(case.column_names, row)) for row in python_rows(case)],
        engine,
    )
    return table
