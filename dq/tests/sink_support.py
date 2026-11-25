# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""A real database for the durable SQL sink's tests (SPEC-007).

SQLite gives every statement a real database to succeed or fail against. Two
settings make it stand in for PostgreSQL honestly: the evidence tables live
in a ``dq`` schema that is attached to each connection, so the qualified
names are the ones PostgreSQL sees; and the driver is stopped from managing
``BEGIN`` itself, which is SQLAlchemy's documented workaround for pysqlite.
Without it a savepoint released before the caller's first write commits what
the caller may still roll back.
"""

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

from pathlib import Path


def sqlite_engine(directory: Path) -> Engine:
    """Return a file-backed engine whose ``dq`` schema is a second database."""
    engine = create_engine(f"sqlite:///{directory / 'evidence.db'}")

    @event.listens_for(engine, "connect")
    def prepare(dbapi_connection, _record):
        cursor = dbapi_connection.cursor()
        cursor.execute(f"ATTACH DATABASE '{directory / 'dq.db'}' AS dq")
        cursor.close()
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def begin(connection):
        connection.exec_driver_sql("BEGIN")

    return engine


def recorded(engine: Engine) -> list:
    """Return the list every statement sent through ``engine`` is appended to.

    Each entry is ``(sql, parameters, executemany)`` as the driver receives
    them.
    """
    statements = []

    def capture(_connection, _cursor, statement, parameters, _context, executemany):
        statements.append((statement, parameters, executemany))

    event.listen(engine, "before_cursor_execute", capture)
    return statements


def provision(engine: Engine, statements) -> None:
    """Apply the DDL ``SqlAlchemyOutcomeSink.ddl_statements()`` returns."""
    with engine.begin() as connection:
        for statement in statements:
            connection.exec_driver_sql(statement)


def rows(engine: Engine, sql: str, **parameters) -> list:
    """Return every row of a query as a dictionary, read on a new connection."""
    with engine.connect() as connection:
        return [
            dict(row) for row in connection.execute(text(sql), parameters).mappings()
        ]
