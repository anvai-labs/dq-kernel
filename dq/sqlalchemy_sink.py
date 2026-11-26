# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Durable SQL outcome sink over SQLAlchemy Core (ADR-006, ADR-010, SPEC-007).

A SQLAlchemy ``Engine`` or ``Connection`` is injected; the deployment owns
the URL, the credentials and the database driver. One run is one
transaction: saves inside :meth:`SqlAlchemyOutcomeSink.run_scope` join a
single transaction that commits on clean exit and rolls back on any
exception, so a partial run never becomes visible evidence. Writes are
append-only and idempotent: primary keys on namespace plus run key and
artifact sequence, with ``ON CONFLICT DO NOTHING``, mean first write wins
and retries are safe.

PostgreSQL is the certified lane, and the only one tested against a real
server. The statements are written in forms other databases also accept and
the DDL is compiled for the engine's own dialect, but nothing else is
certified.

This module imports SQLAlchemy, so the framework imports it only when a SQL
sink is configured. pyarrow is never imported by the framework: when it is
installed, an artifact's rows are read through Arrow.

Threading: one sink instance owns one run scope at a time and is not
thread-safe; share instances across threads only between scopes.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import time

from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    bindparam,
    create_engine,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Connection, Engine, make_url
from sqlalchemy.exc import ArgumentError, NoSuchModuleError
from sqlalchemy.schema import CreateTable
from sqlalchemy.types import JSON, TypeDecorator

from dq.exceptions import ConfigurationError, RepositoryError
from dq.identifiers import ColumnName, TableName
from dq.sinks import OutcomeSink, WriteResult
from dq.utils.sanitization import to_strict_json

DEFAULT_DSN_VARIABLE = "DQ_POSTGRES_DSN"
#: psycopg 3 is the driver this sink has always used; a URL that names none
#: would otherwise mean psycopg 2 to SQLAlchemy.
BARE_POSTGRES_DRIVER = "postgresql+psycopg"
#: How to install each PostgreSQL driver a URL can name.
DRIVER_HINTS = {
    "psycopg": 'pip install "psycopg[binary]"',
    "psycopg2": "pip install psycopg2-binary",
}


class JsonPayload(TypeDecorator):
    """A JSON document column: ``JSONB`` on PostgreSQL, ``JSON`` elsewhere."""

    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect):
        target = JSONB() if dialect.name == "postgresql" else JSON()
        return dialect.type_descriptor(target)


def _psycopg_connector(dsn: str):
    """Return a connect callable that hands a libpq DSN to psycopg 3."""

    def connect():
        import psycopg

        return psycopg.connect(dsn)

    return connect


def _resolve(location: str):
    """Return the URL for ``location`` and the connect callable it needs, if any.

    ``location`` is a SQLAlchemy URL (``postgresql+psycopg://user@host/db``,
    ``sqlite:///evidence.db``...). A bare ``postgresql://`` URL means
    psycopg 3, as it always did for this sink. A string that is not a URL is
    taken as a libpq DSN, which can name no driver, so psycopg 3 reads it
    through the returned callable.
    """
    try:
        url = make_url(location)
    except ArgumentError:
        return make_url(BARE_POSTGRES_DRIVER + "://"), _psycopg_connector(location)
    if url.drivername == "postgresql":
        url = url.set(drivername=BARE_POSTGRES_DRIVER)
    return url, None


def engine_for(location: str) -> Engine:
    """Return an engine for ``location``, as :func:`_resolve` reads it.

    The database driver is imported here, when the engine is created, so a
    missing one is reported when the sink is built, before any rule runs.

    Raises:
        ConfigurationError: When no installed dialect or driver matches.
    """
    url, creator = _resolve(location)
    # A creator is passed only when there is one: `create_engine` installs
    # whatever it is given as the pool's way to connect, None included.
    options = {"creator": creator} if creator is not None else {}
    try:
        return create_engine(url, **options)
    except ModuleNotFoundError as error:
        # The driver is named by the URL. The missing module is only the
        # detail: for a driver that is half installed it is a submodule.
        driver = url.drivername.split("+")[-1]
        raise ConfigurationError(
            f"the database URL names the driver {driver!r}, which is not "
            f"installed or is incomplete ({error}); install it in this "
            f"environment ({DRIVER_HINTS.get(driver, 'pip install ' + driver)})"
        ) from error
    except NoSuchModuleError as error:
        raise ConfigurationError(
            "the database URL names the dialect "
            f"{url.get_backend_name()!r}, which no installed SQLAlchemy "
            "dialect provides"
        ) from error


def _pyarrow():
    """Return the pyarrow module when it is installed, else ``None``."""
    try:
        import pyarrow
    except ImportError:
        return None
    return pyarrow


def _arrow_table(arrow, df):
    """Return ``df`` as a pyarrow table when Arrow can read it, else ``None``.

    A pyarrow table is returned as it is. A Spark DataFrame is read through
    the Arrow export it has: Spark Connect and PySpark 4 offer a public
    ``toArrow()``; classic PySpark 3.5 offers only ``_collect_as_arrow()``,
    which is used when it is there. Anything else that exports the Arrow
    stream interface (Polars, pandas, DuckDB, DataFusion) is read through
    that. None of these libraries is imported: each is a method on the
    object, found by name.
    """
    if isinstance(df, arrow.Table):
        return df
    to_arrow = getattr(df, "toArrow", None)
    if callable(to_arrow):
        return to_arrow()
    collect = getattr(df, "_collect_as_arrow", None)
    if callable(collect):
        batches = collect()
        # An empty frame yields no batch, so no schema to build a table from.
        return arrow.Table.from_batches(batches) if batches else arrow.table({})
    if hasattr(df, "__arrow_c_stream__"):
        return arrow.table(df)
    return None


def _spelled_like_frame_rows(value):
    """Spell a value read through Arrow as a frame's own rows spell it.

    Arrow hands a timestamp back with the time zone it was read in, and
    binary data as ``bytes``. A Spark frame's own rows give the wall time
    without a zone, and a ``bytearray``. A stored payload must not depend on
    whether pyarrow happened to be installed, so Arrow's spelling is brought
    to the frame's.
    """
    if isinstance(value, dict):
        return {name: _spelled_like_frame_rows(item) for name, item in value.items()}
    if isinstance(value, list):
        return [_spelled_like_frame_rows(item) for item in value]
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, bytes):
        return bytearray(value)
    return value


class SqlAlchemyOutcomeSink(OutcomeSink):
    """Persist runs and bounded artifacts durably in a SQL database.

    Args:
        bind: A SQLAlchemy ``Engine``, which gives each use its own
            connection, or a ``Connection`` the caller owns. The sink never
            closes a connection it was given. Inside a transaction the
            caller already began, a run scope is a savepoint; otherwise it
            is the transaction this sink commits or rolls back.
        dataset: Dataset label stamped onto every row.
        schema: Schema holding the evidence tables.
        runs_table / artifacts_table: Evidence table names.
        namespace: Tenant namespace stamped onto every row (SPEC-006).
        retention_days: Days of evidence to keep; pruning requires it.
        arrow: Read artifact rows through pyarrow when it is installed.
            The fallback is the frame's own rows, as before.
    """

    def __init__(
        self,
        bind,
        dataset: str,
        schema: str = "dq",
        runs_table: str = "runs",
        artifacts_table: str = "artifacts",
        namespace: str = "default",
        retention_days: int | None = None,
        arrow: bool = True,
    ):
        if not isinstance(bind, (Engine, Connection)):
            raise ConfigurationError(
                "the durable sink takes a SQLAlchemy Engine or Connection, not "
                f"{type(bind).__name__}; a DB-API connect callable is no longer "
                "supported -- build one with sqlalchemy.create_engine(url)"
            )
        self._bind = bind
        self._preparer = bind.dialect.identifier_preparer
        self._json = JsonPayload()
        self._dataset = dataset
        self._namespace = ColumnName.parse(namespace, label="tenant namespace")
        if retention_days is not None and (
            type(retention_days) is not int or retention_days < 1
        ):
            raise ConfigurationError(
                "retention_days must be a positive integer when provided"
            )
        self._retention_days = retention_days
        self._arrow = _pyarrow() if arrow else None
        self._active = None  # (connection, run_key) while a scope is open

        # Names are validated here, then quoted as the dialect requires. A
        # Column belongs to one table, so each table builds its own.
        self._schema = TableName.parse(schema, label="database schema", max_parts=1)
        metadata = MetaData()

        def key_columns():
            return [
                Column(
                    "namespace", Text, nullable=False, server_default=text("'default'")
                ),
                Column("run_key", BigInteger, nullable=False),
            ]

        self._runs = Table(
            ColumnName.parse(runs_table, label="runs table").value,
            metadata,
            *key_columns(),
            Column("dataset", Text, nullable=False),
            Column("started_millis", BigInteger, nullable=False),
            Column("status", Text, nullable=False),
            Column(
                "identity", JsonPayload, nullable=False, server_default=text("'{}'")
            ),
            PrimaryKeyConstraint("namespace", "run_key"),
            schema=str(self._schema),
        )
        self._artifacts = Table(
            ColumnName.parse(artifacts_table, label="artifacts table").value,
            metadata,
            *key_columns(),
            Column("artifact_type", Text, nullable=False),
            Column("seq", Integer, nullable=False),
            Column("dataset", Text, nullable=False),
            Column("payload", JsonPayload, nullable=False),
            PrimaryKeyConstraint("namespace", "run_key", "artifact_type", "seq"),
            schema=str(self._schema),
        )
        self._statements()

    @classmethod
    def from_config(cls, repoconfig, env=None) -> SqlAlchemyOutcomeSink:
        """Build the sink from ``repository`` configuration.

        The database location is read from the environment variable named by
        ``dsn_env`` (default ``DQ_POSTGRES_DSN``) and its absence fails
        closed: credentials never live in configuration files. Its value is
        a SQLAlchemy URL, or a libpq DSN; see :func:`engine_for`.
        ``arrow = false`` turns off reading artifacts through pyarrow.
        """
        import os

        environment = env if env is not None else os.environ
        variable = repoconfig.get("dsn_env", None) or DEFAULT_DSN_VARIABLE
        location = environment.get(variable, None)
        if not location:
            raise ConfigurationError(
                f"environment variable {variable!r} must contain the database "
                "URL for the durable sink"
            )
        dataset = repoconfig.get("dataset", None)
        if dataset is None:
            raise ConfigurationError("repository dataset name is required")
        return cls(
            engine_for(location),
            dataset=dataset,
            schema=repoconfig.get("schema", None) or "dq",
            runs_table=repoconfig.get("runs_table", None) or "runs",
            artifacts_table=repoconfig.get("artifacts_table", None) or "artifacts",
            namespace=repoconfig.get("namespace", None) or "default",
            retention_days=repoconfig.get("retention_days", None),
            arrow=repoconfig.get("arrow", True) is not False,
        )

    # ------------------------------------------------------------ statements

    def _column(self, table, name, qualifier=None) -> str:
        """Render one column, qualified, quoted as the dialect requires."""
        rendered = self._preparer.format_column(table.c[name])
        return f"{qualifier}.{rendered}" if qualifier else rendered

    def _statements(self):
        """Build this sink's statements once, for the engine's dialect."""
        preparer, runs, artifacts = self._preparer, self._runs, self._artifacts
        qualified_runs = preparer.format_table(runs)
        qualified_artifacts = preparer.format_table(artifacts)

        self._insert_run = text(
            f"INSERT INTO {qualified_runs} ({self._columns(runs)}) VALUES "
            "(:namespace, :run_key, :dataset, :started_millis, :status, :identity) "
            f"ON CONFLICT ({self._columns(runs, 'namespace', 'run_key')}) DO NOTHING"
        ).bindparams(bindparam("identity", type_=self._json))
        self._insert_artifacts = text(
            f"INSERT INTO {qualified_artifacts} ({self._columns(artifacts)}) VALUES "
            "(:namespace, :run_key, :artifact_type, :seq, :dataset, :payload) "
            "ON CONFLICT ("
            + self._columns(artifacts, "namespace", "run_key", "artifact_type", "seq")
            + ") DO NOTHING"
        ).bindparams(bindparam("payload", type_=self._json))

        # Artifacts of a run go before the run. A correlated EXISTS rather
        # than PostgreSQL's DELETE ... USING, so the statement means the
        # same thing on another database.
        target = preparer.format_table(artifacts, use_schema=False)

        def delete_artifacts(scope: str) -> str:
            linked = " AND ".join(
                f"r.{preparer.format_column(runs.c[name])} = "
                f"{target}.{preparer.format_column(artifacts.c[name])}"
                for name in ("namespace", "run_key")
            )
            return (
                f"DELETE FROM {qualified_artifacts} WHERE EXISTS ("
                f"SELECT 1 FROM {qualified_runs} AS r "
                f"WHERE {linked} AND {scope}r.started_millis < :before_millis)"
            )

        namespace_column = preparer.format_column(runs.c["namespace"])
        started_column = preparer.format_column(runs.c["started_millis"])
        self._delete_artifacts = {
            True: text(delete_artifacts(f"r.{namespace_column} = :namespace AND ")),
            False: text(delete_artifacts("")),
        }
        self._delete_runs = {
            scoped: text(
                f"DELETE FROM {qualified_runs} WHERE "
                + (f"{namespace_column} = :namespace AND " if scoped else "")
                + f"{started_column} < :before_millis"
            )
            for scoped in (True, False)
        }
        self._counts = {
            table: select(func.count()).select_from(table)
            for table in (runs, artifacts)
        }

    def _columns(self, table, *names) -> str:
        names = names or tuple(table.c.keys())
        return ", ".join(self._column(table, name) for name in names)

    # ------------------------------------------------------------------- DDL

    def ddl_statements(self) -> list[str]:
        """Reference DDL for operators; provisioning belongs to deployment.

        Compiled for the engine's dialect, so identifiers are quoted where
        that database requires it. Includes the tenant namespace column and
        the admission views (SPEC-001/006): ``runs_completed`` and
        ``latest_per_dataset``. Re-running provisioning is idempotent.
        """
        dialect = self._bind.dialect
        statements = [
            " ".join(
                str(
                    CreateTable(table, if_not_exists=True).compile(dialect=dialect)
                ).split()
            )
            for table in (self._runs, self._artifacts)
        ]
        return statements + self._views()

    def _views(self) -> list[str]:
        """The two admission views, in the dialect's own ``CREATE VIEW`` form."""
        dialect = self._bind.dialect
        verb = (
            "CREATE VIEW IF NOT EXISTS"
            if dialect.name == "sqlite"
            else "CREATE OR REPLACE VIEW"
        )
        runs = self._preparer.format_table(self._runs)

        def qualified(name: str) -> str:
            return self._column(self._runs, name, "r")

        partition = ", ".join(qualified(name) for name in ("namespace", "dataset"))
        order = ", ".join(
            f"{qualified(name)} DESC" for name in ("started_millis", "run_key")
        )
        status = self._column(self._runs, "status")
        definitions = {
            "runs_completed": (f"SELECT * FROM {runs} WHERE {status} = 'completed'"),
            "latest_per_dataset": (
                f"SELECT * FROM (SELECT r.*, ROW_NUMBER() OVER ("
                f"PARTITION BY {partition} ORDER BY {order}) AS rn "
                f"FROM {runs} AS r WHERE {qualified('status')} = 'completed') "
                "ranked WHERE ranked.rn = 1"
            ),
        }
        statements = []
        for name, definition in definitions.items():
            view = Table(name, self._runs.metadata, schema=self._runs.schema)
            statements.append(
                f"{verb} {self._preparer.format_table(view)} AS {definition}"
            )
        return statements

    # ------------------------------------------------------------------- use

    @contextmanager
    def _connection(self):
        """Yield a connection: the injected one, or one this call owns."""
        if isinstance(self._bind, Connection):
            yield self._bind
        else:
            with self._bind.connect() as connection:
                yield connection

    def _transaction(self, connection):
        """Begin a transaction, or a savepoint inside the caller's own."""
        return (
            connection.begin_nested()
            if connection.in_transaction()
            else connection.begin()
        )

    def verify_schema(self) -> dict:
        """Check both evidence tables exist and return their row counts.

        Fails closed with ``RepositoryError`` when either is absent.
        """
        tables = (self._runs, self._artifacts)
        try:
            with self._connection() as connection:
                inspector = inspect(connection)
                for table in tables:
                    if not inspector.has_table(table.name, schema=table.schema):
                        raise RepositoryError(
                            f"evidence table {table.schema}.{table.name} does not "
                            "exist; apply ddl_statements() first"
                        )
                return {table.name: self._count(connection, table) for table in tables}
        except RepositoryError:
            raise
        except Exception as error:
            raise RepositoryError(
                f"evidence schema check failed for {self._runs.schema!r}: {error}"
            ) from error

    def _count(self, connection, table) -> int:
        return int(connection.execute(self._counts[table]).scalar_one())

    def prune(
        self,
        before_millis: int | None = None,
        namespace: str | None = None,
        all_namespaces: bool = False,
    ) -> tuple[int, int]:
        """Delete artifacts and runs older than the cutoff.

        Deleting evidence requires explicit retention configuration
        (``retention_days``). Scope defaults to this sink's namespace; pass
        ``all_namespaces=True`` (with ``namespace=None``) to prune across
        namespaces. Artifacts are deleted before runs.

        Returns:
            (artifacts deleted, runs deleted).
        """
        if self._retention_days is None:
            raise RepositoryError(
                "pruning evidence requires explicit retention_days configuration"
            )
        if before_millis is None:
            before_millis = (
                time.time_ns() // 1_000_000 - self._retention_days * 86_400_000
            )
        scoped = namespace is not None or not all_namespaces
        parameters = {"before_millis": int(before_millis)}
        if scoped:
            parameters["namespace"] = (
                str(self._namespace) if namespace is None else str(namespace)
            )
        with self._connection() as connection:
            transaction = self._transaction(connection)
            try:
                artifacts_deleted = connection.execute(
                    self._delete_artifacts[scoped], parameters
                ).rowcount
                runs_deleted = connection.execute(
                    self._delete_runs[scoped], parameters
                ).rowcount
                transaction.commit()
            except BaseException:
                transaction.rollback()
                raise
        return int(artifacts_deleted or 0), int(runs_deleted or 0)

    @contextmanager
    def run_scope(self, run_key: int, identity=None):
        """See :meth:`dq.sinks.OutcomeSink.run_scope`."""
        with self._connection() as connection:
            transaction = self._transaction(connection)
            try:
                connection.execute(
                    self._insert_run,
                    {
                        "namespace": str(self._namespace),
                        "run_key": int(run_key),
                        "dataset": self._dataset,
                        "started_millis": int(run_key),
                        "status": "completed",
                        "identity": to_strict_json(dict(identity or {})),
                    },
                )
                previous, self._active = self._active, (connection, int(run_key))
                try:
                    yield self
                finally:
                    self._active = previous
                transaction.commit()
            except BaseException:
                transaction.rollback()
                raise

    def save(self, df, metric_type: str, key: int | None = None) -> WriteResult:
        """Persist one artifact under the authoritative run key.

        Inside a run scope the scope's run key is authoritative and the
        caller's key is advisory: all artifacts of one run join that run
        regardless of engine-side timestamps (review finding F01). Outside
        a scope an explicit key is required.
        """
        location = (
            f"{self._bind.dialect.name}:{self._preparer.format_table(self._artifacts)}"
        )
        if self._active is None and key is None:
            raise RepositoryError(
                "sinks outside a run scope require an explicit run key"
            )
        rows = self._rows(df)
        if self._active is not None:
            connection, run_key = self._active
            self._insert(connection, metric_type, run_key, rows)
            return WriteResult(metric_type, run_key, (location,))
        with self._connection() as connection:
            transaction = self._transaction(connection)
            try:
                self._insert(connection, metric_type, int(key), rows)
                transaction.commit()
            except BaseException:
                transaction.rollback()
                raise
        return WriteResult(metric_type, int(key), (location,))

    def _rows(self, df) -> list:
        """Return the artifact's rows as dictionaries, through Arrow if it can.

        Without pyarrow, or for a frame Arrow cannot read, the rows are the
        frame's own, as before; either way they are spelled the same. An
        empty artifact is an empty list and writes nothing.
        """
        if self._arrow is not None:
            table = _arrow_table(self._arrow, df)
            if table is not None:
                return _spelled_like_frame_rows(table.to_pylist())
        return [row.asDict(recursive=True) for row in df.collect()]

    def _insert(self, connection, metric_type: str, run_key: int, rows):
        if not rows:
            return
        namespace, dataset = str(self._namespace), self._dataset
        connection.execute(
            self._insert_artifacts,
            [
                {
                    "namespace": namespace,
                    "run_key": run_key,
                    "artifact_type": metric_type,
                    "seq": sequence,
                    "dataset": dataset,
                    "payload": to_strict_json(row),
                }
                for sequence, row in enumerate(rows)
            ],
        )

    @property
    def dataset(self) -> str:
        return self._dataset
