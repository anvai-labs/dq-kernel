# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The durable SQL sink against a real PostgreSQL (ADR-006, SPEC-007).

PostgreSQL is the certified lane, and this is the one place the sink runs on
it rather than being compiled for it or simulated. The module runs when two
things are present and is skipped otherwise: a PostgreSQL driver, and the
URL of a database it may create and drop schemas in::

    DQ_TEST_POSTGRES_URL=postgresql://postgres@localhost:5432/postgres \\
        pytest dq/tests/test_sqlalchemy_sink_postgres.py

Each test gets a schema of its own and drops it afterwards.
"""

import importlib.util
import json
import os
import uuid
from datetime import datetime
from decimal import Decimal

import pytest

URL = os.environ.get("DQ_TEST_POSTGRES_URL", None)
HAS_DRIVER = any(importlib.util.find_spec(name) for name in ("psycopg", "psycopg2"))

pytestmark = pytest.mark.skipif(
    URL is None or not HAS_DRIVER,
    reason="needs DQ_TEST_POSTGRES_URL and an installed PostgreSQL driver",
)

pytest.importorskip("sqlalchemy", reason="the durable sink needs the sqlalchemy extra")

from sqlalchemy import event, text
from sqlalchemy.exc import IntegrityError

from dq.exceptions import RepositoryError
from dq.sqlalchemy_sink import SqlAlchemyOutcomeSink, engine_for
from dq.tests.helpers import FakeDataFrame
from dq.tests.sink_support import provision, rows

ARTIFACT = FakeDataFrame(
    [{"check": "a", "success": True}, {"check": "b", "success": False}]
)
FAR_AGO = 100
RECENTLY = 1_700_000_000_000


def qualified(schema, table):
    return f'"{schema}".{table}'


def select(engine, schema, columns, table="runs", where="", order=""):
    """Read a table of the test's schema on a connection of its own."""
    return rows(
        engine,
        f"SELECT {columns} FROM {qualified(schema, table)} {where} {order}".strip(),
    )


def write_run(engine, schema, namespace, run_key, checks=("a",)):
    sink = SqlAlchemyOutcomeSink(
        engine, dataset="bars", schema=schema, namespace=namespace, arrow=False
    )
    with sink.run_scope(run_key, {"rule": "r"}):
        sink.save(
            FakeDataFrame([{"check": check} for check in checks]), "verifications"
        )


@pytest.fixture
def database():
    """An engine for the test database, and a schema nobody else is using."""
    engine = engine_for(URL)
    schema = f"dq_test_{uuid.uuid4().hex[:12]}"
    with engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    yield engine, schema
    with engine.begin() as connection:
        connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
    engine.dispose()


@pytest.fixture
def sink(database):
    engine, schema = database
    sink = SqlAlchemyOutcomeSink(
        engine,
        dataset="bars",
        schema=schema,
        namespace="team_a",
        retention_days=30,
        arrow=False,
    )
    provision(engine, sink.ddl_statements())
    return sink


def test_the_ddl_applies_twice_and_the_views_answer(database):
    engine, schema = database
    sink = SqlAlchemyOutcomeSink(engine, dataset="bars", schema=schema, arrow=False)
    provision(engine, sink.ddl_statements())
    provision(engine, sink.ddl_statements())
    assert sink.verify_schema() == {"runs": 0, "artifacts": 0}

    write_run(engine, schema, "team_a", 100)
    write_run(engine, schema, "team_a", 300)
    write_run(engine, schema, "team_b", 200)
    other = SqlAlchemyOutcomeSink(
        engine, dataset="other", schema=schema, namespace="team_a", arrow=False
    )
    with other.run_scope(250, {}):
        other.save(FakeDataFrame([{"check": "x"}]), "verifications")

    completed = select(engine, schema, "namespace, run_key", "runs_completed")
    assert sorted((row["namespace"], row["run_key"]) for row in completed) == [
        ("team_a", 100),
        ("team_a", 250),
        ("team_a", 300),
        ("team_b", 200),
    ]
    latest = select(
        engine,
        schema,
        "namespace, dataset, run_key",
        "latest_per_dataset",
        order="ORDER BY dataset, namespace",
    )
    assert latest == [
        {"namespace": "team_a", "run_key": 300, "dataset": "bars"},
        {"namespace": "team_b", "run_key": 200, "dataset": "bars"},
        {"namespace": "team_a", "run_key": 250, "dataset": "other"},
    ]


def test_columns_have_the_certified_types(database):
    engine, schema = database
    sink = SqlAlchemyOutcomeSink(engine, dataset="bars", schema=schema, arrow=False)
    provision(engine, sink.ddl_statements())
    with engine.connect() as connection:
        columns = connection.execute(
            text(
                "SELECT table_name, column_name, data_type, column_default "
                "FROM information_schema.columns "
                "WHERE table_schema = :schema ORDER BY table_name, ordinal_position"
            ),
            {"schema": schema},
        ).mappings()
        found = {
            (row["table_name"], row["column_name"]): (
                row["data_type"],
                row["column_default"],
            )
            for row in columns
        }
    assert found[("runs", "identity")][0] == "jsonb"
    assert found[("runs", "identity")][1] == "'{}'::jsonb"
    assert found[("artifacts", "payload")] == ("jsonb", None)
    assert found[("runs", "run_key")][0] == "bigint"
    assert found[("artifacts", "seq")][0] == "integer"
    assert "default" in found[("runs", "namespace")][1]


def test_a_payload_lands_as_jsonb_and_is_strict_json(sink, database):
    engine, schema = database
    sink.save(
        FakeDataFrame(
            [
                {
                    "value": float("nan"),
                    "amount": Decimal("1.50"),
                    "at": datetime(2026, 11, 25, 10),
                    "ok": True,
                    "nested": {"a": [1, 2]},
                }
            ]
        ),
        "metrics",
        1,
    )
    (stored,) = select(
        engine,
        schema,
        "payload::text AS payload, jsonb_typeof(payload) AS kind",
        "artifacts",
    )
    assert stored["kind"] == "object"
    assert json.loads(stored["payload"]) == {
        "value": None,
        "amount": "1.50",
        "at": "2026-11-25T10:00:00",
        "ok": True,
        "nested": {"a": [1, 2]},
    }
    # Why the payload is made strict first: the column refuses what Python's
    # json module writes for a NaN.
    with engine.connect() as connection:
        with pytest.raises(Exception, match="invalid input syntax for type json"):
            connection.execute(
                text(
                    f"INSERT INTO {qualified(schema, 'artifacts')} (payload) VALUES (:p)"
                ),
                {"p": '{"value": NaN}'},
            )


def test_a_run_commits_and_a_failed_run_leaves_nothing(sink, database):
    engine, schema = database
    with sink.run_scope(1000, {"first": True}):
        sink.save(ARTIFACT, "verifications", key=5)
    assert select(
        engine,
        schema,
        "namespace, run_key, dataset, started_millis, status, identity::text AS identity",
    ) == [
        {
            "namespace": "team_a",
            "run_key": 1000,
            "dataset": "bars",
            "started_millis": 1000,
            "status": "completed",
            "identity": '{"first": true}',
        }
    ]
    assert select(engine, schema, "run_key, seq", "artifacts") == [
        {"run_key": 1000, "seq": 0},
        {"run_key": 1000, "seq": 1},
    ]
    with pytest.raises(RuntimeError, match="engine failed"):
        with sink.run_scope(2000):
            sink.save(ARTIFACT, "verifications")
            raise RuntimeError("engine failed")
    assert [row["run_key"] for row in select(engine, schema, "run_key")] == [1000]
    assert [
        row["run_key"] for row in select(engine, schema, "run_key", "artifacts")
    ] == [1000, 1000]


def test_repeating_a_run_adds_nothing(sink, database):
    engine, schema = database
    for identity in ({"first": True}, {"first": False}):
        with sink.run_scope(1000, identity):
            sink.save(ARTIFACT, "verifications")
    assert select(engine, schema, "count(*) AS n", "artifacts") == [{"n": 2}]
    assert select(engine, schema, "identity::text AS identity") == [
        {"identity": '{"first": true}'}
    ]


def test_a_caller_s_connection_savepoints_and_rollback(database):
    engine, schema = database
    with engine.connect() as connection:
        sink = SqlAlchemyOutcomeSink(
            connection, dataset="bars", schema=schema, namespace="team_a", arrow=False
        )
        with connection.begin():
            for statement in sink.ddl_statements():
                connection.exec_driver_sql(statement)

        # A scope that committed is still the caller's to roll back.
        transaction = connection.begin()
        connection.execute(text(f"SELECT count(*) FROM {qualified(schema, 'runs')}"))
        with sink.run_scope(1000):
            sink.save(ARTIFACT, "verifications")
        transaction.rollback()

        # A scope that failed leaves the caller's own work untouched.
        with connection.begin():
            connection.execute(
                text(
                    f"INSERT INTO {qualified(schema, 'artifacts')} "
                    "(namespace, run_key, artifact_type, seq, dataset, payload) "
                    "VALUES ('caller', 1, 'metrics', 0, 'bars', '{}')"
                )
            )
            with pytest.raises(ValueError, match="engine failed"):
                with sink.run_scope(2000):
                    sink.save(ARTIFACT, "verifications")
                    raise ValueError("engine failed")
        assert connection.closed is False
    assert select(
        engine, schema, "namespace, run_key", "artifacts", order="ORDER BY run_key"
    ) == [{"namespace": "caller", "run_key": 1}]
    assert select(engine, schema, "run_key") == []


def test_prune_deletes_a_namespace_s_old_artifacts_then_its_runs_and_nothing_else(
    sink, database
):
    engine, schema = database
    write_run(engine, schema, "team_a", FAR_AGO, ("old-a", "old-b"))
    write_run(engine, schema, "team_a", RECENTLY, ("new-a",))
    write_run(engine, schema, "team_b", FAR_AGO, ("other-old",))
    assert sink.prune(before_millis=1000) == (2, 1)
    assert select(engine, schema, "namespace, run_key", order="ORDER BY namespace") == [
        {"namespace": "team_a", "run_key": RECENTLY},
        {"namespace": "team_b", "run_key": FAR_AGO},
    ]
    assert select(
        engine, schema, "namespace, run_key", "artifacts", order="ORDER BY namespace"
    ) == [
        {"namespace": "team_a", "run_key": RECENTLY},
        {"namespace": "team_b", "run_key": FAR_AGO},
    ]
    # Only the other namespace's old run is left before the cutoff; the
    # recent one, newer than it, stays.
    assert sink.prune(before_millis=1000, all_namespaces=True) == (1, 1)
    assert sink.verify_schema() == {"runs": 1, "artifacts": 1}


def test_a_missing_table_is_named_and_a_failure_is_wrapped(database):
    engine, schema = database
    sink = SqlAlchemyOutcomeSink(engine, dataset="bars", schema=schema, arrow=False)
    with pytest.raises(RepositoryError, match="does not exist; apply ddl_statements"):
        sink.verify_schema()
    provision(engine, sink.ddl_statements())
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP TABLE {qualified(schema, 'artifacts')}")
    with pytest.raises(
        RepositoryError, match=r"evidence table .*\.artifacts does not exist"
    ):
        sink.verify_schema()
    with pytest.raises(RepositoryError, match="retention_days"):
        SqlAlchemyOutcomeSink(engine, dataset="bars", schema=schema).prune(
            before_millis=1
        )


def test_a_save_that_fails_halfway_stores_nothing(database):
    """The first row fits and the second does not. Neither may remain."""
    engine, schema = database
    sink = SqlAlchemyOutcomeSink(
        engine, dataset="bars", schema=schema, namespace="team_a", arrow=False
    )
    provision(engine, sink.ddl_statements())
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f"ALTER TABLE {qualified(schema, 'artifacts')} "
            "ADD CONSTRAINT one_row CHECK (seq < 1)"
        )
    with pytest.raises(IntegrityError, match="one_row"):
        sink.save(ARTIFACT, "verifications", 42)
    assert select(engine, schema, "run_key", "artifacts") == []


def test_a_prune_that_fails_halfway_deletes_nothing(database):
    """Artifacts go before runs. When the runs cannot be deleted, the
    artifacts must still be there: one transaction, all or nothing."""
    engine, schema = database
    sink = SqlAlchemyOutcomeSink(
        engine,
        dataset="bars",
        schema=schema,
        namespace="team_a",
        retention_days=30,
        arrow=False,
    )
    provision(engine, sink.ddl_statements())
    write_run(engine, schema, "team_a", FAR_AGO, ("old",))
    assert len(select(engine, schema, "run_key")) == 1

    def fail_on_runs(connection, cursor, statement, parameters, context, executemany):
        # A lowercase schema is not quoted on PostgreSQL, its dialect being
        # the reason the quoting rule exists.
        if statement.startswith(f"DELETE FROM {schema}.runs"):
            raise RuntimeError("connection reset")

    event.listen(engine, "before_cursor_execute", fail_on_runs)
    try:
        with pytest.raises(RuntimeError, match="connection reset"):
            sink.prune(before_millis=1000)
    finally:
        event.remove(engine, "before_cursor_execute", fail_on_runs)
    assert len(select(engine, schema, "run_key")) == 1
    assert len(select(engine, schema, "run_key", "artifacts")) == 1


def test_mixed_case_names_are_quoted_everywhere_they_are_used(database):
    engine, _ = database
    schema = f"DqCase_{uuid.uuid4().hex[:8]}"
    with engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    try:
        sink = SqlAlchemyOutcomeSink(
            engine,
            dataset="bars",
            schema=schema,
            runs_table="Runs",
            artifacts_table="Artifacts",
            namespace="TeamA",
            retention_days=30,
            arrow=False,
        )
        provision(engine, sink.ddl_statements())
        with sink.run_scope(1000, {"cased": True}):
            sink.save(ARTIFACT, "verifications")
        assert sink.verify_schema() == {"Runs": 1, "Artifacts": 2}
        assert (
            rows(
                engine,
                f'SELECT namespace, run_key FROM "{schema}"."Artifacts" ORDER BY seq',
            )
            == [{"namespace": "TeamA", "run_key": 1000}] * 2
        )
        assert sink.prune(before_millis=10**15) == (2, 1)
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')


def test_arrow_rows_reach_postgresql(database):
    pa = pytest.importorskip("pyarrow")
    engine, schema = database
    sink = SqlAlchemyOutcomeSink(engine, dataset="bars", schema=schema, arrow=True)
    provision(engine, sink.ddl_statements())
    sink.save(pa.table({"check": ["a", "b"], "success": [True, False]}), "metrics", 1)
    assert select(engine, schema, "seq, payload::text AS payload", "artifacts") == [
        {"seq": 0, "payload": '{"check": "a", "success": true}'},
        {"seq": 1, "payload": '{"check": "b", "success": false}'},
    ]
