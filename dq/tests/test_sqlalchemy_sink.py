# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The durable SQL sink on a real database (ADR-006, ADR-010, SPEC-007).

Every statement here is run by a database, so one that is wrong fails the
way it would in production. The database is SQLite with a ``dq`` schema
attached (see ``sink_support``); the same statements are compiled for the
PostgreSQL dialect, the certified lane, to hold their shape there.
``test_sqlalchemy_sink_postgres.py`` runs them against a real PostgreSQL
when ``DQ_TEST_POSTGRES_URL`` names one.
"""

import json
import subprocess
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy", reason="the durable sink needs the sqlalchemy extra")

from sqlalchemy import event, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine, make_url

from dq import sqlalchemy_sink as sink_module
from dq.exceptions import ConfigurationError, RepositoryError
from dq.identifiers import IdentifierError
from dq.sqlalchemy_sink import (
    BARE_POSTGRES_DRIVER,
    DEFAULT_DSN_VARIABLE,
    SqlAlchemyOutcomeSink,
    _resolve,
    engine_for,
)
from dq.sinks import sink_from_config
from dq.tests.helpers import FakeDataFrame
from dq.tests.sink_support import provision, recorded, rows, sqlite_engine
from dq.tests.test_package_contract import _run_with_blocked_imports

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONFIG = {"type": "postgres", "dataset": "bars"}
ARTIFACT = FakeDataFrame(
    [{"check": "a", "success": True}, {"check": "b", "success": False}]
)
DAY = 86_400_000
FAR_AGO = 100
RECENTLY = 1_700_000_000_000


def reject(constant):
    """Refuse a JSON number JSON does not define: NaN, Infinity."""
    raise AssertionError(f"{constant} is not JSON")


def stored_runs(engine):
    return rows(engine, "SELECT * FROM dq.runs ORDER BY run_key")


def stored_artifacts(engine):
    return rows(engine, "SELECT * FROM dq.artifacts ORDER BY run_key, seq")


def write_run(engine, namespace, run_key, checks=("a",)):
    """Write one run in ``namespace``; its key is also its start, in millis."""
    sink = SqlAlchemyOutcomeSink(
        engine, dataset="bars", namespace=namespace, retention_days=30, arrow=False
    )
    with sink.run_scope(run_key, {"rule": "r"}):
        sink.save(
            FakeDataFrame([{"check": check} for check in checks]), "verifications"
        )


def bound(entry):
    """The values one executed statement bound, as a tuple per row."""
    statement, parameters, executemany = entry
    if executemany:
        return [
            tuple(each.values()) if isinstance(each, dict) else each
            for each in parameters
        ]
    return [tuple(parameters.values()) if isinstance(parameters, dict) else parameters]


def statements_starting_with(sent, prefix):
    return [entry for entry in sent if entry[0].startswith(prefix)]


@pytest.fixture
def engine(tmp_path):
    engine = sqlite_engine(tmp_path)
    yield engine
    engine.dispose()


@pytest.fixture
def sent(engine):
    """Every statement the database receives: (sql, parameters, executemany)."""
    return recorded(engine)


@pytest.fixture
def sink(engine):
    """A provisioned sink in namespace ``team_a`` that reads a frame's own rows."""
    sink = SqlAlchemyOutcomeSink(
        engine, dataset="bars", namespace="team_a", retention_days=30, arrow=False
    )
    provision(engine, sink.ddl_statements())
    return sink


@pytest.fixture
def postgres():
    """A PostgreSQL dialect with neither a driver nor a server: for compiling."""
    return Engine(
        pool=None, dialect=postgresql.dialect(), url=make_url("postgresql://")
    )


def url_for(tmp_path):
    return f"sqlite:///{tmp_path / 'evidence.db'}"


# ----------------------------------------------------------- configuration


def test_it_builds_from_the_environment_and_fails_closed_without_it(
    tmp_path, monkeypatch
):
    config = {"type": "postgres", "dataset": "bars", "dsn_env": "EVIDENCE_URL"}
    monkeypatch.delenv("EVIDENCE_URL", raising=False)
    monkeypatch.delenv(DEFAULT_DSN_VARIABLE, raising=False)
    with pytest.raises(ConfigurationError, match="'EVIDENCE_URL'.*database URL"):
        sink_from_config(config)
    with pytest.raises(ConfigurationError, match=DEFAULT_DSN_VARIABLE):
        sink_from_config(CONFIG)
    monkeypatch.setenv("EVIDENCE_URL", url_for(tmp_path))
    sink = sink_from_config(config)
    assert type(sink) is SqlAlchemyOutcomeSink and sink.dataset == "bars"
    # A working engine against that database: the tables are not there yet.
    with pytest.raises(RepositoryError, match="does not exist"):
        sink.verify_schema()


def test_config_requires_dataset(tmp_path, monkeypatch):
    monkeypatch.setenv(DEFAULT_DSN_VARIABLE, url_for(tmp_path))
    with pytest.raises(ConfigurationError, match="dataset name is required"):
        sink_from_config({"type": "postgres"})


def test_a_libpq_dsn_reaches_psycopg_unchanged_when_connecting(monkeypatch):
    """The connector is built when the sink is and psycopg imported only when
    a connection is made, as the DB-API sink did."""
    handed = []

    class FakePsycopg:
        @staticmethod
        def connect(dsn):
            handed.append(dsn)
            return "a-connection"

    monkeypatch.setitem(sys.modules, "psycopg", FakePsycopg)
    connect = sink_module._psycopg_connector("host=localhost dbname=evidence")
    assert handed == [] and connect() == "a-connection"
    assert handed == ["host=localhost dbname=evidence"]


def test_a_bare_postgres_url_means_psycopg_3_as_it_always_did():
    url, creator = _resolve("postgresql://user@host/evidence")
    assert url.drivername == BARE_POSTGRES_DRIVER == "postgresql+psycopg"
    assert creator is None
    # A driver the URL names is kept; so is any other database.
    assert _resolve("postgresql+psycopg2://user@host/evidence")[0].drivername == (
        "postgresql+psycopg2"
    )
    assert _resolve("sqlite:///evidence.db")[0].get_backend_name() == "sqlite"


def test_an_engine_built_from_a_url_connects(tmp_path):
    """The way every URL-built engine is used: `creator` must not be None."""
    engine = engine_for(url_for(tmp_path))
    try:
        with engine.connect() as connection:
            assert connection.exec_driver_sql("SELECT 1").scalar_one() == 1
    finally:
        engine.dispose()


def test_a_libpq_dsn_is_handed_to_psycopg_as_it_is(monkeypatch):
    handed = []
    monkeypatch.setattr(
        sink_module, "_psycopg_connector", lambda dsn: handed.append(dsn) or "connector"
    )
    url, creator = _resolve("host=localhost dbname=evidence user=dq")
    assert url.drivername == BARE_POSTGRES_DRIVER
    assert handed == ["host=localhost dbname=evidence user=dq"]
    assert creator == "connector"


def test_a_missing_driver_or_dialect_is_reported_before_any_work(monkeypatch):
    monkeypatch.setitem(sys.modules, "psycopg", None)
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    with pytest.raises(
        ConfigurationError, match="driver 'psycopg'.*psycopg\\[binary\\]"
    ):
        engine_for("postgresql://user@host/evidence")
    with pytest.raises(ConfigurationError, match="driver 'psycopg2'"):
        engine_for("postgresql+psycopg2://user@host/evidence")
    with pytest.raises(ConfigurationError, match="dialect 'nosuchdb'"):
        engine_for("nosuchdb://host/evidence")


def test_a_db_api_connect_callable_is_refused_with_guidance():
    with pytest.raises(ConfigurationError, match="Engine or Connection.*create_engine"):
        SqlAlchemyOutcomeSink(lambda: None, dataset="bars")


def test_the_old_name_is_the_same_sink():
    from dq.postgres_sink import DEFAULT_DSN_VARIABLE as old, PostgresOutcomeSink

    assert PostgresOutcomeSink is SqlAlchemyOutcomeSink
    assert old == DEFAULT_DSN_VARIABLE


def test_retention_days_must_be_positive(engine):
    with pytest.raises(ConfigurationError, match="positive integer"):
        SqlAlchemyOutcomeSink(engine, dataset="bars", retention_days=0)


def test_without_sqlalchemy_the_configuration_names_the_extra(tmp_path, monkeypatch):
    monkeypatch.setenv(DEFAULT_DSN_VARIABLE, url_for(tmp_path))
    result = _run_with_blocked_imports(
        "sqlalchemy",
        statement="""
from dq.sinks import sink_from_config
try:
    sink_from_config({"type": "postgres", "dataset": "bars"})
except ImportError as error:
    assert "sqlalchemy>=2.0,<2.2" in str(error), error
    print("named the extra")
else:
    raise AssertionError("a configured SQL sink without SQLAlchemy must fail")
""",
    )
    assert result.returncode == 0, result.stderr + result.stdout


def test_the_framework_imports_no_database_library_even_when_one_is_installed():
    """G3: nothing imports SQLAlchemy, a PostgreSQL driver or pyarrow until a
    SQL sink is configured."""
    script = (
        "import sys\n"
        "import dq.sinks, dq.dq_framework, dq.cli, dq.validate, dq.engine.dq_engine\n"
        "leaked = sorted(name for name in sys.modules if name.split('.')[0] in "
        "{'sqlalchemy', 'psycopg', 'psycopg2', 'pyarrow'})\n"
        "assert leaked == [], leaked\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


# -------------------------------------------------------------- identifiers


@pytest.mark.parametrize(
    ("setting", "value", "label"),
    [
        ("schema", "dq; drop table runs", "database schema"),
        ("schema", "public.dq", "database schema"),
        ("runs_table", "a.b", "runs table"),
        ("artifacts_table", "art facts", "artifacts table"),
        ("namespace", "team a", "tenant namespace"),
    ],
    ids=["schema-injection", "two-part-schema", "two-part-table", "space", "namespace"],
)
def test_identifiers_are_refused_before_any_sql_is_built(engine, setting, value, label):
    with pytest.raises(IdentifierError, match=label):
        SqlAlchemyOutcomeSink(engine, dataset="bars", **{setting: value})


def test_identifiers_are_quoted_only_where_the_dialect_requires_it(postgres):
    plain = SqlAlchemyOutcomeSink(postgres, dataset="bars").ddl_statements()
    assert plain[0].startswith("CREATE TABLE IF NOT EXISTS dq.runs (")
    cased = SqlAlchemyOutcomeSink(
        postgres, dataset="bars", schema="DQ", runs_table="Runs"
    ).ddl_statements()
    assert cased[0].startswith('CREATE TABLE IF NOT EXISTS "DQ"."Runs" (')
    assert cased[2].startswith(
        'CREATE OR REPLACE VIEW "DQ".runs_completed AS SELECT * FROM "DQ"."Runs"'
    )
    reserved = SqlAlchemyOutcomeSink(postgres, dataset="bars", runs_table="order")
    assert reserved.ddl_statements()[0].startswith(
        'CREATE TABLE IF NOT EXISTS dq."order" ('
    )
    assert str(reserved._insert_run.compile(dialect=postgres.dialect)).startswith(
        'INSERT INTO dq."order" ('
    )


def test_ddl_holds_the_certified_postgresql_shape(postgres):
    runs, artifacts, completed, latest = SqlAlchemyOutcomeSink(
        postgres, dataset="bars"
    ).ddl_statements()
    for fragment in (
        "namespace TEXT DEFAULT 'default' NOT NULL",
        "run_key BIGINT NOT NULL",
        "started_millis BIGINT NOT NULL",
        "identity JSONB DEFAULT '{}' NOT NULL",
        "PRIMARY KEY (namespace, run_key)",
    ):
        assert fragment in runs, fragment
    for fragment in (
        "artifact_type TEXT NOT NULL",
        "seq INTEGER NOT NULL",
        "payload JSONB NOT NULL",
        "PRIMARY KEY (namespace, run_key, artifact_type, seq)",
    ):
        assert fragment in artifacts, fragment
    assert completed == (
        "CREATE OR REPLACE VIEW dq.runs_completed AS SELECT * FROM dq.runs "
        "WHERE status = 'completed'"
    )
    assert latest.startswith("CREATE OR REPLACE VIEW dq.latest_per_dataset AS SELECT")
    for fragment in (
        "ROW_NUMBER() OVER (PARTITION BY r.namespace, r.dataset "
        "ORDER BY r.started_millis DESC, r.run_key DESC) AS rn",
        "WHERE r.status = 'completed'",
        "ranked WHERE ranked.rn = 1",
    ):
        assert fragment in latest, fragment


def test_provisioning_is_idempotent(engine):
    sink = SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=False)
    provision(engine, sink.ddl_statements())
    provision(engine, sink.ddl_statements())
    assert sink.verify_schema() == {"runs": 0, "artifacts": 0}


def test_the_schema_check_names_what_is_missing_and_wraps_failures(engine, monkeypatch):
    sink = SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=False)
    with pytest.raises(
        RepositoryError, match=r"evidence table dq\.runs does not exist"
    ):
        sink.verify_schema()

    def explode(connection):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(sink_module, "inspect", explode)
    with pytest.raises(
        RepositoryError, match="evidence schema check failed for 'dq': connection reset"
    ):
        sink.verify_schema()


# ------------------------------------------------------------------ writing


def test_a_save_outside_a_scope_needs_a_key(sink):
    with pytest.raises(RepositoryError, match="explicit run key"):
        sink.save(ARTIFACT, "verifications")


def test_a_save_stamps_every_column_and_holds_the_row_as_json(sink, engine):
    result = sink.save(ARTIFACT, "verifications", 42)
    assert (result.metric_type, result.key, result.targets) == (
        "verifications",
        42,
        ("sqlite:dq.artifacts",),
    )
    assert stored_artifacts(engine) == [
        {
            "namespace": "team_a",
            "run_key": 42,
            "artifact_type": "verifications",
            "seq": 0,
            "dataset": "bars",
            "payload": '{"check": "a", "success": true}',
        },
        {
            "namespace": "team_a",
            "run_key": 42,
            "artifact_type": "verifications",
            "seq": 1,
            "dataset": "bars",
            "payload": '{"check": "b", "success": false}',
        },
    ]


def test_one_artifact_is_one_executemany_of_what_the_db_api_sink_bound(
    sink, engine, sent
):
    sink.save(ARTIFACT, "verifications", 42)
    (insert,) = statements_starting_with(sent, "INSERT")
    statement, _, executemany = insert
    assert executemany is True
    assert statement.split("VALUES")[1].startswith(
        " (?, ?, ?, ?, ?, ?) ON CONFLICT (namespace, run_key, artifact_type, seq) DO NOTHING"
    )
    # (namespace, run_key, artifact_type, seq, dataset, payload), in the order
    # and with the spelling the DB-API sink bound them: its decision, unchanged.
    assert bound(insert) == [
        ("team_a", 42, "verifications", 0, "bars", '{"check": "a", "success": true}'),
        ("team_a", 42, "verifications", 1, "bars", '{"check": "b", "success": false}'),
    ]


def test_for_postgresql_the_payload_is_cast_in_the_statement(postgres):
    sink = SqlAlchemyOutcomeSink(postgres, dataset="bars")
    compiled = str(sink._insert_artifacts.compile(dialect=postgres.dialect))
    assert (
        "VALUES (%(namespace)s, %(run_key)s, %(artifact_type)s, %(seq)s, %(dataset)s, %(payload)s::JSONB)"
        in compiled
    )
    assert compiled.endswith(
        "ON CONFLICT (namespace, run_key, artifact_type, seq) DO NOTHING"
    )


def test_a_run_is_one_transaction_and_the_scope_s_key_is_authoritative(sink, engine):
    with sink.run_scope(1000, {"dataset_sha256": "a" * 8}):
        result = sink.save(ARTIFACT, "verifications", key=5)
        sink.save(FakeDataFrame([]), "metrics", key=5)
    assert result.key == 1000
    assert stored_runs(engine) == [
        {
            "namespace": "team_a",
            "run_key": 1000,
            "dataset": "bars",
            "started_millis": 1000,
            "status": "completed",
            "identity": '{"dataset_sha256": "aaaaaaaa"}',
        }
    ]
    assert [
        (row["run_key"], row["artifact_type"], row["seq"])
        for row in stored_artifacts(engine)
    ] == [
        (1000, "verifications", 0),
        (1000, "verifications", 1),
    ]


def test_an_empty_artifact_writes_nothing(sink, engine, sent):
    assert sink.save(FakeDataFrame([]), "metrics", 1).key == 1
    assert stored_artifacts(engine) == []
    assert statements_starting_with(sent, "INSERT") == []


def test_a_failed_run_leaves_nothing_behind(sink, engine):
    with pytest.raises(RuntimeError, match="engine failed"):
        with sink.run_scope(2000):
            sink.save(ARTIFACT, "verifications")
            raise RuntimeError("engine failed")
    assert stored_runs(engine) == [] and stored_artifacts(engine) == []


def test_repeating_a_run_adds_nothing(sink, engine):
    with sink.run_scope(1000, {"first": True}):
        sink.save(ARTIFACT, "verifications")
    with sink.run_scope(1000, {"first": False}):
        sink.save(FakeDataFrame([{"check": "late"}]), "verifications")
    assert len(stored_artifacts(engine)) == 2
    assert [row["identity"] for row in stored_runs(engine)] == ['{"first": true}']


def test_a_caller_s_connection_is_used_and_never_closed(engine):
    with engine.connect() as connection:
        sink = SqlAlchemyOutcomeSink(connection, dataset="bars", arrow=False)
        provision(connection.engine, sink.ddl_statements())
        sink.save(ARTIFACT, "metrics", 7)
        assert connection.closed is False
        assert connection.in_transaction() is False
    assert [row["run_key"] for row in stored_artifacts(engine)] == [7, 7]


def test_a_scope_inside_the_caller_s_transaction_is_a_savepoint(engine):
    with engine.connect() as connection:
        sink = SqlAlchemyOutcomeSink(connection, dataset="bars", arrow=False)
        provision(connection.engine, sink.ddl_statements())
        with connection.begin():
            connection.execute(
                text(
                    "INSERT INTO dq.artifacts (namespace, run_key, artifact_type, seq, "
                    "dataset, payload) VALUES ('caller', 1, 'metrics', 0, 'bars', '{}')"
                )
            )
            with sink.run_scope(1000):
                sink.save(ARTIFACT, "verifications")
            with pytest.raises(ValueError, match="engine failed"):
                with sink.run_scope(2000):
                    sink.save(FakeDataFrame([{"check": "lost"}]), "verifications")
                    raise ValueError("engine failed")
    assert sorted(
        (row["namespace"], row["run_key"]) for row in stored_artifacts(engine)
    ) == [("caller", 1), ("default", 1000), ("default", 1000)]
    assert [row["run_key"] for row in stored_runs(engine)] == [1000]


def test_the_caller_s_rollback_undoes_a_scope_that_committed(engine):
    with engine.connect() as connection:
        sink = SqlAlchemyOutcomeSink(connection, dataset="bars", arrow=False)
        provision(connection.engine, sink.ddl_statements())
        transaction = connection.begin()
        connection.execute(text("SELECT count(*) FROM dq.runs"))  # read only, so far
        with sink.run_scope(1000):
            sink.save(ARTIFACT, "verifications")
        transaction.rollback()
    assert stored_runs(engine) == [] and stored_artifacts(engine) == []


# -------------------------------------------------------------------- prune


def test_prune_requires_explicit_retention_configuration(engine):
    sink = SqlAlchemyOutcomeSink(engine, dataset="bars", arrow=False)
    with pytest.raises(RepositoryError, match="retention_days"):
        sink.prune(before_millis=1)


def test_prune_deletes_a_namespace_s_old_artifacts_then_its_runs_and_nothing_else(
    sink, engine, sent
):
    write_run(engine, "team_a", FAR_AGO, ("old-a", "old-b"))
    write_run(engine, "team_a", RECENTLY, ("new-a",))
    write_run(engine, "team_b", FAR_AGO, ("other-old",))
    assert sink.prune(before_millis=1000) == (2, 1)
    remaining = [("team_a", RECENTLY), ("team_b", FAR_AGO)]
    assert (
        sorted((row["namespace"], row["run_key"]) for row in stored_runs(engine))
        == remaining
    )
    assert (
        sorted((row["namespace"], row["run_key"]) for row in stored_artifacts(engine))
        == remaining
    )
    deletes = statements_starting_with(sent, "DELETE")
    # Artifacts first, each a single statement with its values bound.
    assert [entry[0].split(" FROM ")[1].split(" ")[0] for entry in deletes] == [
        "dq.artifacts",
        "dq.runs",
    ]
    assert all(entry[2] is False for entry in deletes)
    assert all("team_a" not in entry[0] for entry in deletes)
    assert bound(deletes[0]) == [("team_a", 1000)] and bound(deletes[1]) == [
        ("team_a", 1000)
    ]


def test_prune_for_one_named_namespace(sink, engine):
    write_run(engine, "team_a", FAR_AGO)
    write_run(engine, "team_b", FAR_AGO)
    assert sink.prune(before_millis=1000, namespace="team_b") == (1, 1)
    assert [row["namespace"] for row in stored_runs(engine)] == ["team_a"]


def test_prune_across_namespaces_needs_only_the_cutoff(sink, engine, sent):
    write_run(engine, "team_a", FAR_AGO)
    write_run(engine, "team_b", FAR_AGO)
    write_run(engine, "team_a", RECENTLY)
    assert sink.prune(before_millis=1000, all_namespaces=True) == (2, 2)
    assert [row["run_key"] for row in stored_runs(engine)] == [RECENTLY]
    assert all(
        bound(entry) == [(1000,)] for entry in statements_starting_with(sent, "DELETE")
    )


def test_the_cutoff_follows_from_retention_days(engine, sent):
    sink = SqlAlchemyOutcomeSink(
        engine, dataset="bars", namespace="team_a", retention_days=7, arrow=False
    )
    provision(engine, sink.ddl_statements())
    before = time.time_ns() // 1_000_000
    sink.prune(namespace="team_a")
    ((namespace, cutoff),) = bound(statements_starting_with(sent, "DELETE")[-1])
    assert namespace == "team_a"
    assert before - 7 * DAY <= cutoff <= before


# ------------------------------------------------------------------ payload


def test_a_payload_is_strict_json_however_the_frame_spells_its_values(sink, engine):
    frame = FakeDataFrame(
        [
            {
                "check": "ratio",
                "value": float("nan"),
                "bound": float("inf"),
                "amount": Decimal("1.50"),
                "at": datetime(2026, 11, 25, 10),
                "ok": True,
            }
        ]
    )
    sink.save(frame, "metrics", 1)
    (stored,) = stored_artifacts(engine)
    assert json.loads(stored["payload"], parse_constant=reject) == {
        "check": "ratio",
        "value": None,
        "bound": None,
        "amount": "1.50",
        "at": "2026-11-25T10:00:00",
        "ok": True,
    }


def test_the_namespace_column_defaults_to_what_the_sink_stamps(sink, engine):
    """A row written without a namespace still carries one (SPEC-006)."""
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO dq.artifacts (run_key, artifact_type, seq, dataset, payload) "
            "VALUES (1, 'metrics', 0, 'bars', '{}')"
        )
    assert [row["namespace"] for row in stored_artifacts(engine)] == ["default"]


# -------------------------------------------------------------- all or nothing


def test_a_save_that_fails_halfway_stores_nothing(sink, engine):
    """The first row fits and the second does not. Neither may remain."""
    with engine.begin() as connection:
        connection.exec_driver_sql(
            # A SQLite trigger lives in its table's database, hence the qualifier.
            "CREATE TRIGGER dq.reject_the_second BEFORE INSERT ON dq.artifacts "
            "WHEN NEW.seq >= 1 BEGIN SELECT RAISE(ABORT, 'row rejected'); END"
        )
    with pytest.raises(IntegrityError, match="row rejected"):
        sink.save(ARTIFACT, "verifications", 42)
    assert stored_artifacts(engine) == []


def test_a_prune_that_fails_halfway_deletes_nothing(engine):
    """Artifacts go before runs. When the runs cannot be deleted, the
    artifacts must still be there: one transaction, all or nothing."""
    sink = SqlAlchemyOutcomeSink(
        engine,
        dataset="bars",
        namespace="team_a",
        retention_days=30,
        arrow=False,
    )
    provision(engine, sink.ddl_statements())
    write_run(engine, "team_a", FAR_AGO, ("old",))
    assert len(stored_runs(engine)) == len(stored_artifacts(engine)) == 1

    def fail_on_runs(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("DELETE FROM dq.runs"):
            raise RuntimeError("connection reset")

    event.listen(engine, "before_cursor_execute", fail_on_runs)
    try:
        with pytest.raises(RuntimeError, match="connection reset"):
            sink.prune(before_millis=1000)
    finally:
        event.remove(engine, "before_cursor_execute", fail_on_runs)
    assert len(stored_runs(engine)) == len(stored_artifacts(engine)) == 1
