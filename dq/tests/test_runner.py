# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Spark-free runner (SPEC-015): a rule pack over files, with no Spark.

Two things are checked above all. That a run reads what it is told, decides
through the one run core, and writes a ``dq-report/v1`` document. And that
everything a run over files cannot do is refused *before a dataset is read*:
each refusal test replaces the reader with one that records, then asserts it
was never called.
"""

import hashlib
import json
import logging
import sys
from decimal import Decimal
from pathlib import Path

import pytest

from dq import runner
from dq.exceptions import ConfigurationError
from dq.notifier import InMemoryNotifier
from dq.observability import InMemoryObserver
from dq.tests.runner_support import (
    COMPLETE,
    OTHER_SHA,
    SHA,
    SIZE,
    orders_table,
    rule,
    rule_pack,
    write_arrow,
    write_orders,
    write_pack,
    write_parquet,
)

pytest.importorskip("pyarrow", reason="the runner needs the arrow extra")

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "arrow_rule.conf"
REPORT_KEYS = {
    "schema_version",
    "generated_at",
    "status",
    "rule_set",
    "dataset",
    "runtime",
    "summary",
    "checks",
}


@pytest.fixture
def unread(monkeypatch):
    """Make the runner's reader record and answer: a refusal must not read."""
    reads = []
    monkeypatch.setattr(
        runner, "read_dataset", lambda name, path: reads.append(name) or orders_table()
    )
    return reads


def refused(pack, datasets, unread, *match):
    """The pack is refused before a dataset is read, saying ``match``."""
    with pytest.raises(ConfigurationError) as raised:
        runner.run(str(pack), datasets)
    for text in match:
        assert text in str(raised.value), (text, str(raised.value))
    assert unread == []


def passing(tmp_path, *rules, **settings):
    """A rule pack over a written Parquet file: the pack's path and the file's."""
    orders = write_orders(tmp_path)
    return write_pack(tmp_path, rule_pack(*rules, **settings)), {"orders": str(orders)}


def two_datasets(tmp_path):
    """Two written files and the rules that run on them, one each."""
    orders = write_orders(tmp_path)
    returns = write_orders(tmp_path, "returns.parquet")
    rules = (
        rule("orders_rule"),
        rule("returns_rule", dataframes=("returns",), dataset="returns", sha=OTHER_SHA),
    )
    return {"orders": str(orders), "returns": str(returns)}, rules


# ------------------------------------------------------------------ the run


def test_a_run_reads_a_parquet_file_and_writes_a_valid_report(tmp_path):
    pack, datasets = passing(tmp_path, rule(dataset="orders_snapshot"))
    report = tmp_path / "evidence" / "report.json"
    result = runner.run(str(pack), datasets, report=str(report))
    assert [(o["check"], o["success"]) for o in result.outcomes] == [
        ("order_id_complete", False),
        ("orders_present", True),
    ]
    assert result.decision.passed is False
    assert result.decision.blocking == ("order_id_complete",)

    document = result.report
    assert set(document) == REPORT_KEYS
    assert document["schema_version"] == "dq-report/v1"
    assert document["runtime"]["spark"] is None
    assert document["runtime"]["application_id"] == "dq-run"
    assert document["dataset"] == {"id": "orders_snapshot", "sha256": SHA}
    assert document["rule_set"] == {
        "reference": str(pack),
        "sha256": hashlib.sha256(pack.read_bytes()).hexdigest(),
    }
    assert document["checks"] == result.outcomes
    assert {o["jobid"] for o in document["checks"]} == {"dq-run"}
    assert {o["ts"] for o in document["checks"]} == {result.run_key}
    summary = document["summary"]
    assert (summary["total"], summary["passed"], summary["failed"]) == (2, 1, 1)
    assert summary["blocked"] + summary["warned"] == summary["failed"]
    assert document["status"] == "failed"
    assert json.loads(report.read_text()) == document
    assert result.event.report == str(report)


def test_the_gate_policy_of_the_pack_decides(tmp_path):
    # Only a Warning-level check fails: under "error" it does not block.
    failing_warning = (
        '{ constraint = "hasSize", constraint_name = "no_orders", '
        'assertion = "lambda size: size > 1000", level = "Warning" }'
    )
    pack, datasets = passing(
        tmp_path, rule(plan=failing_warning), **{"gate.fail_on": '"error"'}
    )
    assert runner.run(str(pack), datasets).decision.passed is True
    pack, datasets = passing(tmp_path, rule(plan=failing_warning))
    assert runner.run(str(pack), datasets).decision.passed is False


def test_the_run_is_observed_and_announced_as_any_run_is(tmp_path):
    pack, datasets = passing(tmp_path, rule())
    observer, notifier = InMemoryObserver(), InMemoryNotifier()
    result = runner.run(
        str(pack),
        datasets,
        report=str(tmp_path / "evidence.json"),
        observer=observer,
        notifier=notifier,
    )
    assert observer.notifications == (
        "run_started",
        "rule_started",
        "rule_finished",
        "run_finished",
        "gate_event",
    )
    (event,) = notifier.events
    assert (event.job_id, event.dataset, event.report) == (
        "dq-run",
        "orders",
        str(tmp_path / "evidence.json"),
    )
    assert event.blocked == ("order_id_complete",)


def test_rule_pack_text_or_a_remote_pack_is_refused(tmp_path, unread):
    with pytest.raises(ConfigurationError, match="is not a local file"):
        runner.run(rule_pack(rule()), {"orders": "orders.parquet"})
    with pytest.raises(ConfigurationError, match="require a local configuration file"):
        runner.run("s3://bucket/rules.conf", {"orders": "orders.parquet"})
    assert unread == []


def test_the_rule_pack_is_parsed_and_validated_like_any_other(tmp_path):
    pack = write_pack(tmp_path, "dqframework { dqrules = [] }")
    with pytest.raises(ConfigurationError, match="No dqrules defined"):
        runner.run(str(pack), {"orders": "orders.parquet"})


# ---------------------------------------------- refused before anything is read


def test_a_rule_that_needs_spark_is_refused(tmp_path, unread):
    pack = write_pack(tmp_path, rule_pack(rule("rows", engine="deequ", adapter=None)))
    refused(
        pack,
        {"orders": str(write_orders(tmp_path))},
        unread,
        "rows uses engine 'deequ'",
        "executes portable rules only",
    )


@pytest.mark.parametrize(
    "adapter, how",
    [
        (None, "it names none, which means spark"),
        ('"spark"', "it names 'spark'"),
        ('"Arrow"', "it names 'Arrow'"),
        ('"sql"', "it names 'sql'"),
    ],
)
def test_a_portable_rule_that_does_not_name_the_arrow_adapter_is_refused(
    tmp_path, unread, adapter, how
):
    pack = write_pack(tmp_path, rule_pack(rule("counts_rule", adapter=adapter)))
    refused(
        pack,
        {"orders": str(write_orders(tmp_path))},
        unread,
        'counts_rule does not name adapter = "arrow"',
        how,
        "does not substitute an adapter",
    )


def test_a_configured_sink_is_refused_rather_than_left_unwritten(tmp_path, unread):
    orders = str(write_orders(tmp_path))
    pack = write_pack(
        tmp_path, rule_pack(rule(), **{"repository": '{ type = "memory" }'})
    )
    refused(
        pack,
        {"orders": orders},
        unread,
        "dqframework.repository configures a sink",
        "its evidence is the report",
    )


def test_a_dataset_a_rule_needs_and_lacks_is_refused(tmp_path, unread):
    orders = str(write_orders(tmp_path))
    pack = write_pack(tmp_path, rule_pack(rule(dataframes=("orders", "returns"))))
    refused(pack, {"orders": orders}, unread, "no dataset is given for 'returns'")


def test_a_dataset_no_rule_uses_is_refused(tmp_path, unread):
    orders = str(write_orders(tmp_path))
    pack = write_pack(tmp_path, rule_pack(rule()))
    refused(
        pack,
        {"orders": orders, "returns": orders},
        unread,
        "a dataset is given for 'returns', which no rule uses",
    )


def test_a_dataset_that_is_not_there_is_refused(tmp_path, unread):
    pack = write_pack(tmp_path, rule_pack(rule()))
    refused(
        pack,
        {"orders": tmp_path / "nowhere.parquet"},
        unread,
        "dataset 'orders': no such file or directory",
    )


def test_a_file_no_reader_knows_is_refused(tmp_path, unread):
    orders = str(write_orders(tmp_path))
    mystery = tmp_path / "orders.dat"
    mystery.write_bytes(b"not a format")
    pack = write_pack(tmp_path, rule_pack(rule()))
    refused(
        pack,
        {"orders": orders, "returns": str(mystery)},
        unread,
        "dataset 'returns': no reader knows a '.dat' file",
        "'.parquet'",
    )


def test_a_dataset_that_is_not_local_is_refused(tmp_path, unread):
    orders = str(write_orders(tmp_path))
    pack = write_pack(tmp_path, rule_pack(rule()))
    refused(
        pack,
        {"orders": "s3://bucket/orders.parquet", "returns": orders},
        unread,
        "dataset 'orders': Evidence reports require a local dataset file",
    )


def test_no_datasets_at_all_is_refused(tmp_path, unread):
    pack = write_pack(tmp_path, rule_pack(rule()))
    refused(pack, {}, unread, "a run needs its datasets")
    refused(pack, ["orders.parquet"], unread, "a run needs its datasets")


def test_dataset_names_that_are_not_strings_are_refused_in_order(tmp_path, unread):
    """A caller's keys need not be strings. Sorting them with the rules'
    names once raised a TypeError where a refusal belongs."""
    orders = str(write_orders(tmp_path))
    pack = write_pack(tmp_path, rule_pack(rule()))
    with pytest.raises(ConfigurationError) as raised:
        runner.run(str(pack), {1: orders, 2: orders, "extra": orders})
    lines = str(raised.value).splitlines()
    assert lines[1:] == [
        "  - no dataset is given for 'orders', which a rule needs",
        "  - a dataset is given for 1, which no rule uses",
        "  - a dataset is given for 2, which no rule uses",
        "  - a dataset is given for 'extra', which no rule uses",
    ]
    assert unread == []


def test_every_problem_is_reported_at_once(tmp_path, unread):
    orders = str(write_orders(tmp_path))
    pack = write_pack(
        tmp_path,
        rule_pack(
            rule("first", engine="deequ", adapter=None),
            rule("second", dataframes=("orders", "returns")),
            rule("third", adapter='"spark"'),
            **{"repository": '{ type = "memory" }'},
        ),
    )
    with pytest.raises(ConfigurationError) as raised:
        runner.run(str(pack), {"orders": orders})
    lines = str(raised.value).splitlines()
    assert lines[0] == "a run over files cannot run this rule pack:"
    assert [line[4:].split(",")[0].split(":")[0] for line in lines[1:]] == [
        "dqframework.repository configures a sink",
        "first uses engine 'deequ'",
        'third does not name adapter = "arrow"',
        "no dataset is given for 'returns'",
    ]
    assert unread == []


# --------------------------------------------------- what the evidence is about


def test_the_evidence_is_about_the_one_dataset_the_rules_run_on(tmp_path):
    pack, datasets = passing(tmp_path, rule())
    assert runner.run(str(pack), datasets).report["dataset"] == {
        "id": "orders",
        "sha256": SHA,
    }


def test_the_evidence_is_about_the_dataset_the_pack_names(tmp_path):
    pack, datasets = passing(tmp_path, rule(), dataset="orders_snapshot")
    assert runner.run(str(pack), datasets).report["dataset"] == {
        "id": "orders_snapshot",
        "sha256": SHA,
    }


def test_a_run_over_several_datasets_must_say_which_one_the_evidence_is_about(
    tmp_path, unread
):
    paths, rules = two_datasets(tmp_path)
    refused(
        write_pack(tmp_path, rule_pack(*rules)),
        paths,
        unread,
        "a run over several datasets ('orders', 'returns')",
        "set dqframework.dataset",
    )
    named = write_pack(tmp_path, rule_pack(*rules, dataset="returns"), "named.conf")
    result = runner.run(str(named), paths)
    assert result.report["dataset"] == {"id": "returns", "sha256": OTHER_SHA}
    assert sorted(unread) == ["orders", "returns"]


def test_a_named_dataset_no_rule_runs_on_is_refused(tmp_path, unread):
    paths, rules = two_datasets(tmp_path)
    pack = write_pack(tmp_path, rule_pack(*rules, dataset="invoices"))
    refused(
        pack,
        paths,
        unread,
        "dqframework.dataset 'invoices' names none of the datasets",
        "['orders', 'returns']",
    )


def test_rules_that_certify_two_snapshots_of_one_dataset_are_refused(tmp_path, unread):
    pack, datasets = passing(tmp_path, rule("first"), rule("second", sha=OTHER_SHA))
    refused(
        pack,
        datasets,
        unread,
        "the rules on dataset 'orders' certify two snapshots",
        SHA,
        OTHER_SHA,
    )


# ------------------------------------------------------------- the formats


@pytest.mark.parametrize("extension", [".parquet", ".pq", ".arrow", ".feather", ".ipc"])
def test_arrow_formats_keep_every_type(tmp_path, extension):
    source = orders_table()
    path = tmp_path / f"orders{extension}"
    if extension in (".parquet", ".pq"):
        write_parquet(source, path)
    else:
        write_arrow(source, path)
    table = runner.read_dataset("orders", path)
    assert table.schema == source.schema
    assert table.to_pylist() == source.to_pylist()


def test_a_csv_is_read_as_pyarrow_reads_one(tmp_path):
    """No type survives a CSV: pyarrow decides each column's type, a decimal
    becomes a double, and a null is an empty field."""
    import pyarrow as pa
    import pyarrow.csv

    source = pa.table(
        {
            "id": ["a", None],
            "amount": pa.array([Decimal("1.25"), None], pa.decimal128(10, 2)),
        }
    )
    path = tmp_path / "orders.csv"
    with path.open("wb") as handle:
        pyarrow.csv.write_csv(source, handle)
    table = runner.read_dataset("orders", path)
    assert table.column_names == ["id", "amount"]
    assert str(table.schema.field("amount").type) == "double"
    assert table.column("amount").to_pylist() == [1.25, None]
    assert table.column("id").to_pylist() == ["a", ""]


def test_newline_delimited_json_is_read(tmp_path):
    path = tmp_path / "orders.json"
    path.write_text('{"id": "a", "amount": 1.25}\n{"id": null, "amount": null}\n')
    table = runner.read_dataset("orders", path)
    assert table.to_pylist() == [
        {"id": "a", "amount": 1.25},
        {"id": None, "amount": None},
    ]


def test_a_directory_is_read_as_the_dataset_it_holds(tmp_path):
    source = orders_table()
    parts = tmp_path / "orders"
    write_parquet(source.slice(0, 2), parts / "part-0.parquet")
    write_parquet(source.slice(2), parts / "part-1.parquet")
    assert runner.read_dataset("orders", parts).to_pylist() == source.to_pylist()


def test_a_file_that_cannot_be_read_is_refused_naming_the_dataset(tmp_path):
    broken = tmp_path / "orders.parquet"
    broken.write_bytes(b"this is not a parquet file")
    with pytest.raises(
        ConfigurationError, match="dataset 'orders' could not be read from"
    ):
        runner.read_dataset("orders", broken)


def test_a_directory_with_nothing_recognisable_in_it_is_refused(tmp_path):
    """pyarrow reads a directory with no file at all as no column, and says
    nothing; every rule would then fail on its first column."""
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(
        ConfigurationError, match="nothing in it was recognised as data"
    ):
        runner.read_dataset("orders", empty)
    # One holding only what pyarrow does not read, pyarrow refuses itself.
    (empty / "notes.txt").write_text("not data")
    with pytest.raises(
        ConfigurationError, match="dataset 'orders' could not be read from"
    ):
        runner.read_dataset("orders", empty)


def test_a_file_url_names_a_dataset_too(tmp_path):
    pack, datasets = passing(tmp_path, rule())
    result = runner.run(f"file://{pack}", {"orders": f"file://{datasets['orders']}"})
    assert result.report["rule_set"]["reference"] == f"file://{pack}"


# ------------------------------------------------- without pyarrow


def test_without_pyarrow_the_extra_is_named(tmp_path, monkeypatch):
    pack, datasets = passing(tmp_path, rule())
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with pytest.raises(ImportError) as raised:
        runner.run(str(pack), datasets)
    message = str(raised.value)
    assert "The arrow runner requires the optional pyarrow package" in message
    assert "poetry install -E arrow" in message


# ------------------------------------------------------------ the example


def test_the_shipped_arrow_example_runs_over_a_file(tmp_path, capsys):
    import pyarrow as pa

    orders = write_parquet(
        orders_table().append_column("customer_id", pa.array(["c1", "c1", "c2", "c2"])),
        tmp_path / "orders.parquet",
    )
    report = tmp_path / "report.json"
    code = runner.main(
        [str(EXAMPLE), "--dataset", f"orders={orders}", "--report", str(report)]
    )
    assert code == 1
    document = json.loads(report.read_text())
    assert [(c["check"], c["success"]) for c in document["checks"]] == [
        ("order_id_complete", False),
        ("orders_present", True),
        ("orders_per_customer.id.min", True),
        ("amount_range.high", True),
        ("amount_range.low", True),
    ]
    assert {c["details"]["semantic_version"] for c in document["checks"]} == {
        "counts/v1",
        "groups/v1",
        "ranges/v1",
    }
    out = capsys.readouterr().out
    assert "Total checks: 5  passed: 4  failed: 1" in out
    assert "The gate failed." in out


# -------------------------------------------------------------- the command


def test_the_command_fails_and_passes_by_the_gate(tmp_path, capsys):
    pack, datasets = passing(tmp_path, rule())
    report = tmp_path / "failed.json"
    failed = runner.main(
        [
            str(pack),
            "--dataset",
            f"orders={datasets['orders']}",
            "--report",
            str(report),
        ]
    )
    assert failed == 1
    assert json.loads(report.read_text())["status"] == "failed"

    pack, datasets = passing(tmp_path, rule(plan=SIZE))
    report = tmp_path / "passed.json"
    assert (
        runner.main(
            [
                str(pack),
                "--dataset",
                f"orders={datasets['orders']}",
                "--report",
                str(report),
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "Total checks: 1  passed: 1  failed: 0" in out
    assert "The gate passed." in out
    assert json.loads(report.read_text())["status"] == "passed"


def test_the_summary_counts_the_way_the_gate_counts(tmp_path, capsys):
    """Under the default policy a failed warning blocks too, and the summary
    says so once, in the gate's terms."""
    warning_only = COMPLETE.replace('"Error"', '"Warning"')
    pack, datasets = passing(tmp_path, rule(plan=warning_only))
    assert (
        runner.main(
            [
                str(pack),
                "--dataset",
                f"orders={datasets['orders']}",
                "--report",
                str(tmp_path / "r.json"),
            ]
        )
        == 1
    )
    out = capsys.readouterr().out
    assert "failed: 1" in out
    assert "Under the 'any' gate policy: 1 blocking, 0 advisory" in out
    assert "Blocking failed checks: order_id_complete" in out


def test_a_pack_that_validates_but_cannot_run_exits_two_and_reads_nothing(
    tmp_path, unread, caplog
):
    pack = write_pack(tmp_path, rule_pack(rule(adapter='"spark"')))
    with caplog.at_level(logging.ERROR):
        code = runner.main(
            [
                str(pack),
                "--dataset",
                "orders=orders.parquet",
                "--report",
                str(tmp_path / "r.json"),
            ]
        )
    assert code == 2
    assert 'does not name adapter = "arrow"' in caplog.text
    assert not (tmp_path / "r.json").exists()
    assert unread == []


def test_an_invalid_pack_is_refused_by_the_validator_before_the_runner(
    tmp_path, unread, caplog
):
    pack = write_pack(tmp_path, rule_pack(rule(engine="deequ", adapter=None)))
    with caplog.at_level(logging.ERROR):
        code = runner.main(
            [
                str(pack),
                "--dataset",
                "orders=orders.parquet",
                "--report",
                str(tmp_path / "r.json"),
            ]
        )
    assert code == 2
    assert "Engine 'deequ' requires a non-empty 'checks' payload" in caplog.text
    assert "Configuration validation failed; no dataset was read" in caplog.text
    assert unread == []


@pytest.mark.parametrize(
    "value", ["orders", "=orders.parquet", "orders=", " =orders.parquet"]
)
def test_a_dataset_argument_must_be_name_and_path(tmp_path, value):
    pack = write_pack(tmp_path, rule_pack(rule()))
    with pytest.raises(SystemExit) as raised:
        runner.main(
            [str(pack), "--dataset", value, "--report", str(tmp_path / "r.json")]
        )
    assert raised.value.code == 2


def test_a_missing_dataset_argument_is_an_argument_error(tmp_path):
    pack = write_pack(tmp_path, rule_pack(rule()))
    with pytest.raises(SystemExit) as raised:
        runner.main([str(pack), "--report", str(tmp_path / "r.json")])
    assert raised.value.code == 2


def test_the_report_directory_is_made(tmp_path):
    pack, datasets = passing(tmp_path, rule())
    report = tmp_path / "deep" / "nested" / "report.json"
    runner.run(str(pack), datasets, report=str(report))
    assert report.is_file()


# ------------------------------------------- what the command does with failure


def test_the_command_names_a_missing_dependency(tmp_path, monkeypatch, caplog):
    pack, datasets = passing(tmp_path, rule())
    report = str(tmp_path / "r.json")
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with caplog.at_level(logging.ERROR):
        code = runner.main(
            [str(pack), "--dataset", f"orders={datasets['orders']}", "--report", report]
        )
    assert code == 2
    assert "Missing dependency or engine: The arrow runner requires" in caplog.text
    assert not Path(report).exists()


def test_the_command_shows_a_traceback_when_asked(tmp_path, caplog, capsys):
    pack = write_pack(tmp_path, rule_pack(rule(adapter='"spark"')))
    with caplog.at_level(logging.DEBUG):
        code = runner.main(
            [
                str(pack),
                "--dataset",
                "orders=orders.parquet",
                "--report",
                str(tmp_path / "r.json"),
                "--verbose",
            ]
        )
    assert code == 2
    assert (
        "ConfigurationError: a run over files cannot run this rule pack"
        in capsys.readouterr().err
    )


def test_an_unexpected_error_is_reported_and_never_raised(
    tmp_path, monkeypatch, caplog, capsys
):
    pack = write_pack(tmp_path, rule_pack(rule()))

    def explode(*args, **kwargs):
        raise ValueError("the unexpected")

    monkeypatch.setattr(runner, "run", explode)
    arguments = [
        str(pack),
        "--dataset",
        "orders=orders.parquet",
        "--report",
        str(tmp_path / "r.json"),
    ]
    with caplog.at_level(logging.ERROR):
        assert runner.main(arguments) == 1
    assert "Error running data quality checks: the unexpected" in caplog.text
    assert runner.main([*arguments, "--verbose"]) == 1
    assert "ValueError: the unexpected" in capsys.readouterr().err


def test_the_command_lists_advisory_failures_and_passes_under_the_error_policy(
    tmp_path, capsys
):
    warning = COMPLETE.replace('"Error"', '"Warning"')
    pack, datasets = passing(
        tmp_path, rule(plan=warning), **{"gate.fail_on": '"error"'}
    )
    code = runner.main(
        [
            str(pack),
            "--dataset",
            f"orders={datasets['orders']}",
            "--report",
            str(tmp_path / "r.json"),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "0 blocking, 1 advisory" in out
    assert "Advisory failures (not blocking): order_id_complete" in out
    assert "The gate passed." in out


# ------------------------------------------------- a rule that names no dataset


def nameless(name):
    """A portable rule without ``dataset`` and ``snapshot_sha256``."""
    text = rule(name)
    return (
        text.replace('      dataset = "orders"\n', "")
        .replace(f'      snapshot_sha256 = "{SHA}"\n', "")
        .rstrip()
    )


def test_a_pack_whose_rules_name_no_dataset_is_refused(tmp_path, unread):
    pack = write_pack(tmp_path, rule_pack(nameless("vague")))
    refused(
        pack,
        {"orders": str(write_orders(tmp_path))},
        unread,
        "no rule names its dataset and the snapshot digest it certifies",
    )


def test_a_dataset_setting_that_is_not_a_name_is_refused(tmp_path, unread):
    pack = write_pack(
        tmp_path,
        rule_pack(rule()).replace(
            "  dqrules = [", '  dataset { name = "orders" }\n  dqrules = ['
        ),
    )
    refused(
        pack,
        {"orders": str(write_orders(tmp_path))},
        unread,
        "dqframework.dataset must be the name of a dataset",
    )


def test_a_rule_that_names_no_digest_is_refused_by_the_engine_after_the_files_are_read(
    tmp_path,
):
    """Naming a dataset is the engine's own payload contract: the runner
    leaves it to the engine, which refuses before computing anything."""
    orders = write_orders(tmp_path)
    pack = write_pack(tmp_path, rule_pack(rule("clear"), nameless("vague")))
    report = tmp_path / "r.json"
    with pytest.raises(ConfigurationError, match="portable engine requires 'dataset'"):
        runner.run(str(pack), {"orders": str(orders)}, report=str(report))
    assert not report.exists()
