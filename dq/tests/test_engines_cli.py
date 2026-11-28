# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the `dq-engines` command (SPEC-011 G5).

The command answers "which engines can I use here, and what can each do"
without starting Spark, so it must stay light, report an absent vendor
library as such, and list third-party engines registered by entry point.
"""

import json
import re

import pytest

from dq import engines as cli
from dq.engine.registry import BUILT_IN_ENGINES, ENTRY_POINT_GROUP
from dq.tests.test_package_contract import _run_with_blocked_imports


def run(capsys, arguments):
    code = cli.main(arguments)
    captured = capsys.readouterr()
    return code, captured.out


def test_json_output_is_stable_and_complete(capsys):
    code, out = run(capsys, ["--format", "json"])
    payload = json.loads(out)
    assert code == 0
    assert list(payload) == ["engines", "entry_point_group"]
    assert payload["entry_point_group"] == ENTRY_POINT_GROUP
    listed = {engine["name"]: engine for engine in payload["engines"]}
    assert set(listed) == set(BUILT_IN_ENGINES)
    assert [engine["name"] for engine in payload["engines"]] == sorted(listed)
    deequ = listed["deequ"]
    assert deequ == {
        "name": "deequ",
        "source": "built-in",
        "module": "dq.engine.deequ.deequ_engine",
        "class": "DeequEngine",
        "payload_key": "checks",
        "extras": ["spark", "deequ"],
        "installed": True,
        "missing": [],
        "problem": None,
        "install_hint": None,
        "renamed_from": [],
        "capabilities": {
            "criticality": "native",
            "row_filter": False,
            "failed_rows": False,
            "persistence": True,
        },
    }
    assert listed["businessrules"]["renamed_from"] == ["custom"]


def test_text_output_is_a_table_with_one_row_per_engine(capsys):
    code, out = run(capsys, [])
    lines = out.splitlines()
    assert code == 0
    header = lines[0].split()
    assert header == [
        "Name",
        "Payload",
        "key",
        "Extras",
        "Installed",
        "Criticality",
        "Row",
        "filter",
        "Failed",
        "rows",
        "Persistence",
    ]
    rows = [line for line in lines if line.split(" ")[0] in BUILT_IN_ENGINES]
    assert len(rows) == len(BUILT_IN_ENGINES)
    assert [row.split(" ")[0] for row in rows] == sorted(BUILT_IN_ENGINES)
    # No trailing padding, and exactly one header.
    assert all(line == line.rstrip() for line in lines)
    assert sum(line.startswith("Name") for line in lines) == 1
    assert f"'{ENTRY_POINT_GROUP}'" in out


def test_an_engine_whose_vendor_library_is_missing_is_reported_not_raised(
    capsys, monkeypatch
):
    monkeypatch.setattr(
        cli,
        "missing_vendor_imports",
        lambda spec: ("pyspark", "pydeequ") if spec.name == "deequ" else (),
    )
    code, out = run(capsys, ["--format", "json"])
    deequ = {engine["name"]: engine for engine in json.loads(out)["engines"]}["deequ"]
    assert code == 0
    assert deequ["installed"] is False
    assert deequ["capabilities"] is None
    assert deequ["missing"] == ["pyspark", "pydeequ"]
    assert deequ["problem"] is None
    # Every extra the engine needs, since no extra of the package pulls in
    # another: "deequ" alone installs no Spark.
    assert deequ["install_hint"] == (
        "pip install 'dq-kernel[spark,deequ]'  (or: poetry install -E spark -E deequ)"
    )
    code, out = run(capsys, [])
    (deequ_row,) = [
        re.split(r"\s{2,}", line.strip())
        for line in out.splitlines()
        if line.startswith("deequ")
    ]
    assert deequ_row[1:4] == ["checks", "spark,deequ", "no"]
    assert deequ_row[4:] == ["-", "-", "-", "-"]  # no manifest could be read
    assert "  deequ: pyspark, pydeequ -- pip install 'dq-kernel[spark,deequ]'" in out


def test_an_engine_that_fails_to_import_is_reported_with_the_reason(
    capsys, monkeypatch
):
    """Installed, and refusing to be imported: pydeequ does so wherever
    SPARK_VERSION is not set. An install hint would send the reader to
    install what is already there; the reason is the library's own words."""
    import importlib

    real = importlib.import_module

    def refusing(name, *args, **kwargs):
        if name == "dq.engine.deequ.deequ_engine":
            raise RuntimeError("SPARK_VERSION environment variable is required")
        return real(name, *args, **kwargs)

    monkeypatch.setattr(cli.importlib, "import_module", refusing)
    monkeypatch.setattr(
        cli, "missing_vendor_imports", lambda spec: []  # installed, every one
    )
    code, out = run(capsys, ["--format", "json"])
    deequ = {engine["name"]: engine for engine in json.loads(out)["engines"]}["deequ"]
    assert code == 0
    assert deequ["installed"] is False
    assert deequ["missing"] == []
    assert deequ["install_hint"] is None
    assert deequ["capabilities"] is None
    assert (
        deequ["problem"]
        == "RuntimeError: SPARK_VERSION environment variable is required"
    )
    code, out = run(capsys, [])
    assert (
        "Cannot be used here:\n"
        "  deequ: RuntimeError: SPARK_VERSION environment variable is required"
    ) in out
    # Not sent to install what is already there.
    assert not [line for line in out.splitlines() if line.startswith("  deequ: pip")]


def test_a_library_an_engine_imports_but_does_not_declare_is_named(capsys, monkeypatch):
    """The declaration missed one, or the engine is from elsewhere: the
    module that was not found is said, as the loader says it."""
    import importlib

    real = importlib.import_module

    def undeclared(name, *args, **kwargs):
        if name == "dq.engine.portable.portable_engine":
            raise ModuleNotFoundError(
                "No module named 'some.library'", name="some.library"
            )
        return real(name, *args, **kwargs)

    monkeypatch.setattr(cli.importlib, "import_module", undeclared)
    code, out = run(capsys, ["--format", "json"])
    portable = {engine["name"]: engine for engine in json.loads(out)["engines"]}[
        "portable"
    ]
    assert code == 0
    assert portable["installed"] is False
    assert portable["missing"] == ["some.library"]
    assert portable["problem"] is None
    # The engine's extras, as for a declared one: what else would be named.
    assert portable["install_hint"] == (
        "pip install 'dq-kernel[arrow,sqlalchemy,spark]'  "
        "(or: poetry install -E arrow -E sqlalchemy -E spark)"
    )
    code, out = run(capsys, [])
    assert (
        "  portable: some.library -- pip install 'dq-kernel[arrow,sqlalchemy,spark]'"
        in out
    )


def test_the_command_answers_even_where_judging_an_engine_fails(capsys, monkeypatch):
    """ "Which engines can I use here" is asked first, on the machine where
    nothing runs: whatever goes wrong while answering, it answers."""

    def exploding(spec):
        raise TypeError("a stand-in in sys.modules")

    monkeypatch.setattr(cli, "missing_vendor_imports", exploding)
    code, out = run(capsys, ["--format", "json"])
    listed = {engine["name"]: engine for engine in json.loads(out)["engines"]}
    assert code == 0
    assert set(listed) == set(BUILT_IN_ENGINES)
    assert {engine["problem"] for engine in listed.values()} == {
        "TypeError: a stand-in in sys.modules"
    }
    assert not any(engine["installed"] for engine in listed.values())
    code, out = run(capsys, [])
    assert code == 0 and "Cannot be used here:" in out


def test_an_engine_whose_class_is_not_in_its_module_is_said_so(capsys, monkeypatch):
    import importlib
    from types import SimpleNamespace

    real = importlib.import_module

    def no_class(name, *args, **kwargs):
        if name == "dq.engine.portable.portable_engine":
            return SimpleNamespace()  # the module is here; its engine is not
        return real(name, *args, **kwargs)

    monkeypatch.setattr(cli.importlib, "import_module", no_class)
    code, out = run(capsys, ["--format", "json"])
    portable = {engine["name"]: engine for engine in json.loads(out)["engines"]}[
        "portable"
    ]
    assert code == 0
    assert portable["installed"] is False
    assert portable["capabilities"] is None
    assert (
        portable["problem"]
        == "Engine class 'PortableEngine' is not in 'dq.engine.portable.portable_engine'"
    )
    code, out = run(capsys, [])
    assert "Cannot be used here:\n  portable: Engine class 'PortableEngine'" in out


def _entry_points(*entries):
    """Return an entry_points() double for the engine group."""
    from types import SimpleNamespace

    def entry_points():
        return SimpleNamespace(
            select=lambda group: (
                [SimpleNamespace(name=name, value=value) for name, value in entries]
                if group == ENTRY_POINT_GROUP
                else []
            )
        )

    return entry_points


def test_third_party_engines_registered_by_entry_point_are_listed(capsys, monkeypatch):
    import dq.engine.registry as registry

    monkeypatch.setattr(
        registry.importlib.metadata,
        "entry_points",
        _entry_points(
            ("Soda", "soda_dq.kernel:SodaEngine"), ("dbt", "dbt_dq.engine.DbtEngine")
        ),
    )
    code, out = run(capsys, ["--format", "json"])
    listed = {engine["name"]: engine for engine in json.loads(out)["engines"]}
    soda = listed["soda"]
    assert soda["source"] == "entry-point"
    assert (soda["module"], soda["class"], soda["payload_key"], soda["extras"]) == (
        "soda_dq.kernel",
        "SodaEngine",
        None,
        [],
    )
    assert listed["dbt"]["class"] == "DbtEngine"
    assert set(listed) == set(BUILT_IN_ENGINES) | {"soda", "dbt"}
    code, out = run(capsys, [])
    assert "soda (entry point)" in out


def test_an_entry_point_cannot_shadow_a_built_in(capsys, monkeypatch):
    import dq.engine.registry as registry

    monkeypatch.setattr(
        registry.importlib.metadata,
        "entry_points",
        _entry_points(("deequ", "evil.kernel:FakeEngine")),
    )
    _, out = run(capsys, ["--format", "json"])
    deequ = {engine["name"]: engine for engine in json.loads(out)["engines"]}["deequ"]
    assert deequ["source"] == "built-in" and deequ["class"] == "DeequEngine"


def test_an_unenumerable_metadata_backend_lists_no_third_party_engines(monkeypatch):
    import dq.engine.registry as registry

    def explode():
        raise RuntimeError("no metadata")

    monkeypatch.setattr(registry.importlib.metadata, "entry_points", explode)
    assert registry.entry_point_engines() == []
    assert all(engine["source"] == "built-in" for engine in cli.known_engines())


def test_the_command_is_registered_as_a_console_script():
    import tomllib
    from pathlib import Path

    scripts = tomllib.loads(Path("pyproject.toml").read_text())["tool"]["poetry"][
        "scripts"
    ]
    assert scripts["dq-engines"] == "dq.engines:main"


def test_the_command_runs_with_every_optional_dependency_blocked(tmp_path):
    """It answers without Spark; an engine it cannot import reports no manifest."""
    script = (
        "import logging; logging.disable(logging.CRITICAL); "
        "from dq.engines import known_engines, main; "
        "from dq.engine.registry import BUILT_IN_ENGINES; "
        "main([]); main(['--format', 'json']); "
        "engines = known_engines(); "
        "assert [e['name'] for e in engines] == sorted(BUILT_IN_ENGINES); "
        "by_name = {e['name']: e for e in engines}; "
        "assert by_name['deequ']['capabilities'] is None, by_name['deequ']; "
        "assert by_name['deequ']['installed'] is False; "
        "assert by_name['deequ']['install_hint']; "
        "print('listed without Spark')"
    )
    result = _run_with_blocked_imports(
        "boto3",
        "databricks",
        "pydeequ",
        "pyspark",
        "great_expectations",
        statement=script,
    )
    assert result.returncode == 0, result.stderr + result.stdout
