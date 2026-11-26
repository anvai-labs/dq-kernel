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
        "extra": "deequ",
        "installed": True,
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
        "Extra",
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
        lambda spec: ("some.vendor",) if spec.name == "deequ" else (),
    )
    code, out = run(capsys, ["--format", "json"])
    deequ = {engine["name"]: engine for engine in json.loads(out)["engines"]}["deequ"]
    assert code == 0
    assert deequ["installed"] is False
    assert deequ["capabilities"] is None
    assert (
        deequ["install_hint"]
        == "pip install 'dq-kernel[deequ]'  (or: poetry install -E deequ)"
    )
    code, out = run(capsys, [])
    (deequ_row,) = [
        re.split(r"\s{2,}", line.strip())
        for line in out.splitlines()
        if line.startswith("deequ")
    ]
    assert deequ_row[1:4] == ["checks", "deequ", "no"]
    assert deequ_row[4:] == ["-", "-", "-", "-"]  # no manifest could be read
    assert "  deequ: pip install 'dq-kernel[deequ]'" in out


def test_an_engine_that_fails_to_import_is_reported_as_unavailable(capsys, monkeypatch):
    import importlib

    real = importlib.import_module

    def broken(name, *args, **kwargs):
        if name == "dq.engine.portable.portable_engine":
            raise RuntimeError("broken import")
        return real(name, *args, **kwargs)

    monkeypatch.setattr(cli.importlib, "import_module", broken)
    _, out = run(capsys, ["--format", "json"])
    portable = {engine["name"]: engine for engine in json.loads(out)["engines"]}[
        "portable"
    ]
    assert portable["installed"] is False
    assert portable["capabilities"] is None
    # The remedy names the extra that provides the dependency.
    assert (
        portable["install_hint"]
        == "pip install 'dq-kernel[spark]'  (or: poetry install -E spark)"
    )


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
    assert (soda["module"], soda["class"], soda["payload_key"], soda["extra"]) == (
        "soda_dq.kernel",
        "SodaEngine",
        None,
        None,
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
