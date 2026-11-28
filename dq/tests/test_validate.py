# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from dq.validate import (
    is_local_config_reference,
    validate_config,
    validate_config_document,
)
from dq.validate import main as validate_main


def write_config(tmp_path, body):
    path = tmp_path / "rules.conf"
    path.write_text(body, encoding="utf-8")
    return str(path)


def test_validate_config_rejects_empty_rule_set(tmp_path):
    path = write_config(tmp_path, "dqframework { dqrules = [] }\n")
    assert validate_config(path) is False


def test_validate_config_rejects_rule_without_checks(tmp_path):
    path = write_config(
        tmp_path,
        'dqframework { dqrules = [{ engine = "deequ", checks = [] }] }\n',
    )
    assert validate_config(path) is False


def test_validate_config_rejects_unvalidated_remote_source():
    assert validate_config("https://example.test/rules.conf") is False


def test_validate_config_rejects_renamed_custom_engine(tmp_path):
    # engine = "custom" was renamed to "businessrules"; validation must
    # fail closed with the migration hint instead of waving it through.
    path = write_config(
        tmp_path,
        'dqframework { dqrules = [{ engine = "custom", checks = [] }] }\n',
    )
    assert validate_config(path) is False


def test_validate_config_accepts_businessrules_engine_with_checks(tmp_path):
    path = write_config(
        tmp_path,
        'dqframework { dqrules = [{ engine = "businessrules",'
        ' checks = [{ constraint = "NoNegativeValues" }] }] }\n',
    )
    assert validate_config(path) is True


def test_a_caller_can_ask_for_the_verdict_without_the_report(tmp_path, caplog):
    """dq-suggest reports the verdict on the draft it wrote in its own words:
    the validator's line about a file would read as if the user had asked."""
    caplog.set_level(logging.INFO)
    whole = (
        'dqframework { dqrules = [{ engine = "businessrules",'
        ' checks = [{ constraint = "NoNegativeValues" }] }] }\n'
    )
    broken = 'dqframework { dqrules = [{ engine = "deequ", checks = [] }] }\n'
    before = len(caplog.records)

    assert validate_config_document(write_config(tmp_path, broken), report=False).errors
    assert validate_config_document(write_config(tmp_path, whole), report=False).valid
    assert len(caplog.records) == before  # nothing logged, either way

    validate_config_document(write_config(tmp_path, broken))
    assert len(caplog.records) == before + 1 and caplog.records[-1].levelname == "ERROR"
    validate_config_document(write_config(tmp_path, whole))
    assert (
        caplog.records[-1].levelname == "INFO"
        and "valid" in caplog.records[-1].getMessage()
    )


def test_validate_config_rejects_businessrules_without_checks(tmp_path):
    path = write_config(
        tmp_path,
        'dqframework { dqrules = [{ engine = "businessrules", checks = [] }] }\n',
    )
    assert validate_config(path) is False


def test_validate_config_rejects_missing_engine(tmp_path):
    path = write_config(tmp_path, "dqframework { dqrules = [{ checks = [1] }] }")
    assert validate_config(path) is False


@pytest.mark.parametrize("ruleset", ['"   "', "123", "[]"])
def test_validate_config_rejects_invalid_dqdl_payload(tmp_path, ruleset):
    path = write_config(
        tmp_path,
        f'dqframework {{ dqrules = [{{ engine = "dqdl", ruleset = {ruleset} }}] }}',
    )
    assert validate_config(path) is False


def test_local_config_reference_detection():
    assert is_local_config_reference("rules.conf") is True
    assert is_local_config_reference("file:///tmp/rules.conf") is True
    assert is_local_config_reference("s3://bucket/rules.conf") is False


def test_validate_config_accepts_executable_local_rule_set(tmp_path):
    path = write_config(
        tmp_path,
        """
        dqframework {
          dqrules = [{
            engine = "deequ"
            checks = [{ constraint = "hasSize", value = 1 }]
          }]
        }
        """,
    )
    assert validate_config(path) is True


@pytest.mark.parametrize(
    ("engine", "payload"),
    [
        ("dqdl", 'ruleset = "Rules=[RowCount >= 1]"'),
        (
            "portable",
            'plan = [{ constraint = "isComplete", column = "id" }], '
            'dataset = "snap", snapshot_sha256 = "' + "a" * 64 + '"',
        ),
        ("schemavalidation", "schema = { id = { datatype = long } }"),
        (
            "pandera",
            'schema = { schema_type = "dataframe", columns = { id = { dtype = "int64" } } }',
        ),
        (
            "greatexpectations",
            'expectations = [{ type = "expect_column_to_exist", column = "id" }]',
        ),
    ],
)
def test_validate_config_accepts_engine_specific_payloads(tmp_path, engine, payload):
    path = write_config(
        tmp_path,
        f'dqframework {{ dqrules = [{{ engine = "{engine}", {payload} }}] }}\n',
    )
    assert validate_config(path) is True


def test_validate_config_rejects_missing_built_in_engine_payload(tmp_path):
    path = write_config(
        tmp_path,
        'dqframework { dqrules = [{ engine = "schemavalidation" }] }\n',
    )
    assert validate_config(path) is False


def test_validate_config_defers_extension_payload_to_extension(tmp_path):
    path = write_config(
        tmp_path,
        'dqframework { dqrules = [{ engine = "organization_plugin", rules = [1] }] }\n',
    )
    assert validate_config(path) is True


# `examples/import/` holds result sources for `dq-import` -- a dbt artifact,
# a Great Expectations result -- not rule packs.
RULE_PACKS = sorted(
    path
    for path in Path("examples").rglob("*")
    if path.is_file()
    and path.suffix in (".conf", ".hocon", ".yaml", ".yml", ".json")
    and "import" not in path.parts
)


@pytest.mark.parametrize(
    "config_path", RULE_PACKS, ids=lambda path: path.relative_to("examples").as_posix()
)
def test_checked_in_example_is_a_valid_configuration(config_path):
    assert validate_config(str(config_path)) is True


def test_every_example_format_is_represented():
    suffixes = {path.suffix for path in RULE_PACKS}
    assert {".conf", ".yaml", ".json"} <= suffixes


@pytest.mark.parametrize("suffix", [".yaml", ".yml", ".json"])
def test_validate_reads_every_format(tmp_path, suffix):
    path = tmp_path / f"rules{suffix}"
    if suffix == ".json":
        path.write_text(
            '{"dqframework": {"dqrules": [{"engine": "deequ", "checks": [1]}]}}'
        )
    else:
        path.write_text(
            "dqframework:\n  dqrules:\n    - engine: deequ\n      checks: [1]\n"
        )
    assert validate_config(str(path)) is True
    assert validate_config_document(str(path)).format.lstrip(".") in ("yaml", "json")


def test_validate_reports_every_problem_of_the_first_failing_stage(tmp_path):
    """Stages: a schema problem hides later stages, so nothing is reported twice."""
    path = tmp_path / "rules.conf"
    path.write_text(
        "dqframework {\n"
        '  catalog_type = "nope"\n'
        '  gate { fail_on = "sometimes" }\n'
        "  dqrules = [{ name = 3, engine = deequ, checks = 1 }]\n"
        "}\n"
    )
    result = validate_config_document(str(path))
    assert result.valid is False
    assert [error for error in result.errors if error.startswith("Schema: ")] == [
        "Schema: $.dqframework.catalog_type: 'nope' is not one of 'spark', 'unity', 'glue', 'hive'",
        "Schema: $.dqframework.gate.fail_on: 'sometimes' is not one of 'any', 'error', 'never'",
        "Schema: $.dqframework.dqrules[0].name: expected string, got integer",
    ]
    assert len(result.errors) == 3


def test_validate_reports_a_rule_shape_problem_for_every_broken_rule(tmp_path):
    """The rule-shape stage is exhaustive, not first-error-only.

    The snapshot is also stricter than the schema here: a whitespace-only
    name has the length the schema asks for and still names nothing.
    """
    path = tmp_path / "rules.conf"
    path.write_text(
        "dqframework { dqrules = [\n"
        '  { engine = "deequ", dataframes = ["  "], checks = [1] }\n'
        '  { engine = "greatexpectations", name = "  ", checks = [1] }\n'
        '  { engine = "deequ", checks = [1] }\n'
        "] }\n"
    )
    result = validate_config_document(str(path))
    assert result.errors == (
        "Rule 0: key 'dataframes' must name at least one dataframe, each with a "
        "non-empty name",
        "Rule 1: key 'name' must be a non-empty string when given",
    )


def test_payload_problems_wait_until_every_rule_is_well_formed(tmp_path):
    path = tmp_path / "rules.conf"
    path.write_text(
        "dqframework { dqrules = [\n"
        '  { engine = "greatexpectations", checks = [1] }\n'
        '  { engine = "  " }\n'
        "] }\n"
    )
    assert validate_config_document(str(path)).errors == (
        "Rule 1: key 'engine' must be a non-empty engine name",
    )


def test_validate_reports_a_missing_engine_payload_after_the_snapshot(tmp_path):
    path = tmp_path / "rules.conf"
    path.write_text(
        'dqframework { dqrules = [{ engine = "greatexpectations", checks = [1] }] }\n'
    )
    result = validate_config_document(str(path))
    assert result.errors == (
        "Rule 0: Engine 'greatexpectations' requires a non-empty 'expectations' payload",
    )


def test_validate_accepts_inline_hocon_text():
    assert (
        validate_config(
            'dqframework { dqrules = [{ engine = "deequ", checks = [1] }] }'
        )
        is True
    )


def test_validate_names_a_broken_file_rather_than_a_parser_dump(tmp_path):
    path = tmp_path / "rules.conf"
    path.write_text("dqframework { dqrules = [\n")
    result = validate_config_document(str(path))
    assert result.valid is False
    (error,) = result.errors
    assert "rules.conf" in error and len(error) < 300


def test_validate_json_output_lists_every_error(tmp_path, capsys):
    path = tmp_path / "rules.yaml"
    path.write_text("dqframework:\n  dqrules: []\n")
    with patch.object(sys, "argv", ["dq-validate", "--format", "json", str(path)]):
        exit_code = validate_main()
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["config"] == str(path)
    assert payload["format"] == "yaml"
    assert payload["valid"] is False
    assert payload["errors"] == [
        "Schema: $.dqframework.dqrules: expected at least 1 item(s)"
    ]


def test_validate_cli_entry_point_is_registered():
    import tomllib

    scripts = tomllib.loads(Path("pyproject.toml").read_text())["tool"]["poetry"][
        "scripts"
    ]
    assert scripts["dq-validate"] == "dq.validate:main"


def test_validate_config_rejects_missing_dqframework_block(tmp_path):
    path = write_config(tmp_path, "not_dqframework {}\n")
    assert validate_config(path) is False


def test_main_reports_valid_and_invalid_configs(tmp_path, capsys):
    from unittest.mock import patch

    import dq.validate as validate_module

    valid_dir = tmp_path / "valid"
    invalid_dir = tmp_path / "invalid"
    valid_dir.mkdir()
    invalid_dir.mkdir()
    valid = write_config(
        valid_dir,
        'dqframework { dqrules = [{ engine = "deequ",'
        ' checks = [{ constraint = "isComplete" }] }] }\n',
    )
    invalid = write_config(
        invalid_dir,
        'dqframework { dqrules = [{ engine = "custom", checks = [] }] }\n',
    )
    with patch("sys.argv", ["dq-validate", valid, "--format", "json"]):
        assert validate_module.main() == 0
    with patch("sys.argv", ["dq-validate", invalid, "--verbose"]):
        assert validate_module.main() == 1
    out = capsys.readouterr().out
    assert "is invalid" in out
