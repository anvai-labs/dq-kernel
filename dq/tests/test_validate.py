# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest

from dq.validate import is_local_config_reference, validate_config


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
        ("schemavalidation", "schema = { id = { datatype = long } }"),
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


@pytest.mark.parametrize(
    "config_path", sorted(Path("examples").glob("*.conf")), ids=lambda path: path.name
)
def test_checked_in_example_is_a_valid_configuration(config_path):
    assert validate_config(str(config_path)) is True


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
