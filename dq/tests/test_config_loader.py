# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the format-neutral rule-pack loader (SPEC-010).

A rule pack may be HOCON, YAML or JSON, inline, a path, or a URI. Every
combination must normalize to the same ``ConfigTree`` and fail closed with
a message a person can act on.
"""

import json
import re
from pathlib import Path

import pytest
import yaml
from pyhocon import ConfigFactory, ConfigTree

import dq.utils.config_utils as config_utils
from dq.exceptions import ConfigurationError
from dq.utils.config_utils import (
    config_format,
    config_get,
    config_tree_to_python,
    load_config,
    parse_config_text,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = REPOSITORY_ROOT / "examples"

RULES = [
    {
        "name": "basic",
        "engine": "deequ",
        "dataframes": ["default"],
        "checks": [
            {
                "constraint_name": "id_complete",
                "constraint": "isComplete",
                "column": "id",
                "level": "Error",
            }
        ],
    }
]
PACK = {"dqframework": {"dqrules": RULES, "gate": {"fail_on": "error"}}}


def write(tmp_path, name, text):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        ("rules.conf", "hocon"),
        ("rules.HOCON", "hocon"),
        ("rules.yaml", "yaml"),
        ("rules.yml", "yaml"),
        ("rules.json", "json"),
        ("rules", "hocon"),
        ("s3://bucket/prefix/rules.yaml", "yaml"),
        ("abfss://c@a.dfs.core.windows.net/x/rules.json", "json"),
        ("https://host/rules.conf?x=1", "hocon"),
    ],
)
def test_format_follows_the_file_extension(reference, expected):
    assert config_format(reference) == expected


def test_hocon_yaml_and_json_load_to_equal_trees(tmp_path):
    hocon = write(
        tmp_path,
        "rules.conf",
        "dqframework {\n"
        "  dqrules = [{\n"
        '    name = "basic"\n'
        "    engine = deequ\n"
        '    dataframes = ["default"]\n'
        "    checks = [{ constraint_name = id_complete, constraint = isComplete,"
        ' column = id, level = "Error" }]\n'
        "  }]\n"
        '  gate { fail_on = "error" }\n'
        "}\n",
    )
    as_yaml = write(tmp_path, "rules.yaml", yaml.safe_dump(PACK, sort_keys=False))
    as_json = write(tmp_path, "rules.json", json.dumps(PACK))

    trees = [
        config_tree_to_python(load_config(path)) for path in (hocon, as_yaml, as_json)
    ]
    assert trees[0] == trees[1] == trees[2] == PACK


def test_bare_paths_load_with_and_without_the_file_scheme(tmp_path, monkeypatch):
    path = write(tmp_path, "rules.conf", yaml.safe_dump(PACK))
    monkeypatch.chdir(tmp_path)

    via_relative = load_config("rules.conf")
    via_absolute = load_config(str(path))
    via_scheme = load_config(f"file://{path}")
    via_scheme_relative = load_config("file://rules.conf")
    for tree in (via_relative, via_absolute, via_scheme, via_scheme_relative):
        assert isinstance(tree, ConfigTree)
        assert config_tree_to_python(tree) == config_tree_to_python(via_absolute)


@pytest.mark.parametrize(
    "text",
    ["a = 1", "a: 1", "a.b = 1", "a { b = 2 }", "  a = 1\n  b = 2"],
    ids=["equals", "colon", "dotted", "braces", "multiline"],
)
def test_inline_hocon_text_is_parsed_as_text(text):
    assert load_config(text) is not None


def test_a_missing_file_is_reported_as_missing_not_as_a_parse_error():
    with pytest.raises(ConfigurationError, match="does not exist") as caught:
        load_config("no/such/rules.conf")
    assert "pyparsing" not in str(caught.value)


def test_invalid_hocon_reports_a_short_message_with_the_position(tmp_path):
    path = write(tmp_path, "broken.conf", "dqframework {\n  dqrules = [\n")
    with pytest.raises(ConfigurationError) as caught:
        load_config(str(path))
    message = str(caught.value)
    assert "broken.conf" in message
    assert "is not valid HOCON" in message
    assert "line:" in message
    assert len(message) < 300, "a parser dump is not an error message"


def test_inline_hocon_errors_are_short_too():
    with pytest.raises(ConfigurationError) as caught:
        load_config("dqframework { dqrules = [ { engine")
    assert len(str(caught.value)) < 300


@pytest.mark.parametrize(
    ("fmt", "text"),
    [
        ("yaml", "- a\n- b\n"),
        ("json", "[1, 2]\n"),
        ("yaml", "just a string\n"),
        ("json", "42\n"),
    ],
    ids=["yaml-list", "json-list", "yaml-scalar", "json-scalar"],
)
def test_yaml_and_json_packs_must_be_mappings(fmt, text):
    with pytest.raises(ConfigurationError, match="must be a mapping"):
        parse_config_text(text, fmt, source="rules." + fmt)


def test_invalid_yaml_and_json_name_the_source_and_the_cause(tmp_path):
    bad_yaml = write(tmp_path, "rules.yaml", "dqframework: [unclosed\n")
    bad_json = write(tmp_path, "rules.json", '{"dqframework": ')
    with pytest.raises(ConfigurationError, match="rules.yaml is not valid YAML"):
        load_config(str(bad_yaml))
    with pytest.raises(ConfigurationError, match="rules.json is not valid JSON"):
        load_config(str(bad_json))


def test_an_unsupported_format_fails_closed():
    with pytest.raises(ConfigurationError, match="Unsupported configuration format"):
        parse_config_text("a = 1", "toml")


def test_yaml_does_not_construct_arbitrary_objects(tmp_path):
    """Only safe YAML loads: a python-object tag is rejected, never executed."""
    hostile = write(
        tmp_path,
        "rules.yaml",
        "dqframework: !!python/object/apply:os.system ['echo pwned']\n",
    )
    with pytest.raises(ConfigurationError, match="not valid YAML"):
        load_config(str(hostile))


@pytest.mark.parametrize("value", [None, "", "   ", 42, b"dqframework {}"])
def test_empty_or_non_text_input_fails_closed(value):
    with pytest.raises(ConfigurationError, match="non-empty string"):
        load_config(value)


def test_mappings_and_trees_pass_through():
    from_dict = load_config(PACK)
    assert config_tree_to_python(from_dict) == PACK
    tree = ConfigFactory.from_dict(PACK)
    assert load_config(tree) is tree


def test_unsupported_scheme_fails_closed():
    with pytest.raises(ConfigurationError, match="Unsupported config scheme: ftp"):
        load_config("ftp://example.invalid/rules.conf")


# -------------------------------------------------------------- remote sources


@pytest.mark.parametrize("fmt", ["yaml", "json", "hocon"])
def test_remote_sources_load_in_every_format_through_the_existing_seams(
    monkeypatch, fmt
):
    if fmt == "yaml":
        text = yaml.safe_dump(PACK)
    elif fmt == "json":
        text = json.dumps(PACK)
    else:
        text = "dqframework { dqrules = [{ engine = deequ }] }"
    seen = {}

    def fake_s3(bucket, key):
        seen["s3"] = (bucket, key)
        return text

    monkeypatch.setattr(config_utils, "load_from_s3", fake_s3)
    monkeypatch.setattr(config_utils, "load_from_uri", lambda uri: text)
    monkeypatch.setattr(config_utils, "load_from_adls", lambda uri: text)

    s3 = load_config(f"s3://dq-bucket/prefix/rules.{fmt}")
    http = load_config(f"https://example.invalid/rules.{fmt}")
    adls = load_config(f"abfss://c@a.dfs.core.windows.net/rules.{fmt}")
    assert seen["s3"] == ("dq-bucket", "/prefix/rules." + fmt)
    for tree in (s3, http, adls):
        assert config_tree_to_python(tree)["dqframework"]["dqrules"]


def test_a_failing_remote_source_is_a_configuration_error(monkeypatch):
    def boom(bucket, key):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(config_utils, "load_from_s3", boom)
    with pytest.raises(
        ConfigurationError, match="Failed to load configuration from s3://"
    ) as caught:
        load_config("s3://dq-bucket/rules.conf")
    assert isinstance(caught.value.__cause__, RuntimeError)


def test_a_non_200_response_from_the_uri_helper_is_a_configuration_error(monkeypatch):
    class Response:
        status_code = 404

    monkeypatch.setattr(config_utils.requests, "get", lambda uri, timeout: Response())
    with pytest.raises(ConfigurationError, match="HTTP 404"):
        config_utils.load_from_uri("https://example.invalid/rules.conf")


# ------------------------------------------------------------------- overlays


def test_overlay_entry_file_includes_the_base_and_reads_the_environment(
    tmp_path, monkeypatch
):
    write(
        tmp_path,
        "base.conf",
        'dqframework {\n  dataset = "orders"\n  gate {\n    fail_on = "error"\n  }\n'
        "  dqrules = [{ engine = deequ }]\n}\n",
    )
    write(
        tmp_path,
        "prod.conf",
        'include "base.conf"\ndqframework.dataset = "orders-prod"\n'
        "dqframework.gate.fail_on = ${?DQ_GATE_FAIL_ON}\n",
    )
    from dq.config import parse_rule_pack

    monkeypatch.chdir(tmp_path)  # includes resolve relative to the file, not the cwd
    monkeypatch.setenv("DQ_GATE_FAIL_ON", "never")
    overlay = parse_rule_pack(load_config(str(tmp_path / "prod.conf")))
    assert overlay.dataset == "orders-prod"
    assert overlay.gate_policy == "never"

    # An unset optional variable omits the key, so the base value stands.
    monkeypatch.delenv("DQ_GATE_FAIL_ON")
    overlay = parse_rule_pack(load_config(str(tmp_path / "prod.conf")))
    assert overlay.gate_policy == "error"
    assert len(overlay.rules) == 1


def test_checked_in_overlay_example_reads_the_environment(monkeypatch):
    from dq.config import parse_rule_pack

    entry = str(EXAMPLES / "overlays" / "prod.conf")
    monkeypatch.setenv("DQ_GATE_FAIL_ON", "never")
    assert parse_rule_pack(load_config(entry)).gate_policy == "never"
    monkeypatch.delenv("DQ_GATE_FAIL_ON")
    overlay = parse_rule_pack(load_config(entry))
    assert overlay.gate_policy == "error"
    assert overlay.dataset == "orders-prod"
    assert len(overlay.rules) == 1


# ---------------------------------------------------------- secrets hygiene

#: A URL whose userinfo part carries a password.
_CREDENTIALED_URL = re.compile(r"[a-z][a-z0-9+.-]*://[^/\s:@]+:[^@\s/]+@")
#: A secret-named key assigned a literal value rather than a reference.
_SECRET_KEY = re.compile(
    r"(?i)\b(password|passwd|secret|token|api[_-]?key|access[_-]?key|client[_-]?secret)"
    r"\s*[:=]\s*[\"']?([A-Za-z0-9+/=_\-]{8,})"
)
#: Accepted ways to name a secret without stating it.
_REFERENCE = re.compile(r"\$\{[?]?[A-Z0-9_]+\}|url_env|os\.environ|getenv\(")


def _inline_credential_findings(text):
    findings = [
        f"credentialed URL: {match.group(0)}"
        for match in _CREDENTIALED_URL.finditer(text)
    ]
    for match in _SECRET_KEY.finditer(text):
        if not _REFERENCE.search(match.group(2)):
            findings.append(f"literal secret for '{match.group(1)}'")
    return findings


def test_the_credential_scanner_detects_a_real_offence():
    """The hygiene contract is only worth having if its scanner can fail."""
    assert _inline_credential_findings(
        "jdbc: postgresql://admin:S3cr3tPW@host:5432/db\n"
    )
    assert _inline_credential_findings('password = "hunter22"\n')
    assert _inline_credential_findings("api_key: ABCDEFGH12345\n") == [
        "literal secret for 'api_key'"
    ]
    assert _inline_credential_findings("") == []


def test_examples_name_secrets_by_reference_never_by_value():
    offenders = {}
    for path in sorted(EXAMPLES.rglob("*")):
        if (
            path.suffix not in (".conf", ".hocon", ".yaml", ".yml", ".json", ".py")
            or not path.is_file()
        ):
            continue
        findings = _inline_credential_findings(path.read_text(encoding="utf-8"))
        if findings:
            offenders[path.relative_to(REPOSITORY_ROOT).as_posix()] = findings
    assert offenders == {}, f"examples carry inline credentials: {offenders}"


# ------------------------------------------------------- config_get (shared)


def test_config_get_serves_trees_nested_and_flat_mappings():
    assert (
        config_get(ConfigFactory.from_dict(PACK), "dqframework.gate.fail_on") == "error"
    )
    assert config_get(PACK, "dqframework.gate.fail_on") == "error"
    assert (
        config_get({"dqframework.gate.fail_on": "never"}, "dqframework.gate.fail_on")
        == "never"
    )
    assert config_get(PACK, "dqframework.missing", "fallback") == "fallback"


@pytest.mark.parametrize(
    "text",
    [
        'dqframework { dataset = "d" gate { fail_on = "error" } }',
        'dqframework { dataset = "d" dqrules = [{ engine = deequ }] }',
    ],
    ids=["object-value", "list-value"],
)
def test_several_keys_on_one_line_are_explained(text):
    """pyhocon reads one `key = value` per line; the loader says so."""
    with pytest.raises(
        ConfigurationError, match="one `key = value` per line"
    ) as caught:
        load_config(text)
    assert "<inline>" in str(caught.value)
    assert "ConfigTree(" not in str(caught.value)


def test_an_invalid_included_file_is_reported_by_name(tmp_path):
    write(
        tmp_path,
        "base.conf",
        'dqframework { dataset = "d" gate { fail_on = "error" } dqrules = [{ engine = deequ }] }\n',
    )
    write(tmp_path, "prod.conf", 'include "base.conf"\ndqframework.dataset = "prod"\n')
    with pytest.raises(ConfigurationError, match="included file\(s\) 'base.conf'"):
        load_config(str(tmp_path / "prod.conf"))


@pytest.mark.parametrize(
    "text",
    [
        "dqframework { dqrules = [ { engine",
        "dqframework {\n  dqrules = [{ engine = deequ\n",
    ],
    ids=["truncated-inline", "truncated-file-like"],
)
def test_an_unrecognized_parse_failure_gets_no_hint(text, tmp_path):
    """A hint for an error the loader cannot diagnose would be a guess."""
    path = write(tmp_path, "rules.conf", text + "\n")
    for reference in (str(path), text):
        with pytest.raises(ConfigurationError) as caught:
            load_config(reference)
        assert "Hint:" not in str(caught.value)
        assert len(str(caught.value)) < 300
