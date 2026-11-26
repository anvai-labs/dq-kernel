# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the rule-pack schema and its validator (SPEC-010).

The validator is small and written here, so it is held to two standards:
each keyword it claims to support is tested directly, and its verdicts are
cross-checked against the reference ``jsonschema`` implementation whenever
that library is installed.
"""

import copy
from pathlib import Path

import pytest

from dq.schema import (
    SCHEMA_PATH,
    SUPPORTED_KEYWORDS,
    rule_pack_schema,
    rule_pack_schema_errors,
    schema_keywords,
    validate_against_schema,
)
from dq.utils.config_utils import config_tree_to_python, load_config

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = REPOSITORY_ROOT / "examples"
RULE_PACK_EXTENSIONS = (".conf", ".hocon", ".yaml", ".yml", ".json")


def example_rule_packs():
    return sorted(
        path
        for path in EXAMPLES.rglob("*")
        # `examples/import/` holds result sources for `dq-import`, not rules.
        if path.is_file()
        and path.suffix in RULE_PACK_EXTENSIONS
        and "import" not in path.parts
    )


def as_document(path):
    return config_tree_to_python(load_config(path))


# ------------------------------------------------------------ the schema itself


def test_schema_is_shipped_in_the_package_and_is_draft_2020_12():
    schema = rule_pack_schema()
    assert SCHEMA_PATH.parent.name == "schemas" and SCHEMA_PATH.exists()
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["required"] == ["dqframework"]
    assert schema["properties"]["dqframework"]["required"] == ["dqrules"]


def test_schema_uses_only_keywords_the_bundled_validator_supports():
    unsupported = schema_keywords(rule_pack_schema()) - SUPPORTED_KEYWORDS
    assert not unsupported, f"the validator cannot evaluate {sorted(unsupported)}"


def test_engine_names_are_not_enumerated_in_the_schema():
    """Extension engines register through entry points; the schema stays open."""
    rule = rule_pack_schema()["$defs"]["rule"]["properties"]
    assert "enum" not in rule["engine"]


# ------------------------------------------------------- each supported keyword


@pytest.mark.parametrize(
    ("schema", "instance", "valid"),
    [
        ({"type": "string"}, "x", True),
        ({"type": "string"}, 1, False),
        ({"type": ["string", "null"]}, None, True),
        ({"type": "integer"}, 3, True),
        ({"type": "integer"}, 3.0, True),
        ({"type": "integer"}, 3.5, False),
        ({"type": "integer"}, True, False),
        ({"type": "number"}, 2.5, True),
        ({"type": "number"}, True, False),
        ({"type": "boolean"}, False, True),
        ({"type": "boolean"}, 0, False),
        ({"type": "null"}, None, True),
        ({"type": "null"}, "", False),
        ({"type": "object"}, {}, True),
        ({"type": "object"}, [], False),
        ({"type": "array"}, [1], True),
        ({"type": "array"}, (1,), True),
        ({"type": "array"}, {"a": 1}, False),
        ({"enum": ["a", "b"]}, "a", True),
        ({"enum": ["a", "b"]}, "c", False),
        ({"const": 1}, 1, True),
        ({"const": 1}, "1", False),
        ({"required": ["a"]}, {"a": 1}, True),
        ({"required": ["a"]}, {"b": 1}, False),
        ({"properties": {"a": {"type": "string"}}}, {"a": "s"}, True),
        ({"properties": {"a": {"type": "string"}}}, {"a": 1}, False),
        ({"properties": {"a": {}}, "additionalProperties": False}, {"a": 1}, True),
        ({"properties": {"a": {}}, "additionalProperties": False}, {"b": 1}, False),
        (
            {"additionalProperties": {"type": "number"}},
            {"anything": 2},
            True,
        ),
        (
            {"additionalProperties": {"type": "number"}},
            {"anything": "two"},
            False,
        ),
        ({"items": {"type": "integer"}}, [1, 2], True),
        ({"items": {"type": "integer"}}, [1, "2"], False),
        ({"minItems": 1}, [], False),
        ({"items": {"type": "integer"}, "minItems": 2}, [1], False),
        ({"maxItems": 2}, [1, 2, 3], False),
        ({"minLength": 2}, "ab", True),
        ({"minLength": 2}, "a", False),
        ({"maxLength": 2}, "abc", False),
        ({"pattern": "^[a-z]+$"}, "abc", True),
        ({"pattern": "^[a-z]+$"}, "aBc", False),
        ({"minimum": 3}, 3, True),
        ({"minimum": 3}, 2, False),
        ({"maximum": 3}, 4, False),
        ({"oneOf": [{"type": "string"}, {"type": "number"}]}, "s", True),
        ({"oneOf": [{"type": "string"}, {"type": "number"}]}, 1, True),
        ({"oneOf": [{"type": "string"}, {"type": "number"}]}, None, False),
        (
            {
                "oneOf": [
                    {"type": "string"},
                    {"type": "integer"},
                ]
            },
            True,
            False,
        ),
        ({"$ref": "#/$defs/name"}, "x", True),
        ({"$ref": "#/$defs/name"}, 1, False),
    ],
)
def test_each_keyword(schema, instance, valid):
    root = {"$defs": {"name": {"type": "string"}}}
    errors = validate_against_schema(instance, schema, root=root)
    assert (not errors) is valid, errors


def test_errors_name_the_location_and_the_expectation():
    errors = validate_against_schema(
        {"a": {"b": [1, "x"]}},
        {"properties": {"a": {"properties": {"b": {"items": {"type": "integer"}}}}}},
    )
    assert errors == ["$.a.b[1]: expected integer, got string"]


def test_a_type_mismatch_stops_deeper_checks_for_that_value():
    errors = validate_against_schema("text", {"type": "object", "required": ["x"]})
    assert errors == ["$: expected object, got string"]


def test_unsupported_schema_types_and_foreign_references_are_rejected():
    with pytest.raises(ValueError, match="unsupported schema type"):
        validate_against_schema(1, {"type": "wizard"})
    with pytest.raises(ValueError, match="only local schema references"):
        validate_against_schema(1, {"$ref": "https://elsewhere/schema.json#/x"})


# ----------------------------------------------------------- real rule packs


@pytest.mark.parametrize("path", example_rule_packs(), ids=lambda p: p.name)
def test_every_checked_in_example_validates_against_the_schema(path):
    assert rule_pack_schema_errors(load_config(path)) == [], path


def test_schema_errors_are_readable_and_located():
    tree = load_config(
        "dqframework {\n"
        "  catalog_type = unty\n"
        '  notifiers = [{ type = "slack", url_env = "SLACK_URL", colour = "red" }]\n'
        '  gate { fail_on = "sometimes" }\n'
        "  dqrules = [{ engine = deequ, dataframes = [] }]\n"
        "}\n"
    )
    assert sorted(rule_pack_schema_errors(tree)) == [
        "$.dqframework.catalog_type: 'unty' is not one of 'spark', 'unity', 'glue', 'hive'",
        "$.dqframework.dqrules[0].dataframes: expected at least 1 item(s)",
        "$.dqframework.gate.fail_on: 'sometimes' is not one of 'any', 'error', 'never'",
        "$.dqframework.notifiers[0]: unexpected key 'colour'",
    ]


def _mutations(document):
    """Yield (name, document) pairs, each one change away from a valid pack."""

    def clone():
        return copy.deepcopy(document)

    out = []
    m = clone()
    del m["dqframework"]
    out.append(("no-dqframework", m))
    m = clone()
    m["dqframework"]["dqrules"] = []
    out.append(("no-rules", m))
    m = clone()
    del m["dqframework"]["dqrules"][0]["engine"]
    out.append(("rule-without-engine", m))
    m = clone()
    m["dqframework"]["dqrules"][0]["engine"] = ""
    out.append(("blank-engine", m))
    m = clone()
    m["dqframework"]["dqrules"][0]["dataframes"] = []
    out.append(("no-dataframes", m))
    m = clone()
    m["dqframework"]["gate"] = {"fail_on": "sometimes"}
    out.append(("unknown-policy", m))
    m = clone()
    m["dqframework"]["gate"] = {"fail_on": "error", "other": 1}
    out.append(("unknown-gate-key", m))
    m = clone()
    m["dqframework"]["notifiers"] = [{"type": "pigeon"}]
    out.append(("unknown-notifier", m))
    m = clone()
    m["dqframework"]["notifiers"] = [{"url_env": "X"}]
    out.append(("notifier-without-type", m))
    m = clone()
    m["dqframework"]["notifiers"] = [{"type": "slack", "extra": 1}]
    out.append(("unknown-notifier-key", m))
    m = clone()
    m["dqframework"]["catalog_type"] = "nope"
    out.append(("unknown-catalog", m))
    out.append(("not-an-object", [document]))
    return out


def test_the_validator_rejects_each_mutation():
    document = as_document(EXAMPLES / "basic_deequ_validation.conf")
    assert rule_pack_schema_errors(document) == []
    for name, mutated in _mutations(document):
        assert rule_pack_schema_errors(mutated), f"{name} was accepted"


def test_validator_agrees_with_the_reference_jsonschema_implementation():
    """Cross-check: same verdicts, on real packs and on each mutation."""
    jsonschema = pytest.importorskip("jsonschema")
    schema = rule_pack_schema()
    validator = jsonschema.Draft202012Validator(schema)

    documents = [("valid", as_document(EXAMPLES / "basic_deequ_validation.conf"))]
    documents += _mutations(documents[0][1])
    disagreements = []
    for name, document in documents:
        mine = not validate_against_schema(document, schema)
        theirs = not list(validator.iter_errors(document))
        if mine != theirs:
            disagreements.append((name, mine, theirs))
    assert disagreements == []


@pytest.mark.parametrize("path", example_rule_packs(), ids=lambda p: p.name)
def test_every_example_is_also_valid_for_the_reference_validator(path):
    jsonschema = pytest.importorskip("jsonschema")
    errors = list(
        jsonschema.Draft202012Validator(rule_pack_schema()).iter_errors(
            as_document(path)
        )
    )
    assert errors == [], [error.message for error in errors]


# ------------------------------------------------------------- the wheel


def test_the_schema_lives_inside_the_packaged_directory_and_is_not_excluded():
    """`poetry build` ships every file under the package directory.

    CI verifies the built wheel actually carries the schema; here, guard
    the two ways it could be dropped: a file outside the package, or an
    explicit exclude in the packaging configuration.
    """
    import tomllib

    package_root = REPOSITORY_ROOT / "dq"
    assert SCHEMA_PATH.is_file() and package_root in SCHEMA_PATH.parents
    poetry = tomllib.loads(
        (REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )["tool"]["poetry"]
    for pattern in poetry.get("exclude", []):
        assert "schema" not in pattern and "json" not in pattern, pattern
    for entry in poetry.get("packages", []):
        assert entry.get("include") == "dq", entry
