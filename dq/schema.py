# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Validate a rule pack, or a profile, against its published JSON Schema.

The rule-pack schema (SPEC-010) describes the *envelope*: the keys every
rule pack shares regardless of engine. Engine payloads stay open on
purpose -- each engine owns its vocabulary (ADR-012), and the key an
engine requires is declared once in the registry.

The profile schema (SPEC-019) describes a ``dq-profile/v1`` document: its
shape, which is what a schema can say. What the shape cannot say -- which
measures a type has, and that a least value is not above its greatest --
is enforced where a profile is read, in :mod:`dq.profile.profile`.

This is a small JSON Schema validator covering the keywords that schema
uses, rather than a dependency on a validation library: the core package
stays free of one more transitive surface, and the validator is checked
against the reference ``jsonschema`` implementation whenever that library
happens to be installed (it arrives with Great Expectations).
"""

import json
import re
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any, List

SCHEMA_PATH = Path(__file__).with_name("schemas") / "rule-pack.schema.json"
PROFILE_SCHEMA_PATH = Path(__file__).with_name("schemas") / "profile.schema.json"

#: Keywords this validator understands. The schema file is itself checked
#: against this set, so an unsupported keyword cannot slip in unnoticed.
SUPPORTED_KEYWORDS = frozenset(
    {
        "$comment",
        "$defs",
        "$id",
        "$ref",
        "$schema",
        "additionalProperties",
        "const",
        "description",
        "enum",
        "items",
        "maxItems",
        "maxLength",
        "maximum",
        "minItems",
        "minLength",
        "minimum",
        "oneOf",
        "pattern",
        "properties",
        "required",
        "title",
        "type",
    }
)


@lru_cache(maxsize=1)
def rule_pack_schema() -> dict:
    """Return the published rule-pack schema."""
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def profile_schema() -> dict:
    """Return the published profile schema (``dq-profile/v1``)."""
    return json.loads(PROFILE_SCHEMA_PATH.read_text(encoding="utf-8"))


def _is_type(value: Any, name: str) -> bool:
    if name == "object":
        return isinstance(value, Mapping)
    if name == "array":
        return isinstance(value, (list, tuple))
    if name == "string":
        return isinstance(value, str)
    if name == "boolean":
        return isinstance(value, bool)
    if name == "null":
        return value is None
    if name == "integer":
        return (isinstance(value, int) and not isinstance(value, bool)) or (
            isinstance(value, float) and value.is_integer()
        )
    if name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    raise ValueError(f"unsupported schema type {name!r}")


def _resolve(schema: dict, root: dict) -> dict:
    """Follow a local ``$ref`` (``#/$defs/name``) to its target."""
    while "$ref" in schema:
        ref = schema["$ref"]
        if not ref.startswith("#/"):
            raise ValueError(f"only local schema references are supported, got {ref!r}")
        target = root
        for part in ref[2:].split("/"):
            target = target[part]
        schema = target
    return schema


def _describe(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, (list, tuple)):
        return "array"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "integer"
    return "number"


def validate_against_schema(
    instance: Any, schema: dict, *, root: dict = None, path: str = "$"
) -> List[str]:
    """Return every way ``instance`` violates ``schema`` (empty when valid).

    Args:
        instance: Plain JSON-native data (mappings, lists, scalars).
        schema: A JSON Schema using :data:`SUPPORTED_KEYWORDS`.
        root: The top-level schema document, for local references.
        path: JSON pointer of ``instance``, used in error messages.
    """
    root = schema if root is None else root
    schema = _resolve(schema, root)
    errors: List[str] = []
    where = path

    if "type" in schema:
        names = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if not any(_is_type(instance, name) for name in names):
            expected = " or ".join(names)
            return [f"{where}: expected {expected}, got {_describe(instance)}"]

    if "const" in schema and instance != schema["const"]:
        errors.append(f"{where}: must be {schema['const']!r}")
    if "enum" in schema and instance not in schema["enum"]:
        allowed = ", ".join(repr(choice) for choice in schema["enum"])
        errors.append(f"{where}: {instance!r} is not one of {allowed}")

    if isinstance(instance, Mapping):
        for key in schema.get("required", []):
            if key not in instance:
                errors.append(f"{where}: missing required key '{key}'")
        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        for key, value in instance.items():
            child = f"{where}.{key}"
            if key in properties:
                errors.extend(
                    validate_against_schema(
                        value, properties[key], root=root, path=child
                    )
                )
            elif additional is False:
                errors.append(f"{where}: unexpected key '{key}'")
            elif isinstance(additional, dict):
                errors.extend(
                    validate_against_schema(value, additional, root=root, path=child)
                )

    if isinstance(instance, (list, tuple)):
        items = schema.get("items")
        if isinstance(items, dict):
            for index, value in enumerate(instance):
                errors.extend(
                    validate_against_schema(
                        value, items, root=root, path=f"{where}[{index}]"
                    )
                )
        if "minItems" in schema and len(instance) < schema["minItems"]:
            errors.append(f"{where}: expected at least {schema['minItems']} item(s)")
        if "maxItems" in schema and len(instance) > schema["maxItems"]:
            errors.append(f"{where}: expected at most {schema['maxItems']} item(s)")

    if isinstance(instance, str):
        if "minLength" in schema and len(instance) < schema["minLength"]:
            errors.append(
                f"{where}: expected a string of at least {schema['minLength']} character(s)"
            )
        if "maxLength" in schema and len(instance) > schema["maxLength"]:
            errors.append(
                f"{where}: expected a string of at most {schema['maxLength']} character(s)"
            )
        if "pattern" in schema and not re.search(schema["pattern"], instance):
            errors.append(f"{where}: {instance!r} does not match {schema['pattern']!r}")

    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        if "minimum" in schema and instance < schema["minimum"]:
            errors.append(f"{where}: must be >= {schema['minimum']}")
        if "maximum" in schema and instance > schema["maximum"]:
            errors.append(f"{where}: must be <= {schema['maximum']}")

    if "oneOf" in schema:
        matched = sum(
            1
            for option in schema["oneOf"]
            if not validate_against_schema(instance, option, root=root, path=where)
        )
        if matched != 1:
            errors.append(
                f"{where}: must match exactly one alternative (matched {matched})"
            )
    return errors


#: Keywords whose child keys are data (property or definition names), not
#: keywords themselves.
_CONTAINERS_OF_NAMES = ("properties", "$defs", "definitions", "dependentSchemas")


def schema_keywords(schema: Any) -> set:
    """Return every keyword a schema document uses.

    Keys beneath ``properties`` and ``$defs`` name properties and
    definitions, so they are not keywords and are not collected.
    """
    found = set()
    if isinstance(schema, dict):
        for key, value in schema.items():
            found.add(key)
            if key in _CONTAINERS_OF_NAMES and isinstance(value, dict):
                for child in value.values():
                    found |= schema_keywords(child)
            else:
                found |= schema_keywords(value)
    elif isinstance(schema, list):
        for value in schema:
            found |= schema_keywords(value)
    return found


def rule_pack_schema_errors(config: Any) -> List[str]:
    """Validate a rule pack (``ConfigTree`` or mapping) against the schema.

    Args:
        config: The parsed rule pack.

    Returns:
        Violations as readable strings; empty when the pack is valid.
    """
    from dq.utils.config_utils import config_tree_to_python

    return validate_against_schema(config_tree_to_python(config), rule_pack_schema())


def profile_schema_errors(profile: Any) -> List[str]:
    """Validate a profile document against its schema.

    Args:
        profile: A profile as a mapping, as a ``Profile.to_document()`` gives.

    Returns:
        Violations as readable strings; empty when the profile is valid.
    """
    return validate_against_schema(profile, profile_schema())
