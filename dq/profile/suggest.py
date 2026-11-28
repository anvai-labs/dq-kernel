# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Suggesters: a profile in, a draft rule pack out (SPEC-019).

Pure functions of the profile -- the same profile always yields the same
draft -- and no engine is imported to write one. No check is invented:
each suggestion is written in the words its target already accepts, and
only where the target has the check and its own would confirm the
suggestion on the profile's data. The kernel's portable subset has no
uniqueness check, so its suggester emits none.

Three suggestions are made, each only where the profile warrants it: a
column with no nulls is complete; a column that is exactly distinct, by an
exact count, and has no nulls is unique; an integer, decimal or float
column whose least value is 0 or more is non-negative. None is made for a
column whose name is not a plain identifier, which is how a rule asks for
a column. Every suggested rule is a warning, and the pack names the gate
policy under which a warning blocks nothing: nothing here gates a pipeline
until a human promotes it.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, NamedTuple, Optional, Tuple

import yaml

from dq.engine.registry import find_spec
from dq.exceptions import ConfigurationError
from dq.identifiers import ColumnName, IdentifierError
from dq.profile.profile import (
    DECIMAL,
    FLOAT,
    INTEGER,
    NESTED,
    Column,
    Profile,
    unspell,
)

#: The vocabularies a draft can be written in. Each is an engine the
#: registry knows, so its payload key is the registry's.
TARGETS = ("portable", "deequ", "dqx", "greatexpectations")

#: The rule-pack formats a draft can be written as.
FORMATS = ("hocon", "yaml", "json")

COMPLETE = "complete"
UNIQUE = "unique"
NON_NEGATIVE = "non_negative"

#: The portable subset's words for what it can say of one column.
_PORTABLE = {COMPLETE: "isComplete", NON_NEGATIVE: "isNonNegative"}

#: A key HOCON takes unquoted; every other key is quoted, since a dot in
#: an unquoted key is a path.
_PLAIN_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class Suggestion(NamedTuple):
    """One thing the profile warrants about one column."""

    column: str
    kind: str

    @property
    def name(self) -> str:
        """Return the suggestion's name: the column, then what is said of it."""
        return f"{self.column}_{self.kind}"


def addressable(name: str) -> bool:
    """Return whether a rule can ask for a column by this name.

    A rule asks for a column by a name that is a plain identifier, as the
    kernel reads one (``dq.identifiers.ColumnName``: letters, digits and
    underscores), so nothing is suggested for a column named otherwise.
    Measured on Spark 3.5: the portable engine refuses such a rule when it
    runs, Deequ raises on a name with a dot in it, DQX fails every row of
    one, and only Great Expectations takes a dotted name.
    """
    try:
        ColumnName.parse(name)
    except IdentifierError:
        return False
    return True


def passed_over(profile: Profile) -> Tuple[str, ...]:
    """Return the columns no draft names, because no rule can ask for them."""
    return tuple(
        column.name for column in profile.columns if not addressable(column.name)
    )


def warranted(profile: Profile) -> Tuple[Suggestion, ...]:
    """Return what a profile warrants, in the order of its columns.

    Nothing is warranted of an empty dataset, nothing of a column no rule
    can name (:func:`addressable`), and no uniqueness of a column whose
    distinct count is missing or approximate.
    """
    if profile.rows == 0:
        return ()
    found = []
    for column in profile.columns:
        if not addressable(column.name):
            continue
        whole = column.nulls == 0
        if whole:
            found.append(Suggestion(column.name, COMPLETE))
        if (
            whole
            and column.distinct is not None
            and column.distinct_exact
            and column.distinct == profile.rows
        ):
            found.append(Suggestion(column.name, UNIQUE))
        if column.type in (INTEGER, DECIMAL, FLOAT):
            least = unspell(column.min, column.type)
            if least is not None and least >= 0:
                found.append(Suggestion(column.name, NON_NEGATIVE))
    return tuple(found)


def suggest(
    profile: Profile, target: str, *, snapshot_sha256: Optional[str] = None
) -> Dict[str, Any]:
    """Return a draft rule pack for ``target``, as a plain mapping.

    Args:
        profile: The profile to suggest from.
        target: One of :data:`TARGETS`.
        snapshot_sha256: The snapshot a portable rule certifies, where the
            profile does not carry one.

    Returns:
        ``{"dqframework": {"gate": {"fail_on": "error"}, "dqrules": [rule,
        ...]}}`` -- a rule named ``<dataset>_draft``, on the dataset of the
        profile, every check a warning, under the gate policy where a failed
        warning blocks nothing; no rule at all where the profile warrants
        none; and for the portable subset one rule per semantic version,
        completeness and non-negativity being separate.

    Raises:
        ConfigurationError: For a target that is not one of
            :data:`TARGETS`, or a portable draft with no snapshot digest
            to certify.
    """
    if target not in TARGETS:
        raise ConfigurationError(
            f"a draft is suggested for one of {list(TARGETS)}, got {target!r}"
        )
    by_name = {column.name: column for column in profile.columns}
    said = [_says(by_name, target, one) for one in warranted(profile)]
    checks = [check for check in said if check is not None]
    if target == "portable":
        return _pack(_portable_rules(profile, checks, snapshot_sha256))
    return _pack([_rule(profile, target, checks)] if checks else [])


def _pack(rules: list) -> Dict[str, Any]:
    """Return a draft as a rule pack: the gate policy it is written under, and its rules.

    Every rule is a warning, and a warning is advisory only under the gate
    policy ``error``: measured, the kernel's default policy, ``any``, blocks
    on a failed warning as on any other failure. A draft names the policy it
    was written for, so that it never gates a pipeline as it stands.
    """
    return {"dqframework": {"gate": {"fail_on": "error"}, "dqrules": rules}}


def unconfirmed(profile: Profile, target: str) -> Tuple[str, ...]:
    """Return what the profile warrants but ``target``'s own check would not
    confirm on this data, each as ``<column>_<kind>``: a draft leaves it out."""
    by_name = {column.name: column for column in profile.columns}
    return tuple(
        one.name
        for one in warranted(profile)
        if one.kind == COMPLETE and not _completeness_holds(target, by_name[one.column])
    )


def _completeness_holds(target: str, column: Column) -> bool:
    """Return whether ``target``'s own completeness check finds ``column`` complete.

    Measured on the data a profile is drawn from. The portable ``isComplete``
    counts a float that is not a number as a missing value, where the profile
    counts it a value; and Deequ's ``isComplete`` refuses a nested column --
    ``Unsupported nested column type`` -- and so fails it. A draft for either
    says nothing of the completeness of such a column.
    """
    if column.type == FLOAT and column.not_a_number:
        return target != "portable"
    if column.type == NESTED:
        return target != "deequ"
    return True


def _rule(profile: Profile, target: str, checks: list) -> Dict[str, Any]:
    """Return one rule holding ``checks`` in ``target``'s words."""
    return {
        "name": f"{profile.dataset}_draft",
        "engine": target,
        "dataframes": [profile.dataset],
        find_spec(target).payload_key: checks,
    }


def _portable_rules(
    profile: Profile, checks: list, snapshot_sha256: Optional[str]
) -> list:
    """Return the portable draft: one rule per semantic version.

    A plan cannot mix counts and ranges -- ``plan cannot mix metric semantic
    versions``, and only when it runs: dq-validate accepts the mixed rule --
    so completeness and non-negativity are separate rules.

    Raises:
        ConfigurationError: Where a rule is warranted and no snapshot
            digest says which one it certifies.
    """
    held = {
        kind: [check for check in checks if check["constraint"] == constraint]
        for kind, constraint in _PORTABLE.items()
    }
    if not any(held.values()):
        return []
    # A portable rule names the snapshot it certifies, and the Arrow
    # adapter, so that dq-run can run it over the files.
    digest = profile.snapshot_sha256 or snapshot_sha256
    if digest is None:
        raise ConfigurationError(
            "a portable rule names the snapshot it certifies, and neither the "
            "profile nor snapshot_sha256 says which: profile the files with "
            "dq-profile, or pass --snapshot-sha256"
        )
    several = sum(bool(said) for said in held.values()) > 1
    rules = []
    for kind, said in held.items():
        if not said:
            continue
        rule = _rule(profile, "portable", said)
        if several:
            rule["name"] = f"{profile.dataset}_draft_{kind}"
        rule.update(dataset=profile.dataset, snapshot_sha256=digest, adapter="arrow")
        rules.append(rule)
    return rules


def _says(
    by_name: dict, target: str, suggestion: Suggestion
) -> Optional[Dict[str, Any]]:
    """Return the suggestion in ``target``'s words, or ``None`` where it cannot
    be said: the target has no such check, or its own would not confirm it
    on the data the profile was drawn from."""
    column = by_name[suggestion.column]
    if suggestion.kind == COMPLETE and not _completeness_holds(target, column):
        return None
    return _check(target, suggestion)


def _check(target: str, suggestion: Suggestion) -> Optional[Dict[str, Any]]:
    """Return the suggestion in ``target``'s words, or ``None`` where it has no such check."""
    column, kind = suggestion.column, suggestion.kind
    if target == "portable":
        if kind not in _PORTABLE:
            return None  # the portable subset has no uniqueness check
        return {
            "constraint_name": suggestion.name,
            "constraint": _PORTABLE[kind],
            "column": column,
            "level": "Warning",
        }
    if target == "deequ":
        constraints = {
            COMPLETE: "isComplete",
            UNIQUE: "isUnique",
            NON_NEGATIVE: "isNonNegative",
        }
        return {
            "constraint_name": suggestion.name,
            "constraint": constraints[kind],
            "column": column,
            "level": "Warning",
        }
    if target == "dqx":
        function, arguments = {
            COMPLETE: ("is_not_null", {"column": column}),
            UNIQUE: ("is_unique", {"columns": [column]}),
            # Not is_in_range with one limit: DQX raises MissingParameterError
            # when a limit is left out -- at run time; its validator accepts it.
            NON_NEGATIVE: ("is_not_less_than", {"column": column, "limit": 0}),
        }[kind]
        return {
            "name": suggestion.name,
            "criticality": "warn",
            "check": {"function": function, "arguments": arguments},
        }
    type_, arguments = {
        COMPLETE: ("expect_column_values_to_not_be_null", {}),
        UNIQUE: ("expect_column_values_to_be_unique", {}),
        NON_NEGATIVE: ("expect_column_values_to_be_between", {"min_value": 0}),
    }[kind]
    return {
        "name": suggestion.name,
        "type": type_,
        "column": column,
        "criticality": "warn",
        **arguments,
    }


# ------------------------------------------------------------- the formats


def format_of(path) -> str:
    """Return the rule-pack format a path's extension names.

    Raises:
        ConfigurationError: For an extension that names none.
    """
    suffix = str(path).rsplit(".", 1)[-1].lower() if "." in str(path) else ""
    if suffix in ("conf", "hocon"):
        return "hocon"
    if suffix in ("yaml", "yml"):
        return "yaml"
    if suffix == "json":
        return "json"
    raise ConfigurationError(
        f"a draft is written as .conf, .hocon, .yaml, .yml or .json; {str(path)!r} "
        f"names no format (pass --format)"
    )


def render(pack: Dict[str, Any], fmt: str) -> str:
    """Return a rule pack as text in ``fmt``.

    Raises:
        ConfigurationError: For a format that is not one of :data:`FORMATS`.
    """
    if fmt == "hocon":
        return "\n".join(_lines(pack, 0)) + "\n"
    if fmt == "yaml":
        return yaml.safe_dump(pack, sort_keys=False, default_flow_style=False)
    if fmt == "json":
        return json.dumps(pack, indent=2) + "\n"
    raise ConfigurationError(
        f"a draft is written as one of {list(FORMATS)}, got {fmt!r}"
    )


def _lines(value: Any, depth: int) -> list:
    """Return ``value`` as lines of HOCON, each already indented.

    Every string is quoted as JSON quotes it, which HOCON reads as it is,
    and a key is quoted unless it is a plain identifier. An object with
    nothing composite in it is written on one line, as the examples are.
    """
    pad = "  " * depth
    if isinstance(value, dict):
        if not value:
            return [pad + "{}"]
        if all(_on_a_line(item) for item in value.values()):
            said = ", ".join(
                f"{_key(key)} = {_lines(item, 0)[0].strip()}"
                for key, item in value.items()
            )
            return [pad + "{ " + said + " }"]
        lines = [pad + "{"]
        for key, item in value.items():
            lines += _entry(_key(key), item, depth + 1)
        return lines + [pad + "}"]
    if isinstance(value, list):
        if not value:
            return [pad + "[]"]
        if all(_on_a_line(item) for item in value):
            return [pad + "[" + ", ".join(_scalar(item) for item in value) + "]"]
        lines = [pad + "["]
        for item in value:
            lines += _lines(item, depth + 1)
        return lines + [pad + "]"]
    return [pad + _scalar(value)]


def _entry(key: str, value: Any, depth: int) -> list:
    """Return a key's line and its value's, an object opening on its key's line."""
    written = _lines(value, depth)
    if isinstance(value, dict) and value:
        return [f"{'  ' * depth}{key} {written[0].lstrip()}"] + written[1:]
    return [f"{'  ' * depth}{key} = {written[0].lstrip()}"] + written[1:]


def _on_a_line(value: Any) -> bool:
    """Return whether ``value`` is a scalar or a list of them, which fits on a line."""
    return not isinstance(value, dict) and (
        not isinstance(value, list) or all(not _composite(item) for item in value)
    )


def _composite(value: Any) -> bool:
    return isinstance(value, (dict, list))


def _scalar(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value)
    return repr(value)


def _key(key: Any) -> str:
    if isinstance(key, str) and _PLAIN_KEY.match(key):
        return key
    return json.dumps(key)
