# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Engine-neutral rule identity and severity normalization: the check seam.

Every wrapped engine (Deequ, GX, DQX, ...) expresses the same three axes
differently -- per-rule identity, per-rule severity, and row scoping -- and
the kernel wraps them behind one plugin boundary. This module is the single
fail-closed place where those common axes are normalized, so rule packs
author one vocabulary regardless of engine and evidence consumers read one
vocabulary regardless of producer.

Deliberately NOT unified here: the per-engine check-function vocabularies
(GX expectation types, Deequ constraint methods, DQX check functions). They
carry genuinely different semantics (threshold models, null handling,
result details), so a universal catalog would couple the kernel to the
union of three engines' quirks and make every future engine a whole-catalog
port. Engines pass their native payloads through untouched; only the axes
that are truly common cross the seam.

A fourth axis crosses it too (SPEC-025): a rule asks for a column by a
name that is a plain identifier. Where each engine's payload names a
column is the engine's declaration, ``EngineSpec.column_keys``;
:func:`column_name_problems` reads a rule by it, for whoever validates a
rule pack and for whoever loads an engine.
"""

from collections.abc import Mapping
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from dq.identifiers import ColumnName, IdentifierError

#: Canonical severity spellings accepted across all engines.
CRITICALITIES = ("error", "warn")

#: Accepted spellings mapped to canonical form: DQX ("error"/"warn"),
#: Deequ levels ("Error"/"Warning"), and common short forms.
_CRITICALITY_ALIASES = {
    "error": "error",
    "err": "error",
    "warn": "warn",
    "warning": "warn",
}

_DEFAULT_CRITICALITY = "error"


def normalize_criticality(value: Any, default: str = _DEFAULT_CRITICALITY) -> str:
    """Normalize an engine's severity spelling to the canonical form.

    Accepts the DQX spellings (``error``/``warn``), Deequ level spellings
    (``Error``/``Warning``), and common short forms, case-insensitively.

    Args:
        value: Raw severity from configuration or engine output; ``None``
            yields ``default``.
        default: Value used when ``value`` is ``None`` or empty.

    Returns:
        One of :data:`CRITICALITIES`.

    Raises:
        ValueError: If the value is not a recognized severity spelling.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    key = str(value).strip().lower()
    criticality = _CRITICALITY_ALIASES.get(key)
    if criticality is None:
        raise ValueError(
            f"Unknown criticality {value!r}: expected one of "
            f"{sorted(_CRITICALITY_ALIASES)}"
        )
    return criticality


def derive_name(function: str, index: int) -> str:
    """Derive a deterministic rule name when configuration omits one.

    ``<function>_<index>`` is stable across runs (index is the rule's
    position in its payload) and collision-free, so results map
    unambiguously back to configured rules without relying on any
    engine's private auto-naming.

    Args:
        function: The engine-native check identifier.
        index: Zero-based position of the rule in its payload.

    Returns:
        The derived name.
    """
    return f"{function}_{index}"


def build_result(
    check: Any,
    success: Any,
    details: Optional[Dict[str, Any]] = None,
    *,
    criticality: Optional[str] = None,
) -> Dict[str, Any]:
    """Construct one legacy outcome dict ({``check``, ``success``, ``details``}).

    The single shared shape constructor for every engine and harvester:
    result dicts are always built through this function so the envelope
    shape has one definition. Deliberately permissive -- the outcome
    boundary (:mod:`dq.outcomes`) owns the strict structural gates, so
    duplicating them here would double-raise.

    Args:
        check: Rule identity as reported in results.
        success: Truthy-coerced to ``bool``.
        details: Caller-owned details mapping; copied (never mutated)
            when ``criticality`` is merged.
        criticality: Optional severity spelling, normalized fail-closed
            via :func:`normalize_criticality` and merged into ``details``.

    Returns:
        ``{"check": ..., "success": bool, "details": dict}``.
    """
    if criticality is not None:
        merged = dict(details or {})
        merged["criticality"] = normalize_criticality(criticality)
    else:
        merged = details if details is not None else {}
    return {"check": check, "success": bool(success), "details": merged}


def require_unique_labels(labels: Iterable[str]) -> None:
    """Fail closed when two checks would report the same label.

    Evidence is attributed by a result's ``check`` label. Two checks with
    one label cannot be told apart -- in a report, a sink, or a gate event
    that names failing checks -- so an engine refuses such a configuration
    and says which checks collide.

    Args:
        labels: The labels of a rule's checks, in configuration order.

    Raises:
        ValueError: Naming the label and the two checks that share it.
    """
    seen: Dict[str, int] = {}
    for index, label in enumerate(labels):
        if label in seen:
            raise ValueError(
                f"Duplicate check label {label!r}: checks {seen[label]} and "
                f"{index} would report the same name and their evidence could "
                "not be told apart; give each check a distinct name"
            )
        seen[label] = index


def column_name_problems(
    spec: Any, definition: Mapping[str, Any], where: str = ""
) -> List[str]:
    """Return every way a rule names a column by anything but a plain identifier.

    Spark reads a name it is handed as an expression: ``a.b`` is the field
    ``b`` of a struct ``a`` where the frame has one, ``x-y`` is a
    subtraction, and a backtick is a quote. Measured on Spark 3.5, a DQX
    check and a business-rules check on a column called ``s.t`` beside a
    struct ``s`` answered "passed" over data where that column held a
    null (SPEC-025). So a rule asks for a column by a plain identifier, as
    :class:`dq.identifiers.ColumnName` reads one, and this is where the
    kernel refuses any other name: in ``dq-validate``, and when an engine
    is loaded, before the engine is imported.

    Where the payload names a column is the engine's declaration,
    ``spec.column_keys``. A test reads each wrapper -- and DQX's own
    registry -- and holds the declaration to it.

    Args:
        spec: The engine's declaration.
        definition: The rule. The engine's payload is under
            ``spec.payload_key``.
        where: A prefix for each problem, naming the rule.

    Returns:
        Problems as readable strings: empty when every name is plain, when
        the engine declares no column keys, or when there is no payload to
        read -- which another check says.
    """
    if not spec.column_keys or not isinstance(definition, Mapping):
        return []
    payload = definition.get(spec.payload_key, None)
    if payload is None or spec.payload_type == "string":
        return []
    problems: List[str] = []
    entries = payload if isinstance(payload, (list, tuple)) else [payload]
    for number, entry in enumerate(entries, start=1):
        for path in spec.column_keys:
            for name in _names_at(entry, tuple(path.split("."))):
                try:
                    ColumnName.parse(name)
                except IdentifierError as error:
                    problems.append(
                        f"{where}check {number}: {path} = {name!r}: {error} -- and "
                        "Spark reads any other string as an expression, not as a name"
                    )
    return problems


def _names_at(value: Any, segments: Tuple[str, ...]) -> Iterator[str]:
    """Yield every name at a path into ``value``.

    What a path names is one name, a list of names, or a list of those: a
    list of pairs, or a list of mappings each holding the key. Anything
    else is not a name, and is left to the engine to judge.
    """
    if not segments:
        if isinstance(value, str):
            yield value
        elif isinstance(value, (list, tuple)):
            for item in value:
                yield from _names_at(item, ())
        return
    if isinstance(value, Mapping):
        if segments[0] in value:
            yield from _names_at(value[segments[0]], segments[1:])
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _names_at(item, segments)


def _explicit_name(check: Mapping[str, Any], name_keys) -> Optional[str]:
    for key in name_keys:
        value = check.get(key, None)
        if value:
            return str(value)
    return None


def _target_columns(check: Mapping[str, Any], column_keys) -> List[str]:
    columns: List[str] = []
    for key in column_keys:
        value = check.get(key, None)
        if isinstance(value, str):
            columns.append(value)
        elif value:
            columns.extend(str(column) for column in value)
    return columns


def labels_for_checks(
    checks: Iterable[Mapping[str, Any]], name_keys, kind_key: str, column_keys
) -> List[str]:
    """Return the labels of a rule's checks, in configuration order.

    The labelling rule every engine follows. A check is named by:

    * its configured name (the first of ``name_keys`` that is set);
    * else its kind and the columns it targets, ``isComplete_id``;
    * else its kind alone -- a check with no columns, such as a row count
      or a table-level expectation -- unless the rule has an earlier check
      of the same kind, in which case by its kind and position.

    Args:
        checks: The rule's check mappings.
        name_keys: Keys that hold an explicit name, in precedence order.
        kind_key: The key holding the check's kind.
        column_keys: Keys holding the columns the check targets.

    Returns:
        One label per check. Two may still coincide; the caller refuses
        such a rule with :func:`require_unique_labels`.
    """
    labels: List[str] = []
    for check in checks:
        explicit = _explicit_name(check, name_keys)
        if explicit is not None:
            labels.append(explicit)
            continue
        kind = str(check.get(kind_key, None) or "check")
        columns = _target_columns(check, column_keys)
        labels.append(f"{kind}_{'_'.join(columns)}" if columns else kind)

    bare_kinds_seen: Dict[str, int] = {}
    for index, (check, label) in enumerate(zip(checks, labels)):
        if _explicit_name(check, name_keys) is not None or _target_columns(
            check, column_keys
        ):
            continue
        if label in bare_kinds_seen:
            labels[index] = derive_name(label, index)
        else:
            bare_kinds_seen[label] = index
    return labels
