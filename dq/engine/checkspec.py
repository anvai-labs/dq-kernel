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
"""

from collections.abc import Mapping
from typing import Any, Dict, Iterable, List, Optional

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
