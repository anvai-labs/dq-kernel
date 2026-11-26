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

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Dict, Iterable, Optional

logger = logging.getLogger(__name__)

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


def require_unique_names(names: Iterable[str]) -> None:
    """Fail closed when explicitly configured rule names repeat.

    Duplicate names would make evidence ambiguous (two rules, one label);
    derived names are exempt -- callers validate only names the author
    wrote, so legacy payloads with repeated engine-native labels keep
    working.

    Args:
        names: Explicitly configured names.

    Raises:
        ValueError: On the first duplicate.
    """
    seen = set()
    for name in names:
        if name in seen:
            raise ValueError(
                f"Duplicate check name {name!r}: explicitly configured "
                "check names must be unique so evidence maps unambiguously "
                "to rules"
            )
        seen.add(name)


@dataclass(frozen=True)
class CheckSpec:
    """One rule as seen across the plugin seam.

    Attributes:
        name: Rule identity (explicit or :func:`derive_name`).
        criticality: Canonical severity (:data:`CRITICALITIES`).
        function: Engine-native check identifier (passthrough -- the
            engine's vocabulary is not interpreted here).
        native: The engine-native rule definition, frozen for safety.
            Engines build their runtime objects from this.
        columns: Target columns, informational (engines resolve targets
            from ``native`` themselves).
        filter: Optional row-scope expression; only meaningful for
            engines whose capabilities include ``row_filter``.
    """

    name: str
    criticality: str
    function: str
    native: Mapping[str, Any] = field(default_factory=dict)
    columns: tuple = ()
    filter: Optional[str] = None

    def __post_init__(self):
        if self.criticality not in CRITICALITIES:
            raise ValueError(
                f"CheckSpec criticality must be one of {CRITICALITIES}, "
                f"got {self.criticality!r}; normalize with "
                "normalize_criticality() first"
            )
        object.__setattr__(self, "native", MappingProxyType(dict(self.native)))

    def as_detail(self) -> Dict[str, Any]:
        """Severity fragment engines embed in metric ``details``.

        Returns:
            ``{"criticality": ...}`` plus the target columns when known.
        """
        detail: Dict[str, Any] = {"criticality": self.criticality}
        if self.columns:
            detail["columns"] = list(self.columns)
        return detail
