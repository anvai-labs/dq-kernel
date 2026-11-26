# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the declarative engine registry.

The drift guard is the load-bearing one: every engine directory carrying a
``{name}_engine.py`` must have a registry entry, so the historical trap of
parallel-editing loader convention and validate tables cannot recur.
"""

import types
from pathlib import Path

import pytest

from dq.engine import registry
from dq.engine.registry import (
    BUILT_IN_ENGINES,
    EngineSpec,
    ENTRY_POINT_GROUP,
    find_entry_point,
    find_spec,
    payload_key_for,
    payload_type_for,
    renamed_target,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ENGINE_ROOT = REPOSITORY_ROOT / "dq" / "engine"


def test_every_engine_directory_is_registered():
    """Drift guard: engine dirs with {name}_engine.py must be in the registry."""
    unregistered = []
    for engine_dir in sorted(ENGINE_ROOT.iterdir()):
        if not engine_dir.is_dir():
            continue
        name = engine_dir.name
        if (engine_dir / f"{name}_engine.py").exists() and name not in BUILT_IN_ENGINES:
            unregistered.append(name)
    assert unregistered == [], (
        f"engine directories without a registry entry: {unregistered}; "
        "add an EngineSpec to dq/engine/registry.py"
    )


def test_built_in_registry_covers_the_known_engines():
    assert set(BUILT_IN_ENGINES) == {
        "businessrules",
        "deequ",
        "dqdl",
        "dqx",
        "greatexpectations",
        "portable",
        "schemavalidation",
    }


def test_payload_keys_match_declared_engines():
    assert payload_key_for("deequ") == "checks"
    assert payload_key_for("dqdl") == "ruleset"
    assert payload_key_for("greatexpectations") == "expectations"
    assert payload_key_for("schemavalidation") == "schema"
    assert payload_key_for("businessrules") == "checks"
    assert payload_key_for("dqx") == "checks"
    assert payload_key_for("not_an_engine") is None


def test_payload_types_match_declared_engines():
    assert payload_type_for("dqdl") == "string"
    assert payload_type_for("deequ") == "list"
    assert payload_type_for("unknown") == "list"


def test_renamed_target_maps_deprecated_aliases():
    assert renamed_target("custom") == "businessrules"
    assert renamed_target("Custom") == "businessrules"
    assert renamed_target("deequ") is None


def test_find_spec_is_case_insensitive():
    spec = find_spec("DeeQu")
    assert spec is not None and spec.name == "deequ"


def test_entry_point_resolution(monkeypatch):
    soda_ep = types.SimpleNamespace(value="soda_dq.kernel:SodaEngine", name="soda")
    monkeypatch.setattr(
        registry.importlib.metadata,
        "entry_points",
        lambda: types.SimpleNamespace(
            select=lambda group: [soda_ep] if group == ENTRY_POINT_GROUP else []
        ),
    )
    assert find_entry_point("soda") == ("soda_dq.kernel", "SodaEngine")
    assert find_entry_point("unregistered") is None


def test_entry_point_shorthand_value_form(monkeypatch):
    # Some distributions register "module.attr" without the colon.
    entries = [types.SimpleNamespace(name="soda", value="soda_dq.kernel.SodaEngine")]
    monkeypatch.setattr(
        registry.importlib.metadata,
        "entry_points",
        lambda: types.SimpleNamespace(select=lambda group: entries),
    )
    assert find_entry_point("soda") == ("soda_dq.kernel", "SodaEngine")


def test_engine_spec_is_frozen():
    spec = BUILT_IN_ENGINES["deequ"]
    with pytest.raises(Exception):
        spec.name = "other"
