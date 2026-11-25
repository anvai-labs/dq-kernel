# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the DQEngine plugin contract (ADR-012).

Enforces the template lifecycle (guard -> log -> prepare -> execute ->
persist), the capability refusal, and that built-in engines implement
hooks rather than overriding ``apply``.
"""

import importlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pyhocon import ConfigFactory

from dq.engine.dq_engine import DQEngine
from dq.exceptions import ConfigurationError

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ENGINE_ROOT = REPOSITORY_ROOT / "dq" / "engine"


class RecordingEngine(DQEngine):
    """Minimal hook-contract engine: records the template's call order."""

    def __init__(self):
        config = ConfigFactory.from_dict({"name": "recording", "engine": "recording"})
        super().__init__(config)
        self.calls = []
        self.metrics = [{"check": "r", "success": True, "details": {}}]

    def _prepare(self, dataframe):
        self.calls.append("prepare")
        return "prepared"

    def _execute(self, dataframe, prepared):
        self.calls.append(f"execute:{prepared}")
        return self.metrics

    def _persist_outcomes(self, repository, metrics, dataframe):
        self.calls.append(f"persist:{repository is not None}")


class _Frame:
    sparkSession = None


def test_template_runs_guard_prepare_execute_persist_in_order():
    engine = RecordingEngine()
    results = engine.apply(_Frame(), repository="sink")
    assert results == engine.metrics
    assert engine.calls == ["prepare", "execute:prepared", "persist:True"]


def test_template_passes_repository_through_to_the_persist_hook():
    engine = RecordingEngine()
    engine.apply(_Frame())
    assert engine.calls == ["prepare", "execute:prepared", "persist:False"]
    engine.apply(_Frame(), repository="sink")
    assert engine.calls[-1] == "persist:True"


def test_capability_guard_refuses_persistence_before_any_work():
    class NonPersisting(DQEngine):
        CAPABILITIES = {"persistence": False}

        def __init__(self):
            super().__init__(ConfigFactory.from_dict({"name": "np"}))
            self.calls = []

        def _execute(self, dataframe, prepared):
            self.calls.append("execute")
            return []

    engine = NonPersisting()
    with pytest.raises(ConfigurationError, match="persistence=False"):
        engine.apply(_Frame(), repository="sink")
    assert engine.calls == []  # refused before prepare/execute


def test_engines_may_not_override_apply():
    """Built-in engines implement hooks; apply overrides are deprecated."""
    offenders = []
    for engine_dir in sorted(ENGINE_ROOT.iterdir()):
        if not engine_dir.is_dir():
            continue
        for path in engine_dir.rglob("*_engine.py"):
            source = path.read_text(encoding="utf-8")
            if "def apply(" in source:
                offenders.append(path.relative_to(REPOSITORY_ROOT).as_posix())
    assert offenders == [], (
        f"engines overriding DQEngine.apply: {offenders}; implement "
        "_prepare/_execute hooks instead (ADR-012)"
    )


def test_apply_override_still_functions_with_deprecation_warning():
    """External legacy engines keep working through MRO during migration.

    Legacy engines overriding ``apply`` must also provide a trivial
    ``_execute`` (the abstract hook) -- the override itself takes
    precedence over the template via MRO.
    """
    with pytest.warns(DeprecationWarning, match="overrides DQEngine.apply"):

        class LegacyEngine(DQEngine):
            def apply(self, dataframe, repository=None):
                return [{"check": "legacy", "success": True, "details": {}}]

            def _execute(self, dataframe, prepared):
                return []  # unused while apply is overridden

    assert LegacyEngine(ConfigFactory.from_dict({"name": "l"})).apply(_Frame()) == [
        {"check": "legacy", "success": True, "details": {}}
    ]


def test_template_logs_processing_start(caplog):
    import logging as logging_module

    engine = RecordingEngine()
    with caplog.at_level(logging_module.INFO):
        engine.apply(_Frame())
    start_lines = [
        r.getMessage()
        for r in caplog.records
        if r.getMessage() == "Processing recording with recording Engine"
    ]
    assert start_lines, "template must emit the shared log preamble"


def test_default_persist_outcomes_uses_shared_artifact_frames():
    class DefaultPersisting(DQEngine):
        def __init__(self):
            super().__init__(ConfigFactory.from_dict({"name": "dp"}))
            self.metrics = [{"check": "r", "success": True, "details": {}}]

        def _execute(self, dataframe, prepared):
            return self.metrics

    engine = DefaultPersisting()
    engine._persist = MagicMock()
    frame = MagicMock()
    engine._persist_outcomes("sink", engine.metrics, frame)
    assert engine._persist.call_count == 1
    args = engine._persist.call_args[0]
    assert args[0] == "sink" and len(args) == 3
    engine._persist.reset_mock()
    engine._persist_outcomes(None, engine.metrics, frame)
    assert engine._persist.call_count == 0  # falsy repository skips work


# ------------------------------------------------------- capability manifest


def _engine_with(manifest):
    class Engine(DQEngine):
        CAPABILITIES = manifest

        def _execute(self, dataframe, prepared):
            return []

    return Engine


def test_a_manifest_states_only_what_differs_from_the_defaults():
    Engine = _engine_with({"criticality": "native"})
    assert Engine.CAPABILITIES == {
        "criticality": "native",
        "row_filter": False,
        "failed_rows": False,
        "persistence": True,
    }


def test_a_subclass_inherits_its_parent_s_manifest_not_the_defaults():
    """A second-level subclass must not silently reset what its parent said."""
    Parent = _engine_with({"persistence": False, "criticality": "native"})
    Child = type("Child", (Parent,), {"CAPABILITIES": {"row_filter": True}})
    Grandchild = type("Grandchild", (Child,), {})

    def body(cls):
        def _execute(self, dataframe, prepared):
            return []

        return {"_execute": _execute}

    Child.__init_subclass__  # already initialized through the hook
    Grandchild  # likewise
    assert Parent.CAPABILITIES["persistence"] is False
    assert Child.CAPABILITIES == {**Parent.CAPABILITIES, "row_filter": True}
    assert (
        Grandchild.CAPABILITIES == Child.CAPABILITIES
    ), "a silent subclass inherits its parent"


@pytest.mark.parametrize(
    ("manifest", "message"),
    [
        ({"criticality": "sometimes"}, "criticality must be one of"),
        ({"persistence": "yes"}, "persistence must be a boolean, got 'yes'"),
        ({"row_filter": None}, "row_filter must be a boolean, got None"),
        ({"teleport": True}, "unknown capability 'teleport'"),
        ("native", "CAPABILITIES must be a mapping"),
        (None, "CAPABILITIES must be a mapping"),
    ],
)
def test_an_invalid_manifest_fails_at_the_definition(manifest, message):
    with pytest.raises(ConfigurationError, match=message) as caught:
        _engine_with(manifest)
    assert type(caught.value).__name__ == "ConfigurationError"
    assert "declares an invalid CAPABILITIES manifest" in str(caught.value)


def test_the_built_in_manifests_are_all_valid():
    from dq.engine.dq_engine import CAPABILITY_KEYS, capability_problems
    from dq.engine.registry import BUILT_IN_ENGINES

    assert CAPABILITY_KEYS == (
        "criticality",
        "row_filter",
        "failed_rows",
        "persistence",
    )
    for name, spec in BUILT_IN_ENGINES.items():
        try:
            module = importlib.import_module(spec.module)
        except ImportError:  # an extra that is not installed
            pytest.skip(f"{name} needs an optional dependency")
        assert capability_problems(getattr(module, spec.class_name).CAPABILITIES) == []
