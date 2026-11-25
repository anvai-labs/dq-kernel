# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The "Writing an engine" guide's example is executed, not just printed.

The engine is read out of the guide's own code block, so the page cannot
drift from the contract it teaches.
"""

import re
from pathlib import Path

import pytest
from pyhocon import ConfigFactory

from dq.engine.registry import BUILT_IN_ENGINES, EngineSpec
from dq.engine.engine_loader import EngineLoader
from dq.sdk.conformance import assert_engine_conforms
from dq.sinks import InMemorySink
from dq.validate import validate_config_document

pytestmark = pytest.mark.spark

GUIDE = Path(__file__).resolve().parents[2] / "docs" / "writing-an-engine.adoc"

CHECKS = [
    {"name": "names_not_empty", "column": "name", "level": "Error"},
    {"name": "cities_not_empty", "column": "city", "level": "Warning"},
]


def engine_from_the_guide():
    """Return the engine class defined in the guide's "complete engine" block."""
    text = GUIDE.read_text(encoding="utf-8")
    section = text.split("== A complete engine", 1)[1]
    block = re.search(r"\[source,python\]\n----\n(.*?)\n----", section, flags=re.S)
    assert block, "the guide has no code block under 'A complete engine'"
    namespace = {}
    exec(compile(block.group(1), str(GUIDE), "exec"), namespace)  # noqa: S102
    from dq.engine.dq_engine import DQEngine

    # The block imports the base class too; the example is its subclass.
    return next(
        value
        for key, value in namespace.items()
        if isinstance(value, type) and key.endswith("Engine") and value is not DQEngine
    )


@pytest.fixture
def registered(monkeypatch):
    engine_class = engine_from_the_guide()
    # The block was executed here, so publish the class under this module's
    # name for the loader, which imports the module named by the spec.
    engine_class.__module__ = __name__
    globals()[engine_class.__name__] = engine_class
    spec = EngineSpec(
        "nonempty", engine_class.__module__, engine_class.__name__, "checks"
    )
    monkeypatch.setitem(BUILT_IN_ENGINES, "nonempty", spec)
    return engine_class


def test_the_guide_s_example_engine_runs_through_the_framework(spark, registered):
    frame = spark.createDataFrame(
        [("ann", ""), ("", "Oslo"), ("cy", "Tromso")], "name string, city string"
    )
    config = ConfigFactory.from_dict(
        {"name": "nonempty", "engine": "nonempty", "checks": CHECKS}
    )
    outcomes = {
        o["check"]: o
        for o in EngineLoader().load_engine("nonempty", config, 1).apply(frame)
    }
    assert {label: o["success"] for label, o in outcomes.items()} == {
        "names_not_empty": False,
        "cities_not_empty": False,
    }
    assert outcomes["names_not_empty"]["details"] == {
        "failed_rows": 1,
        "criticality": "error",
    }
    assert outcomes["cities_not_empty"]["details"]["criticality"] == "warn"


def test_the_guide_s_example_engine_conforms(spark, registered):
    engine_class = registered
    frame = spark.createDataFrame(
        [("ann", ""), ("", "Oslo")], "name string, city string"
    )
    config = ConfigFactory.from_dict(
        {"name": "nonempty", "engine": "nonempty", "checks": CHECKS}
    )
    report = assert_engine_conforms(
        lambda dqts=None: engine_class(config, dqts=dqts), frame, InMemorySink
    )
    assert "artifacts_carry_the_injected_run_key" in report.checks


def test_the_guide_s_example_engine_validates_like_a_built_in(tmp_path, registered):
    path = tmp_path / "rules.conf"
    path.write_text(
        "dqframework { dqrules = [{ engine = nonempty, checks = ["
        "{ name = a, column = name }, { name = b, column = city }"
        "] }] }\n",
        encoding="utf-8",
    )
    assert validate_config_document(str(path)).valid
    missing = tmp_path / "missing.conf"
    missing.write_text(
        "dqframework { dqrules = [{ engine = nonempty }] }\n", encoding="utf-8"
    )
    assert "non-empty 'checks' payload" in " ".join(
        validate_config_document(str(missing)).errors
    )


def test_the_guide_s_engine_refuses_a_name_that_collides_with_another_label(
    spark, registered
):
    """`name` is the label the second check derives; naming the first one
    that would make the two results indistinguishable."""
    engine_class = registered
    config = ConfigFactory.from_dict(
        {
            "name": "twins",
            "engine": "nonempty",
            "checks": [{"name": "name", "column": "city"}, {"column": "name"}],
        }
    )
    with pytest.raises(
        ValueError, match="Duplicate check label 'name': checks 0 and 1"
    ):
        engine_class(config)._prepare(
            spark.createDataFrame([("a", "b")], "name string, city string")
        )


def test_the_guide_documents_every_capability_key_and_hook():
    from dq.engine.dq_engine import CAPABILITY_KEYS, DQEngine

    text = GUIDE.read_text(encoding="utf-8")
    for key in CAPABILITY_KEYS:
        assert f"`{key}`" in text, f"the guide does not document the {key!r} capability"
    for hook in ("_prepare", "_execute", "_persist_outcomes"):
        assert hook in text, f"the guide does not mention {hook!r}"
