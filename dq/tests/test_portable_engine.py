# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""End-to-end tests for the portable engine through the plugin path.

The first tests that execute the kernel's portable plans inside
``DQFramework.run()`` -- closing the "parallel universes" gap (ADR-010)
-- plus fail-closed payload validation and a differential case against
the deequ engine for the shared ``isComplete`` constraint.
"""

import pytest
from pyhocon import ConfigFactory

from dq.exceptions import ConfigurationError

pytestmark = pytest.mark.spark

_SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def portable_config(plan=None, dataset="orders", sha=_SHA, **extra):
    """A portable rule; ``extra`` adds keys to its payload, such as ``adapter``."""
    return ConfigFactory.from_dict(
        {
            "name": "portable-test",
            "engine": "portable",
            "dataset": dataset,
            "snapshot_sha256": sha,
            "plan": (
                plan
                if plan is not None
                else [
                    {
                        "constraint": "isComplete",
                        "constraint_name": "id_complete",
                        "column": "id",
                        "level": "Error",
                    }
                ]
            ),
            **extra,
        }
    )


@pytest.fixture
def sample_frame(spark):
    return spark.createDataFrame([("a",), ("b",), ("c",)], ["id"])


def test_portable_engine_reports_success_through_hooks(sample_frame):
    from dq.engine.portable.portable_engine import PortableEngine

    results = PortableEngine(portable_config()).apply(sample_frame)
    assert results
    assert all(result["success"] is True for result in results)
    assert results[0]["details"].get("rule_id"), "portable outcomes keep rule identity"


def test_portable_engine_reports_incomplete_column(sample_frame):
    from dq.engine.portable.portable_engine import PortableEngine

    nullable = sample_frame.sparkSession.createDataFrame(
        [("a",), (None,), ("c",)], ["id"]
    )
    results = PortableEngine(portable_config()).apply(nullable)
    assert any(result["success"] is False for result in results)
    assert all(result["details"].get("rule_id") == "id_complete" for result in results)


def test_portable_engine_requires_dataset_and_snapshot(sample_frame):
    from dq.engine.portable.portable_engine import PortableEngine

    config = ConfigFactory.from_dict(
        {
            "name": "portable-missing",
            "engine": "portable",
            "plan": [{"constraint": "isComplete", "column": "id"}],
        }
    )
    with pytest.raises(Exception, match="snapshot_sha256"):
        PortableEngine(config).apply(sample_frame)


def test_portable_engine_requires_non_empty_plan(sample_frame):
    from dq.engine.portable.portable_engine import PortableEngine

    config = portable_config(plan=[])
    with pytest.raises(Exception, match="non-empty 'plan'"):
        PortableEngine(config).apply(sample_frame)


def test_portable_engine_fails_closed_on_untranslatable_constraint(sample_frame):
    from dq.engine.portable.portable_engine import PortableEngine

    config = portable_config(
        plan=[
            {
                "constraint": "hasCompletelyRandomValues",
                "constraint_name": "random",
                "column": "id",
            }
        ]
    )
    with pytest.raises(Exception):
        PortableEngine(config).apply(sample_frame)


def test_portable_engine_matches_deequ_for_iscomplete(spark):
    """Differential: same isComplete rule through both execution paths."""
    from dq.engine.deequ.deequ_engine import DeequEngine
    from dq.engine.portable.portable_engine import PortableEngine

    frame = spark.createDataFrame([("a",), (None,), ("c",)], ["id"])
    portable = PortableEngine(portable_config()).apply(frame)
    deequ_config = ConfigFactory.from_dict(
        {
            "name": "deequ-differential",
            "checks": [{"constraint": "isComplete", "column": "id", "level": "Error"}],
        }
    )
    deequ = DeequEngine(deequ_config).apply(frame)
    portable_verdict = any(result["success"] is False for result in portable)
    deequ_verdict = any(result["success"] is False for result in deequ)
    assert portable_verdict is True and deequ_verdict is True


# ------------------------------------------------- which compute runs it


def verdicts(results):
    return [(result["check"], result["success"]) for result in results]


def test_a_portable_rule_runs_on_spark_unless_it_names_an_adapter(sample_frame):
    from dq.engine.portable.portable_engine import PortableEngine

    results = PortableEngine(portable_config()).apply(sample_frame)
    assert results[0]["details"]["adapter"] == {"name": "spark", "version": "1"}
    assert (
        results[0]["details"]["adapter"]
        == PortableEngine(portable_config(adapter="spark")).apply(sample_frame)[0][
            "details"
        ]["adapter"]
    )


def test_a_portable_rule_names_the_arrow_adapter_and_decides_the_same(sample_frame):
    pytest.importorskip("pyarrow", reason="the Arrow adapter needs the arrow extra")
    """The same Spark frame, read through Spark's Arrow export: the decisions
    are the kernel's, whichever adapter computed the values."""
    from dq.engine.portable.portable_engine import PortableEngine

    nullable = sample_frame.sparkSession.createDataFrame(
        [("a",), (None,), ("c",)], ["id"]
    )
    on_arrow = PortableEngine(portable_config(adapter="arrow")).apply(nullable)
    on_spark = PortableEngine(portable_config()).apply(nullable)
    assert on_arrow[0]["details"]["adapter"] == {"name": "arrow", "version": "1"}
    assert verdicts(on_arrow) == verdicts(on_spark) == [("id_complete", False)]
    assert on_arrow[0]["details"]["observed"] == on_spark[0]["details"]["observed"]


class Unread:
    """A frame that must not be looked at."""

    def __getattr__(self, name):
        raise AssertionError(f"the frame's {name} was read")


@pytest.mark.parametrize("adapter", ["Arrow", "arrow2", "", None, 7])
def test_an_adapter_that_does_not_exist_is_refused_before_the_frame_is_read(adapter):
    from dq.engine.portable.portable_engine import PortableEngine

    engine = PortableEngine(portable_config(adapter=adapter))
    with pytest.raises(ConfigurationError) as raised:
        engine.apply(Unread())
    assert f"adapter {adapter!r} is not a portable adapter" in str(raised.value)
    assert "['arrow', 'spark', 'sql']" in str(raised.value)


def test_the_refusal_names_the_adapter_after_the_payload_is_checked(sample_frame):
    """A rule with no plan is refused for that, whichever adapter it names."""
    from dq.engine.portable.portable_engine import PortableEngine

    with pytest.raises(ConfigurationError, match="non-empty 'plan'"):
        PortableEngine(portable_config(plan=[], adapter="no-such-adapter")).apply(
            Unread()
        )


def test_a_portable_rule_on_the_arrow_adapter_runs_through_dqframework_run(spark):
    pytest.importorskip("pyarrow", reason="the Arrow adapter needs the arrow extra")
    from dq.dq_framework import DQFramework

    config = ConfigFactory.parse_string(f"""
        dqframework {{
          dataframes {{ orders = "orders" }}
          dqrules = [
            {{
              name = "portable_on_arrow"
              engine = "portable"
              dataframes = ["orders"]
              dataset = "orders"
              snapshot_sha256 = "{_SHA}"
              adapter = "arrow"
              plan = [
                {{ constraint = "isComplete", constraint_name = "id_complete", column = "id", level = "Error" }}
              ]
            }}
          ]
        }}
        """)
    framework = DQFramework.__new__(DQFramework)
    framework._config = config
    framework._spark = spark
    framework.get_dataframe = lambda name: spark.createDataFrame(
        [("a",), (None,), ("c",)], ["id"]
    )
    results = framework.run()
    assert results, "the rule executed through the standard run path"
    assert results[0]["details"]["adapter"] == {"name": "arrow", "version": "1"}
    assert results[0]["success"] is False and results[0].get("ts")


def test_portable_plan_runs_through_dqframework_run(spark):
    """First framework-run test of the kernel path: L2 reaches L4."""
    from dq.dq_framework import DQFramework

    config = ConfigFactory.parse_string(f"""
        dqframework {{
          dataframes {{ orders = "orders" }}
          dqrules = [
            {{
              name = "portable_via_framework"
              engine = "portable"
              dataframes = ["orders"]
              dataset = "orders"
              snapshot_sha256 = "{_SHA}"
              plan = [
                {{ constraint = "isComplete", constraint_name = "id_complete", column = "id", level = "Error" }}
              ]
            }}
          ]
        }}
        """)
    framework = DQFramework.__new__(DQFramework)
    framework._config = config
    framework._spark = spark
    orders = spark.createDataFrame([("a",), (None,), ("c",)], ["id"])
    framework.get_dataframe = lambda name: orders
    results = framework.run()
    assert results, "portable rules execute through the standard run path"
    assert all(result.get("ts") for result in results)
    assert any(result["success"] is False for result in results)


# ------------------------------------------------------- the shipped example


def test_the_shipped_spark_example_runs_through_the_framework(spark):
    """It was stale once -- a size check written the way the translator no
    longer reads it -- and nothing ran it."""
    from pathlib import Path

    from dq.dq_framework import DQFramework
    from dq.utils.config_utils import load_config

    example = Path(__file__).resolve().parents[2] / "examples" / "portable_rule.conf"
    framework = DQFramework.__new__(DQFramework)
    framework._config = load_config(str(example))
    framework._spark = spark
    framework.get_dataframe = lambda name: spark.createDataFrame(
        [("a",), (None,)], ["order_id"]
    )
    results = framework.run()
    assert {result["check"]: result["success"] for result in results} == {
        "order_id_complete": False,
        "orders_present": True,
    }
    assert results[0]["details"]["adapter"] == {"name": "spark", "version": "1"}
