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

pytestmark = pytest.mark.spark

_SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def portable_config(plan=None, dataset="orders", sha=_SHA):
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
