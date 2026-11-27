# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface for Data Quality Framework."""

import argparse
import sys
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for dq-kernel CLI."""
    parser = argparse.ArgumentParser(
        description="Data Quality Framework - Run data quality checks on Spark DataFrames",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0  the quality gate passed
  1  the gate failed (or the run errored)
  2  the configuration is invalid or a dependency is missing

The gate policy is dqframework.gate.fail_on: "any" (default: every failed
check blocks), "error" (warn-level failures do not block) or "never".

Examples:
  dq-kernel config.conf
  dq-kernel file://path/to/config.conf
  dq-kernel s3://bucket/path/to/config.conf --spark-master spark://host:7077
        """,
    )
    parser.add_argument(
        "config",
        help="Path to HOCON configuration file (supports file://, s3://, abfss://, http://)",
    )
    parser.add_argument(
        "--spark-master",
        default="local[*]",
        help="Spark master URL (default: local[*])",
    )
    parser.add_argument(
        "--app-name",
        default="dq-kernel",
        help="Spark application name (default: dq-kernel)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "--report-json",
        help="Write a versioned, machine-readable evidence report to this path",
    )
    parser.add_argument(
        "--dataset-id",
        help="Immutable dataset identifier (required with --report-json)",
    )
    parser.add_argument(
        "--dataset-sha256",
        help="Dataset SHA-256 digest (required with --report-json)",
    )

    args = parser.parse_args()

    if args.report_json and not (args.dataset_id and args.dataset_sha256):
        parser.error("--report-json requires --dataset-id and --dataset-sha256")
    if (args.dataset_id or args.dataset_sha256) and not args.report_json:
        parser.error("dataset evidence options require --report-json")

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Bound before the try: the handlers below return these, and an import
    # inside it is what fails where Spark is not installed.
    from dq.exceptions import ConfigurationError
    from dq.gate import EXIT_CONFIGURATION, EXIT_GATE_FAILED, EXIT_PASSED, evaluate_gate

    try:
        from dq.dq_framework import DQFramework
        from dq.report import build_report, sha256_file_reference, write_report
        from dq.validate import is_local_config_reference, validate_config

        # Judged before Spark is looked for, so the verdict on a rule pack is
        # the same with or without Spark installed.
        if is_local_config_reference(args.config) and not validate_config(args.config):
            logger.error("Configuration validation failed; Spark was not started")
            return EXIT_CONFIGURATION

        from pyspark.sql import SparkSession

        logger.info(f"Initializing Spark session with master: {args.spark_master}")
        spark = (
            SparkSession.builder.master(args.spark_master)
            .appName(args.app_name)
            .getOrCreate()
        )

        logger.info(f"Loading configuration from: {args.config}")
        framework = DQFramework(spark, args.config, report_reference=args.report_json)

        logger.info("Running data quality checks...")
        results = framework.run()

        if not results:
            logger.error("Validation emitted zero check outcomes")
            return EXIT_GATE_FAILED

        # The gate decision is made in one place (ADR-013): the policy is
        # dqframework.gate.fail_on, and severity comes from each outcome.
        decision = evaluate_gate(results, framework.gate_policy)
        passed = sum(1 for r in results if r.get("success", False))
        failed = len(results) - passed

        if args.report_json:
            report = build_report(
                results=results,
                config_reference=args.config,
                config_sha256=sha256_file_reference(args.config),
                dataset_id=args.dataset_id,
                dataset_sha256=args.dataset_sha256,
                application_id=spark.sparkContext.applicationId,
                spark_version=spark.version,
            )
            write_report(args.report_json, report)
            logger.info("Wrote data-quality evidence report: %s", args.report_json)

        print(f"\n{'='*60}")
        print("Data Quality Check Results")
        print(f"{'='*60}")
        print(f"Total checks: {len(results)}")
        print(f"Passed: {passed}")
        print(
            f"Failed: {failed} ({len(decision.blocking)} blocking, {len(decision.advisory)} advisory)"
        )
        print(f"Gate policy: {decision.policy}")
        print(f"{'='*60}\n")

        if decision.advisory:
            print("Advisory failures (not blocking):")
            for name in decision.advisory:
                print(f"  - {name}")
        if decision.passed:
            return EXIT_PASSED
        print("Blocking failed checks:")
        for name in decision.blocking:
            print(f"  - {name}")
        return decision.exit_code

    except ImportError as e:
        # EngineNotFoundError is an ImportError too: an unknown engine is a
        # configuration problem, fixed by a commit rather than by the data.
        logger.error(f"Missing dependency or engine: {e}")
        logger.error("Install Spark support with: pip install dq-kernel[spark]")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return EXIT_CONFIGURATION
    except ConfigurationError as e:
        logger.error(f"Invalid configuration: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return EXIT_CONFIGURATION
    except Exception as e:
        logger.error(f"Error running data quality checks: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return EXIT_GATE_FAILED


if __name__ == "__main__":
    sys.exit(main())
