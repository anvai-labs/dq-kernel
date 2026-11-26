# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""``dq-import``: another tool's results as this framework's evidence (SPEC-013).

Reads an imported source once, so that the report's digest is of the bytes
that were parsed, applies the gate policy to what it holds, and writes a
``dq-report/v1`` document.
"""

import argparse
import hashlib
import logging
from typing import List, Optional

from dq.exceptions import ConfigurationError, HarvestError, ValidationError
from dq.gate import (
    DEFAULT_POLICY,
    EXIT_CONFIGURATION,
    POLICIES,
    evaluate_gate,
)
from dq.harvest import HARVESTERS, harvest
from dq.report import build_report, read_local_file, write_report

logger = logging.getLogger(__name__)


def application_id(outcomes: List[dict], digest: str) -> str:
    """Return what identifies the run that produced a source's evidence.

    The run the source names, when all of it comes from one. A columnar
    file names none, so it is identified by what it is: the first digits of
    its own digest, which the same bytes always give.
    """
    runs = {outcome["details"]["origin"].get("run") for outcome in outcomes}
    runs.discard(None)
    if len(runs) == 1:
        return runs.pop()
    return f"import-{digest[:16]}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dq-import",
        description=(
            "Import another tool's data-quality results as dq-kernel evidence: "
            "a dbt run_results.json, a Great Expectations validation result, or a "
            "columnar file (CSV, or a JSON array of records)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0  the gate passed
  1  the gate failed
  2  the source was rejected, could not be read, or an argument is invalid

The gate policy is --fail-on: "any" (default: every failed check blocks),
"error" (warn-level failures do not block) or "never".

Examples:
  dq-import target/run_results.json --dataset-id orders \
      --dataset-sha256 <64 hex digits> --report-json evidence.json
  dq-import checks.csv --fail-on error
        """,
    )
    parser.add_argument(
        "source",
        help="The file to import: a local path, or file://path",
    )
    parser.add_argument(
        "--system",
        choices=sorted(HARVESTERS),
        help="The source system; told from the document when omitted",
    )
    parser.add_argument(
        "--fail-on",
        choices=POLICIES,
        default=DEFAULT_POLICY,
        help="Which failed checks block (default: any)",
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
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    # A refusal reaches the user through a handler of this command's own, not
    # through the interpreter's last-resort one, which anything in the
    # process could have displaced.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.report_json and not (args.dataset_id and args.dataset_sha256):
        parser.error("--report-json requires --dataset-id and --dataset-sha256")
    if (args.dataset_id or args.dataset_sha256) and not args.report_json:
        parser.error("dataset evidence options require --report-json")

    try:
        # Read once: the digest in the report is of the bytes that are parsed.
        source = read_local_file(args.source, kind="source")
        outcomes = harvest(source, reference=args.source, system=args.system)
    except (HarvestError, ConfigurationError) as error:
        logger.error("Cannot import %s: %s", args.source, error)
        return EXIT_CONFIGURATION

    decision = evaluate_gate(outcomes, args.fail_on)
    digest = hashlib.sha256(source).hexdigest()
    if args.report_json:
        try:
            report = build_report(
                results=outcomes,
                config_reference=args.source,
                config_sha256=digest,
                dataset_id=args.dataset_id,
                dataset_sha256=args.dataset_sha256,
                application_id=application_id(outcomes, digest),
                spark_version=None,
            )
        except ValidationError as error:
            logger.error("Cannot write a report: %s", error)
            return EXIT_CONFIGURATION
        write_report(args.report_json, report)
        logger.info("Imported evidence report: %s", args.report_json)

    passed = sum(1 for outcome in outcomes if outcome["success"])
    print(f"\n{'=' * 60}")
    print("Imported evidence")
    print(f"{'=' * 60}")
    print(f"Source: {outcomes[0]['details']['origin']['system']} {args.source}")
    print(f"Total checks: {len(outcomes)}")
    print(f"Passed: {passed}")
    print(
        f"Failed: {len(outcomes) - passed} "
        f"({len(decision.blocking)} blocking, {len(decision.advisory)} advisory)"
    )
    print(f"Gate policy: {decision.policy}")
    print(f"{'=' * 60}\n")

    if decision.advisory:
        print("Advisory failures (not blocking):")
        for name in decision.advisory:
            print(f"  - {name}")
    if decision.passed:
        return decision.exit_code
    print("Blocking failed checks:")
    for name in decision.blocking:
        print(f"  - {name}")
    return decision.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
