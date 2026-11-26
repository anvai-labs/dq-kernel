# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Spark-free runner: a rule pack over files, with no cluster (ADR-014).

``DQFramework`` needs a Spark session, and most teams outside a lakehouse
have none. This runner binds the same run core (``dq.run``) to files read
with pyarrow: the datasets are named by the caller, each portable rule's
plan is computed by the Arrow adapter, the gate policy decides once, and
the evidence is a ``dq-report/v1`` document. Nothing is written to a sink.

What it will not do is substitute. A rule that needs Spark -- any engine
but the portable one, or a portable rule that does not name the Arrow
adapter -- is refused, with the reason, before a single dataset is read.
So is a configured sink: one that is configured and never written would
hold no evidence, silently.

The report names the dataset it is about by what the rule pack says: the
name from ``dqframework.dataset``, or the one dataset the rules run on;
and the digest each rule certifies for it (``snapshot_sha256``), which every
rule on that dataset must agree on. The digest is never worked out from
the files.

This module imports with pyarrow absent and with every other extra
blocked: pyarrow is imported when a dataset is read, and without it the
runner refuses, naming the extra.

    from dq.runner import run

    result = run("rules.conf", {"orders": "orders.parquet"}, report="report.json")
    result.decision.passed
"""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

from dq.config import RulePack, parse_rule_pack
from dq.engine.engine_loader import EngineLoader
from dq.exceptions import ConfigurationError
from dq.gate import EXIT_CONFIGURATION, EXIT_GATE_FAILED, GateDecision
from dq.notifier import GateEvent, GateNotifier, notifier_from_config
from dq.observability import RunObserver, observer_from_config
from dq.report import (
    build_report,
    local_file_path,
    sha256_file_reference,
    write_report,
)
from dq.run import run_rules
from dq.utils.config_utils import load_config
from dq.utils.imports import require_extra

logger = logging.getLogger(__name__)

#: What identifies the run on every outcome, where a Spark application id would.
JOB_ID = "dq-run"
#: The one engine a Spark-free run executes, and the one adapter it computes on.
ENGINE = "portable"
ADAPTER = "arrow"

#: How a file is read, by its extension. A directory is left for pyarrow to
#: tell from the files it holds.
FORMATS = {
    ".parquet": "parquet",
    ".pq": "parquet",
    ".csv": "csv",
    ".json": "json",
    ".ndjson": "json",
    ".arrow": "ipc",
    ".feather": "ipc",
    ".ipc": "ipc",
}


@dataclass(frozen=True)
class Run:
    """What a run over files produced.

    Attributes:
        outcomes: The run's legacy outcome dictionaries, in rule order.
        decision: The gate's decision.
        report: The ``dq-report/v1`` document, written when a path was given.
        run_key: The key every outcome and the report's run share.
        event: The gate event the notifier received.
    """

    outcomes: list
    decision: GateDecision
    report: dict
    run_key: int
    event: GateEvent


def run(
    config: Any,
    datasets: Mapping[str, Any],
    *,
    report: Optional[str] = None,
    observer: Optional[RunObserver] = None,
    notifier: Optional[GateNotifier] = None,
    clock=time.perf_counter,
) -> Run:
    """Run a rule pack over files, with no Spark.

    Args:
        config: A local rule-pack file: a path (with or without
            ``file://``), HOCON, YAML or JSON by its extension. A run's
            evidence names and digests the file it was configured by, so an
            inline rule pack cannot be evidenced.
        datasets: ``{name: path}`` for every dataset the rules name. A file
            is read by its extension (:data:`FORMATS`); a directory as the
            dataset pyarrow finds in it.
        report: Where to write the ``dq-report/v1`` document.
        observer: An observer in place of the configured ones
            (``dqframework.observers``); see :mod:`dq.observability`.
        notifier: A notifier in place of the configured ones
            (``dqframework.notifiers``).
        clock: Reads a monotonic clock in seconds, for durations.

    Returns:
        The :class:`Run`.

    Raises:
        ConfigurationError: For anything a Spark-free run cannot do. All of
            it is refused before a dataset is read: a rule that needs Spark,
            a portable rule that does not name the Arrow adapter, a
            configured sink, a dataset a rule needs that was not given or
            does not exist, one given that no rule uses.
        ImportError: When pyarrow is not installed, named with the extra.
    """
    reference = str(config)
    try:
        is_file = local_file_path(reference).is_file()
    except OSError:
        # Too long a name to be one: rule-pack text given as a path.
        is_file = False
    if not is_file:
        raise ConfigurationError(
            f"the rule pack {reference!r} is not a local file; a run's "
            "evidence names and digests the file it was configured by"
        )
    pack = parse_rule_pack(load_config(reference))
    paths = runnable(pack, datasets)
    identifier, dataset_sha256 = dataset_identity(pack)
    tables = {name: read_dataset(name, paths[name]) for name in sorted(paths)}

    result = run_rules(
        pack.rules,
        load_engine=lambda rule, run_key: EngineLoader().load_engine(
            rule.engine, rule.definition, run_key
        ),
        dataset=tables.__getitem__,
        run_key=time.time_ns() // 1_000_000,
        job_id=JOB_ID,
        dataset_name=identifier,
        policy=pack.gate_policy,
        observer=(
            observer if observer is not None else observer_from_config(pack.observers)
        ),
        notifier_factory=lambda: (
            notifier if notifier is not None else notifier_from_config(pack.notifiers)
        ),
        clock=clock,
        report=report,
    )
    document = build_report(
        results=result.outcomes,
        config_reference=reference,
        config_sha256=sha256_file_reference(reference),
        dataset_id=identifier,
        dataset_sha256=dataset_sha256,
        application_id=JOB_ID,
        spark_version=None,
    )
    if report is not None:
        write_report(report, document)
        logger.info("Wrote data-quality evidence report: %s", report)
    return Run(result.outcomes, result.decision, document, result.run_key, result.event)


def runnable(pack: RulePack, datasets: Any) -> dict:
    """Refuse what a Spark-free run cannot do, and return what is left to read.

    Args:
        pack: A validated rule pack.
        datasets: ``{name: path}``, as :func:`run` takes it.

    Returns:
        ``{name: Path}`` for every dataset the rules name.

    Raises:
        ConfigurationError: Naming every problem at once.
    """
    if not isinstance(datasets, Mapping) or not datasets:
        raise ConfigurationError("a run needs its datasets, as {name: path}")
    problems: List[str] = []
    if pack.repository:
        problems.append(
            "dqframework.repository configures a sink, and a run over files "
            "does not persist: its evidence is the report"
        )
    used = set()
    for rule in pack.rules:
        used.update(rule.dataframes)
        if rule.engine != ENGINE:
            problems.append(
                f"{rule.label} uses engine {rule.engine!r}: a run over files "
                "executes portable rules only, on Arrow. The dq-kernel command "
                "runs the rest on Spark"
            )
        elif rule.definition.get("adapter", None) != ADAPTER:
            named = rule.definition.get("adapter", None)
            how = (
                "names none, which means spark" if named is None else f"names {named!r}"
            )
            problems.append(
                f'{rule.label} does not name adapter = "arrow": it {how}. A run '
                "over files does not substitute an adapter"
            )
    for name in sorted(used - set(datasets)):
        problems.append(f"no dataset is given for {name!r}, which a rule needs")
    for name in sorted(set(datasets) - used):
        problems.append(f"a dataset is given for {name!r}, which no rule uses")
    paths = {}
    for name, given in datasets.items():
        try:
            path = local_file_path(str(given), kind="dataset")
            if not path.exists():
                problems.append(f"dataset {name!r}: no such file or directory: {given}")
            elif path.is_file() and path.suffix.lower() not in FORMATS:
                problems.append(
                    f"dataset {name!r}: no reader knows a {path.suffix!r} file. "
                    f"Use one of {sorted(FORMATS)}, or a directory"
                )
            else:
                paths[name] = path
        except ConfigurationError as error:
            problems.append(f"dataset {name!r}: {error}")
    if problems:
        raise ConfigurationError(
            "a run over files cannot run this rule pack:\n  - "
            + "\n  - ".join(problems)
        )
    return paths


def dataset_identity(pack: RulePack) -> tuple:
    """Return the dataset a run's evidence is about: its name and its digest.

    The name is ``dqframework.dataset``, or the one dataset the rules run
    on. The digest is the snapshot each rule on that dataset certifies
    (``snapshot_sha256``); the rules on one dataset cannot certify two.

    Raises:
        ConfigurationError: When no rule names its dataset and digest; when
            the rules on a dataset certify two snapshots; when several
            datasets are run and the pack does not say which one the
            evidence is about.
    """
    digests = {}
    for rule in pack.rules:
        name = rule.definition.get("dataset", None)
        digest = rule.definition.get("snapshot_sha256", None)
        if not isinstance(name, str) or not isinstance(digest, str):
            continue  # the engine refuses such a payload, in its own words
        if digests.setdefault(name, digest) != digest:
            raise ConfigurationError(
                f"the rules on dataset {name!r} certify two snapshots: "
                f"{digests[name]} and {digest}"
            )
    if not digests:
        raise ConfigurationError(
            "no rule names its dataset and the snapshot digest it certifies: a "
            "portable rule needs `dataset` and `snapshot_sha256`"
        )
    named = pack.dataset
    if named is not None:
        if not isinstance(named, str):
            raise ConfigurationError(
                "dqframework.dataset must be the name of a dataset; "
                f"got {type(named).__name__}"
            )
        if named in digests:
            return named, digests[named]
        if len(digests) == 1:
            return named, next(iter(digests.values()))
        raise ConfigurationError(
            f"dqframework.dataset {named!r} names none of the datasets the "
            f"rules run on: {sorted(digests)}"
        )
    if len(digests) == 1:
        return next(iter(digests.items()))
    names = ", ".join(repr(name) for name in sorted(digests))
    raise ConfigurationError(
        f"a run over several datasets ({names}) must say which one its "
        "evidence is about: set dqframework.dataset to its name"
    )


def read_dataset(name: str, path: Path):
    """Read one dataset with pyarrow, by the format its extension names.

    Parquet and Arrow IPC keep every type. A CSV keeps none: pyarrow decides
    each column's type, a decimal becomes a double, and a NaN reads back as
    null. JSON must be newline-delimited, and its types are told the same way.

    Raises:
        ImportError: When pyarrow is not installed, naming the extra.
        ConfigurationError: When pyarrow cannot read the path, or finds
            no column in it.
    """
    pin, kind = "pyarrow>=16,<27", "runner"
    pyarrow = require_extra("pyarrow", "pyarrow", "arrow", pin=pin, kind=kind)
    datasets = require_extra("pyarrow.dataset", "pyarrow", "arrow", pin=pin, kind=kind)
    try:
        table = datasets.dataset(
            str(path), format=FORMATS.get(path.suffix.lower())
        ).to_table()
    except (pyarrow.ArrowException, OSError) as error:
        raise ConfigurationError(
            f"dataset {name!r} could not be read from {path}: {error}"
        ) from error
    if table.num_columns == 0:
        # pyarrow reads a directory holding nothing it recognises as a
        # dataset with no file and no column at all, and says nothing.
        raise ConfigurationError(
            f"dataset {name!r} could not be read from {path}: nothing in "
            "it was recognised as data"
        )
    logger.info(
        "dataset %r: %d rows and %d columns read from %s",
        name,
        table.num_rows,
        table.num_columns,
        path,
    )
    return table


# ------------------------------------------------------------- the command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dq-run",
        description=(
            "Run a rule pack over files with Arrow: no Spark. Portable rules "
            'name adapter = "arrow"; every other rule needs the dq-kernel command.'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0  the quality gate passed
  1  the gate failed (or the run errored)
  2  the rule pack cannot be run as given, or a dependency is missing

The gate policy is dqframework.gate.fail_on: "any" (default: every failed
check blocks), "error" (warn-level failures do not block) or "never".
The dq-report/v1 document is always written, to --report.

Examples:
  dq-run rules.conf --dataset orders=orders.parquet --report report.json
  dq-run rules.yaml --dataset orders=data/orders/ --report report.json
        """,
    )
    parser.add_argument(
        "config",
        help="A local rule pack: HOCON, YAML or JSON, by its extension",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        required=True,
        metavar="NAME=PATH",
        help=(
            "A dataset the rules name, and where it is; repeat for each. A "
            "file is read by its extension (.parquet, .csv, .json, .ndjson, "
            ".arrow, .feather, .ipc); a directory as the dataset it holds"
        ),
    )
    parser.add_argument(
        "--report",
        required=True,
        help="Where to write the versioned, machine-readable evidence report",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show each refusal's traceback"
    )
    return parser


def named_datasets(values: List[str], parser: argparse.ArgumentParser) -> dict:
    """Return ``{name: path}`` from ``--dataset NAME=PATH`` arguments."""
    datasets = {}
    for value in values:
        name, separator, path = value.partition("=")
        if not separator or not name.strip() or not path.strip():
            parser.error(f"--dataset takes NAME=PATH; got {value!r}")
        datasets[name.strip()] = path.strip()
    return datasets


def summary_lines(result: Run) -> List[str]:
    """What a run came to, counted the way the gate counts.

    Everything here is the policy's own split of the failures into blocking
    and advisory. How many failed checks were warnings is in the report.
    """
    decision = result.decision
    passed = sum(1 for outcome in result.outcomes if outcome.get("success") is True)
    lines = [
        "",
        f"Total checks: {len(result.outcomes)}  passed: {passed}  "
        f"failed: {len(result.outcomes) - passed}",
        f"Under the '{decision.policy}' gate policy: "
        f"{len(decision.blocking)} blocking, {len(decision.advisory)} advisory",
    ]
    if decision.advisory:
        lines.append(
            "Advisory failures (not blocking): " + ", ".join(decision.advisory)
        )
    if decision.blocking:
        lines.append("Blocking failed checks: " + ", ".join(decision.blocking))
    lines.append(f"Report: {result.event.report}")
    lines.append("The gate " + ("passed." if decision.passed else "failed."))
    return lines


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point of the ``dq-run`` command."""
    # A refusal reaches the user through a handler of this command's own, not
    # through the interpreter's last-resort one, which anything in the
    # process could have displaced.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        from dq.validate import is_local_config_reference, validate_config

        if is_local_config_reference(args.config) and not validate_config(args.config):
            logger.error("Configuration validation failed; no dataset was read")
            return EXIT_CONFIGURATION
        result = run(
            args.config, named_datasets(args.dataset, parser), report=args.report
        )
    except ImportError as error:
        # An unknown engine is an ImportError too.
        logger.error("Missing dependency or engine: %s", error)
        return EXIT_CONFIGURATION
    except ConfigurationError as error:
        logger.error("Cannot run %s: %s", args.config, error)
        if args.verbose:
            import traceback

            traceback.print_exc()
        return EXIT_CONFIGURATION
    except Exception as error:  # noqa: BLE001 - a command reports, never crashes
        logger.error("Error running data quality checks: %s", error)
        if args.verbose:
            import traceback

            traceback.print_exc()
        return EXIT_GATE_FAILED

    for line in summary_lines(result):
        print(line)
    return result.decision.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
