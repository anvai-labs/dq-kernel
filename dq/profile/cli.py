# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The two commands of profiling: ``dq-profile`` and ``dq-suggest`` (SPEC-019).

``dq-profile`` reads a dataset -- with the Arrow profiler, or with the
Spark one -- and writes its profile as JSON. ``dq-suggest`` reads a profile and writes a draft rule
pack in HOCON, YAML or JSON, every rule a warning, and checks that what it
wrote validates. Neither applies anything to a pipeline.

Both import nothing optional until it is needed, so each answers ``--help``
with no extra installed, and names the extra when one is missing.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import re
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

from dq.engine.registry import find_spec
from dq.exceptions import ConfigurationError
from dq.gate import EXIT_CONFIGURATION, EXIT_GATE_FAILED, EXIT_PASSED
from dq.profile.profile import Profile
from dq.profile.suggest import (
    FORMATS,
    TARGETS,
    format_of,
    passed_over,
    render,
    suggest,
    unconfirmed,
)

logger = logging.getLogger(__name__)

#: The profilers a dataset can be read with. In the kernel's own words an
#: engine is what a rule runs on and an adapter what computes it; these
#: are neither, and ``dq-suggest --engine`` names the other thing.
PROFILERS = ("arrow", "spark")

#: What the Spark profiler reads, by a file's extension. Arrow IPC is not
#: among them: Spark has no reader for it.
SPARK_FORMATS = {
    ".parquet": "parquet",
    ".pq": "parquet",
    ".csv": "csv",
    ".json": "json",
    ".ndjson": "json",
}

#: The extension each draft format is read by, as the package reads a rule
#: pack: a printed draft is checked through a file of its own.
EXTENSIONS = {"hocon": "conf", "yaml": "yaml", "json": "json"}


# --------------------------------------------------------------- the reading


def snapshot_digest(path: Path) -> str:
    """Return the SHA-256 of a dataset's bytes.

    A file's own; or, for a directory, every file's under it in name order
    -- a marker or a checksum file included, so the digest of a snapshot
    never depends on which files a reader passes over. A portable rule
    certifies a snapshot by such a digest, and the framework never computes
    one: ``dq-profile`` read the bytes, so it is the one that can.

    Raises:
        ConfigurationError: When there is no file to read.
    """
    files = (
        sorted(
            (item for item in path.rglob("*") if item.is_file()),
            key=lambda item: item.relative_to(path).as_posix(),
        )
        if path.is_dir()
        else [path]
    )
    if not files:
        raise ConfigurationError(f"the dataset at {path} holds no file")
    digest = hashlib.sha256()
    for item in files:
        with item.open("rb") as stream:
            for block in iter(lambda: stream.read(1 << 20), b""):
                digest.update(block)
    return digest.hexdigest()


def profile_file(
    path: Path, name: str, profiler: str, *, approximate: bool = False
) -> Profile:
    """Return the profile of a dataset on disk, read with ``profiler``.

    Raises:
        ImportError: When the profiler's library is not installed, naming
            the extra.
        ConfigurationError: For a format the profiler cannot read, for an
            approximation asked of Arrow (whose counts are always exact),
            or for data Spark itself cannot read.
        OSError: When the dataset's bytes cannot be read.
    """
    digest = snapshot_digest(path)
    if profiler == "arrow":
        from dq.profile.arrow import profile_table
        from dq.runner import read_dataset

        return profile_table(read_dataset(name, path), name, snapshot_sha256=digest)
    from dq.profile import spark as spark_profiler

    spark_session = spark_profiler.session_class()
    # What Spark says of the data is a refusal of the data: on Arrow the
    # reader's own refusal is, and on Spark this is.
    of_spark = spark_profiler.own_exceptions()
    started = spark_session.getActiveSession() is None
    try:
        session = spark_session.builder.appName("dq-profile").getOrCreate()
        return spark_profiler.profile_frame(
            _frame(session, path),
            name,
            approximate=approximate,
            snapshot_sha256=digest,
        )
    except of_spark as error:
        raise ConfigurationError(
            f"dataset {name!r} could not be read with Spark: {_said(error)}"
        ) from error
    finally:
        # A session that was not there before is not left running after.
        if started and spark_session.getActiveSession() is not None:
            spark_session.getActiveSession().stop()


def _frame(session, path: Path):
    """Return the dataset as a Spark DataFrame, read by its extension."""
    if path.is_dir():
        return session.read.parquet(str(path))
    reader = SPARK_FORMATS.get(path.suffix.lower())
    if reader is None:
        raise ConfigurationError(
            f"the Spark profiler reads Parquet, CSV or JSON, or a Parquet "
            f"directory; {path.name!r} is a {path.suffix or 'file with no extension'}"
        )
    if reader == "csv":
        return (
            session.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(str(path))
        )
    return getattr(session.read, reader)(str(path))


#: How much of what Spark said a refusal repeats; the rest is under --verbose.
SAID_AT_MOST = 600

#: A Spark error class, as Spark states one: ``[PATH_NOT_FOUND]``.
_STATED = re.compile(r"\[[A-Z][A-Z0-9_]*\] ")


def _said(error: BaseException) -> str:
    """Return what Spark said of a failure, without its stack trace.

    Spark states a failure by class, in brackets, and restates it down the
    chain of causes; the last it states is why the first happened, and the
    one a reader needs. A failure stated without a class keeps its first
    line. Either is cut at :data:`SAID_AT_MOST` characters.
    """
    text = str(error)
    stated = [found.start() for found in _STATED.finditer(text)]
    said = text[stated[-1] :] if stated else text
    # The frames of a stack start on their own line, indented by a tab.
    said = " ".join(said.split("\n\tat ", 1)[0].split())
    return said if len(said) <= SAID_AT_MOST else said[: SAID_AT_MOST - 3] + "..."


# ------------------------------------------------------------ the commands


def _refusal(verbose: bool, error: BaseException, lead: str) -> int:
    """Report a refusal in the command's own words; return its exit code."""
    logger.error("%s: %s", lead, error)
    if verbose:
        import traceback

        traceback.print_exc()
    return EXIT_CONFIGURATION


def _errored(verbose: bool, error: BaseException, lead: str) -> int:
    """Report a failure that is no one's stated refusal; return its exit code."""
    logger.error("%s: %s", lead, error)
    if verbose:
        import traceback

        traceback.print_exc()
    return EXIT_GATE_FAILED


def _profile_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dq-profile",
        description=(
            "Profile a dataset -- one bounded summary per column -- and write "
            "it as JSON, for dq-suggest to turn into a draft rule pack."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0  the profile was written
  1  the profiler errored
  2  the dataset or an argument is refused, or a dependency is missing

Examples:
  dq-profile --dataset orders=orders.parquet --profile orders.profile.json
  dq-profile --dataset orders=orders.parquet --profiler spark --approximate-distinct \\
      --profile orders.profile.json
        """,
    )
    parser.add_argument(
        "--dataset",
        action="append",
        required=True,
        metavar="NAME=PATH",
        help="The dataset's name and where it is: a file by its extension, or a directory",
    )
    parser.add_argument(
        "--profile",
        help="Where to write the profile; printed to standard output when omitted",
    )
    parser.add_argument(
        "--profiler",
        choices=PROFILERS,
        default="arrow",
        help="Which profiler reads the dataset (default: arrow)",
    )
    parser.add_argument(
        "--approximate-distinct",
        action="store_true",
        help="With Spark: count distinct values approximately",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show each refusal's traceback"
    )
    return parser


def profile_main(argv: Optional[List[str]] = None) -> int:
    """Entry point of the ``dq-profile`` command."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = _profile_parser()
    args = parser.parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    from dq.runner import named_datasets

    datasets = named_datasets(args.dataset, parser)
    if len(datasets) != 1:
        parser.error("a profile is of one dataset: give one --dataset")
    name, where = next(iter(datasets.items()))
    path = Path(where)
    try:
        if not path.exists():
            raise ConfigurationError(
                f"dataset {name!r}: no such file or directory: {where}"
            )
        if args.approximate_distinct and args.profiler != "spark":
            raise ConfigurationError(
                "--approximate-distinct asks Spark to count approximately; "
                "Arrow's distinct counts are always exact"
            )
        profile = profile_file(
            path, name, args.profiler, approximate=args.approximate_distinct
        )
    except ImportError as error:
        logger.error("Missing dependency: %s", error)
        return EXIT_CONFIGURATION
    except ConfigurationError as error:
        return _refusal(args.verbose, error, "Cannot profile")
    except OSError as error:
        return _refusal(args.verbose, error, "Cannot read the dataset")
    except Exception as error:  # noqa: BLE001 - a command reports, never crashes
        return _errored(args.verbose, error, "Error profiling")
    try:
        if args.profile:
            _write(Path(args.profile), profile.to_json())
    except (ConfigurationError, OSError) as error:
        return _refusal(args.verbose, error, "Cannot write the profile")

    if args.profile:
        logger.info(
            "wrote the profile of %s to %s: %d rows, %d columns, read with %s",
            name,
            args.profile,
            profile.rows,
            len(profile.columns),
            profile.source,
        )
    else:
        sys.stdout.write(profile.to_json())
    return EXIT_PASSED


def _suggest_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dq-suggest",
        description=(
            "Turn a profile into a draft rule pack: every suggested rule a "
            "warning, written in words its engine already accepts."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0  the draft was written -- or the profile warrants no rule
  1  the draft written does not validate, or the command errored: a defect,
     not the profile's fault
  2  the profile or an argument is refused

Examples:
  dq-suggest orders.profile.json --engine deequ --output orders.deequ.conf
  dq-suggest orders.profile.json --engine portable --output orders.portable.yaml
        """,
    )
    parser.add_argument("profile", help="A profile, as dq-profile writes it")
    parser.add_argument(
        "--engine",
        choices=TARGETS,
        required=True,
        help="The vocabulary the draft is written in",
    )
    parser.add_argument(
        "--output",
        help="Where to write the draft; its format follows its extension "
        "(.conf, .hocon, .yaml, .yml, .json). Printed to standard output when omitted",
    )
    parser.add_argument(
        "--format",
        choices=FORMATS,
        help="The draft's format, where --output's extension does not say; "
        "HOCON when printed",
    )
    parser.add_argument(
        "--snapshot-sha256",
        help="The snapshot a portable rule certifies, where the profile carries none",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show each refusal's traceback"
    )
    return parser


def suggest_main(argv: Optional[List[str]] = None) -> int:
    """Entry point of the ``dq-suggest`` command."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = _suggest_parser()
    args = parser.parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        profile = Profile.from_json(Path(args.profile).read_text(encoding="utf-8"))
        pack = suggest(profile, args.engine, snapshot_sha256=args.snapshot_sha256)
        fmt = args.format or (format_of(args.output) if args.output else "hocon")
        _agree(args.format, args.output)
        text = render(pack, fmt)
    except (OSError, UnicodeDecodeError) as error:
        return _refusal(args.verbose, error, "Cannot read the profile")
    except ConfigurationError as error:
        return _refusal(args.verbose, error, "Cannot suggest from the profile")
    except Exception as error:  # noqa: BLE001 - a command reports, never crashes
        return _errored(args.verbose, error, "Error suggesting")

    passed = passed_over(profile)
    if passed:
        logger.warning(
            "no rule is suggested for a column whose name is not a plain "
            "identifier, which is how a rule asks for a column: %s",
            ", ".join(repr(name) for name in passed),
        )
    unconfirmed_ = unconfirmed(profile, args.engine)
    if unconfirmed_:
        logger.warning(
            "not suggested for %s, whose own completeness check would not confirm "
            "it on this data: %s",
            args.engine,
            ", ".join(repr(name) for name in unconfirmed_),
        )
    rules = pack["dqframework"]["dqrules"]
    if not rules:
        logger.info(
            "the profile warrants no rule for %s that its own checks would confirm: "
            "no column is without nulls, exactly distinct, or never below 0",
            args.engine,
        )
        return EXIT_PASSED
    # A portable draft holds one rule per semantic version; the rest, one.
    checks = sum(len(rule[find_spec(args.engine).payload_key]) for rule in rules)
    written = Path(args.output) if args.output else None
    try:
        if written is not None:
            _write(written, text)
        else:
            sys.stdout.write(text)
            # A printed draft is checked like a written one: through a copy
            # of it, since the package's validator reads a file.
            written = _held(text, fmt)
    except (ConfigurationError, OSError) as error:
        return _refusal(args.verbose, error, "Cannot write the draft")

    from dq.validate import validate_config_document

    try:
        # The command reports the verdict in its own words, with the
        # reasons: the validator's line about a file would read as if the
        # user had asked it.
        checked = validate_config_document(str(written), report=False)
    finally:
        if args.output is None:
            written.unlink(missing_ok=True)
    if not checked.valid:
        logger.error(
            "the draft written to %s does not validate -- a defect of "
            "dq-suggest, not of the profile: %s",
            args.output or "standard output",
            "; ".join(checked.errors),
        )
        return EXIT_GATE_FAILED
    logger.info(
        "wrote %d suggested rule%s for %s to %s, every one a warning",
        checks,
        "" if checks == 1 else "s",
        args.engine,
        args.output or "standard output",
    )
    return EXIT_PASSED


def _agree(fmt: Optional[str], output: Optional[str]) -> None:
    """Refuse a format that contradicts the extension of where the draft goes.

    The package reads a rule pack by its extension, so a draft written as
    HOCON into a ``.json`` file would be read as JSON, and fail.

    Raises:
        ConfigurationError: When both name a format and they differ.
    """
    if not fmt or not output:
        return
    try:
        named = format_of(output)
    except ConfigurationError:
        return  # the extension names none: --format is what stands
    if named != fmt:
        raise ConfigurationError(
            f"--format {fmt} contradicts {output!r}, which names {named}: a rule "
            "pack is read by its extension"
        )


def _write(path: Path, text: str) -> None:
    """Write text to a path, refusing a directory that is not there."""
    if str(path.parent) and not path.parent.is_dir():
        raise ConfigurationError(f"there is no directory to write into: {path.parent}")
    path.write_text(text, encoding="utf-8")


def _held(text: str, fmt: str) -> Path:
    """Return a temporary file holding ``text``, under its format's extension.

    The caller removes it.
    """
    with tempfile.NamedTemporaryFile(
        "w", suffix=f".{EXTENSIONS[fmt]}", encoding="utf-8", delete=False
    ) as stream:
        stream.write(text)
        return Path(stream.name)


if __name__ == "__main__":
    raise SystemExit(profile_main())
