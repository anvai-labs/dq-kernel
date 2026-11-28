# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Validation utilities for the Data Quality Framework (the `dq-validate` command).

A rule pack is validated before any Spark work: its syntax, its envelope
against the published schema, its rules against the typed snapshot, and
each built-in engine's required payload against the registry. Every
supported format (HOCON, YAML, JSON) validates here without loading an
engine or a vendor library.
"""

import argparse
import json
import logging
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import List, Tuple
from urllib.parse import urlparse

from dq.config import RulePack, parse_rule_pack, rule_pack_problems
from dq.engine.checkspec import column_name_problems
from dq.engine.registry import (
    find_spec,
    payload_key_for,
    payload_type_for,
    renamed_target,
)
from dq.exceptions import ConfigurationError
from dq.schema import rule_pack_schema_errors
from dq.utils.config_utils import config_format, load_config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConfigValidation:
    """The result of validating one rule pack."""

    reference: str
    format: str
    errors: Tuple[str, ...]

    @property
    def valid(self) -> bool:
        return not self.errors


def is_local_config_reference(config_path: str) -> bool:
    """Return whether a configuration can be validated without Spark or I/O."""
    return urlparse(config_path).scheme in ("", "file")


def _engine_payload_errors(pack: RulePack) -> List[str]:
    """Check each built-in engine's required payload against the registry."""
    errors: List[str] = []
    for rule in pack.rules:
        engine_key = rule.engine.lower()
        renamed = renamed_target(engine_key)
        if renamed:
            errors.append(
                f"Rule {rule.index}: Engine '{rule.engine}' was renamed to "
                f"'{renamed}'; update the configuration"
            )
            continue
        payload_key = payload_key_for(engine_key)
        if not payload_key:
            continue  # an extension engine owns its own payload contract
        payload = rule.definition.get(payload_key, None)
        if not payload:
            errors.append(
                f"Rule {rule.index}: Engine '{rule.engine}' requires a non-empty "
                f"'{payload_key}' payload"
            )
        elif payload_type_for(engine_key) == "string" and (
            not isinstance(payload, str) or not payload.strip()
        ):
            errors.append(
                f"Rule {rule.index}: Engine '{rule.engine}' payload '{payload_key}' "
                "must be a non-empty string"
            )
        elif payload_type_for(engine_key) == "document" and not _is_document(payload):
            errors.append(
                f"Rule {rule.index}: Engine '{rule.engine}' payload '{payload_key}' "
                "must be a mapping, or the text of one"
            )
        else:
            # A column is asked for by a plain name, wherever the engine's
            # payload names one (SPEC-025).
            spec = find_spec(engine_key)
            if spec is not None:
                errors.extend(
                    column_name_problems(spec, rule.definition, f"Rule {rule.index}: ")
                )
    return errors


def _is_document(payload) -> bool:
    """Return whether a payload is a mapping, or the text of one."""
    if isinstance(payload, str):
        return bool(payload.strip())
    return isinstance(payload, Mapping)


def validate_config_document(
    config_path: str, *, report: bool = True
) -> ConfigValidation:
    """Validate a rule pack and report every problem found.

    Args:
        config_path: A local path (with or without ``file://``) or inline
            HOCON text, in any supported format.
        report: To log each problem, and the verdict when there is none. A
            caller that reports the verdict in its own words, as
            ``dq-suggest`` does of the draft it wrote, asks for the verdict
            without it.

    Validation proceeds in stages -- syntax, the envelope against the
    schema, the shape of each rule, then each built-in engine's payload,
    the names it gives a column among them (SPEC-025) -- and reports every
    problem of the first failing stage.

    Returns:
        The :class:`ConfigValidation`; ``errors`` is empty when the pack is
        valid.
    """
    errors: List[str] = []
    parsed = urlparse(config_path)
    if is_local_config_reference(config_path):
        fmt = config_format(config_path)
    else:
        # Treat an unvalidated remote rule set as unknown, never valid. A
        # caller that needs a remote source must first materialize and
        # validate an immutable local copy.
        fmt = parsed.scheme
        errors.append(
            f"Remote config validation is not supported for scheme: {parsed.scheme}"
        )

    if not errors:
        try:
            config = load_config(config_path)
        except ConfigurationError as error:
            errors.append(str(error))
        else:
            # Stages, each reported only when the previous one is clean, so
            # one problem never produces two messages: syntax (above), the
            # envelope against the schema, the shape of each rule, and each
            # built-in engine's payload. Every problem of the first failing
            # stage is reported.
            errors.extend(
                f"Schema: {problem}" for problem in rule_pack_schema_errors(config)
            )
            if not errors:
                shape_problems = rule_pack_problems(config)
                errors.extend(shape_problems)
                if not shape_problems:
                    errors.extend(_engine_payload_errors(parse_rule_pack(config)))

    if report:
        for error in errors:
            logger.error(error)
        if not errors:
            logger.info("Configuration is valid")
    return ConfigValidation(reference=config_path, format=fmt, errors=tuple(errors))


def validate_config(config_path: str) -> bool:
    """Return whether a rule pack is valid (see validate_config_document)."""
    return validate_config_document(config_path).valid


def main():
    """Main entry point for the `dq-validate` command."""
    parser = argparse.ArgumentParser(
        description="Validate a dq-kernel rule pack without running Spark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0  the rule pack is valid
  1  it is not; every problem is reported

Formats follow the file extension: .conf/.hocon (default), .yaml/.yml, .json.
Engine payloads are validated for built-in engines only; an extension engine
owns its own payload contract.

Examples:
  dq-validate config.conf
  dq-validate file://path/to/config.conf
  dq-validate rules.yaml --format json
        """,
    )
    parser.add_argument(
        "config",
        help="Rule pack to validate: a HOCON, YAML or JSON file path, or inline HOCON",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    result = validate_config_document(args.config)

    if args.format == "json":
        print(
            json.dumps(
                {
                    "config": result.reference,
                    "format": result.format,
                    "valid": result.valid,
                    "errors": list(result.errors),
                },
                indent=2,
            )
        )
    elif result.valid:
        print(f"Configuration '{args.config}' is valid.")
    else:
        print(f"Configuration '{args.config}' is invalid.")

    return 0 if result.valid else 1


if __name__ == "__main__":
    sys.exit(main())
