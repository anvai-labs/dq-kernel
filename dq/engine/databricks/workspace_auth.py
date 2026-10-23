# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared workspace-credential validation (stdlib-only, no SDK/Spark).

Used by DatabricksCheck and PipelineExpectationsHarvester so the
harvester module stays importable without pyspark or cstriggers.
"""

from urllib.parse import urlsplit


def validate_workspace_url(value):
    """Require an HTTPS origin with no path, query, or credentials."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Databricks workspace URL must not be blank")
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Databricks workspace URL must be an HTTPS origin")
    path = parsed.path.rstrip("/")
    if path:
        raise ValueError("Databricks workspace URL must not contain a path")
    return value.strip().rstrip("/")


def validate_token(value):
    """Require a nonblank token without whitespace."""
    if not isinstance(value, str) or not value or any(ch.isspace() for ch in value):
        raise ValueError("Databricks token must be nonblank and contain no whitespace")
    return value
