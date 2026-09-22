# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Custom exceptions for Data Quality Framework."""


class DQFrameworkError(Exception):
    """Base exception for all Data Quality Framework errors."""

    pass


class ConfigurationError(DQFrameworkError):
    """Raised when configuration is invalid or missing required fields."""

    pass


class EngineNotFoundError(DQFrameworkError):
    """Raised when the specified engine cannot be loaded."""

    pass


class EngineExecutionError(DQFrameworkError):
    """Raised when an engine fails during execution."""

    pass


class DataFrameNotFoundError(DQFrameworkError):
    """Raised when a referenced DataFrame cannot be found."""

    pass


class ValidationError(DQFrameworkError):
    """Raised when data validation fails."""

    pass


class RepositoryError(DQFrameworkError):
    """Raised when there's an error with the metrics repository."""

    pass
