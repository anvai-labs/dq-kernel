# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Custom exceptions for Data Quality Framework."""


class DQFrameworkError(Exception):
    """Base exception for all Data Quality Framework errors."""

    pass


class ConfigurationError(DQFrameworkError):
    """Raised when configuration is invalid or missing required fields."""

    pass


class EngineNotFoundError(DQFrameworkError, ImportError):
    """Raised when the specified engine cannot be loaded.

    Deliberately also an ``ImportError``: existing callers (notably the
    CLI's missing-dependency handler) catch ``ImportError`` around engine
    loading, and vendor-import failures must keep flowing through that
    net unchanged.
    """

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


class HarvestError(ConfigurationError):
    """Raised when an external result source cannot be imported.

    Deliberately also a ``ConfigurationError``: the source is to be fixed
    or exported again, not the data, so it means what that means -- the
    importer's command exits 2, as it does for a rule pack it cannot load.

    Attributes:
        system: The source system the harvester was reading, if known.
        position: Index of the offending record in the source, from 0, when
            one record is at fault.
        record: What the source calls the offending record, when it has a
            name.
    """

    def __init__(self, message, *, system=None, position=None, record=None):
        super().__init__(message)
        self.system = system
        self.position = position
        self.record = record
