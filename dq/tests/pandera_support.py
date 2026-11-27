# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Frames and documents for the pandera engine's live tests (SPEC-017)."""

import copy
from pathlib import Path

import pandas as pd
import polars as pl
from pyhocon import ConfigFactory

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "pandera_validation.conf"

BACKENDS = ("pandas", "polars")

#: Rows that fail an error-level check, a warning-level check and a
#: duplicate: every kind of outcome has its case.
ROWS = {"id": [1, 2, 2], "amount": [-5.0, 2000.0, 3.0], "note": ["a", None, "c"]}

#: Rows that satisfy the schema.
CLEAN = {"id": [1, 2, 3], "amount": [3.0, 4.0, 5.0], "note": ["a", None, "c"]}

#: The same schema for each backend. Polars spells a check's argument
#: `value` where pandas writes `min_value` and `max_value`.
DOCUMENTS = {
    "pandas": {
        "schema_type": "dataframe",
        "api": "pandas",
        "name": "orders",
        "columns": {
            "id": {"dtype": "int64", "unique": True},
            "amount": {
                "dtype": "float64",
                "greater_than_or_equal_to": {"min_value": 0},
                "less_than_or_equal_to": {"max_value": 1000, "raise_warning": True},
            },
            "note": {"dtype": "str", "nullable": True},
        },
    },
    "polars": {
        "schema_type": "polars_dataframe",
        "api": "polars",
        "name": "orders",
        "columns": {
            "id": {"dtype": "Int64", "unique": True},
            "amount": {
                "dtype": "Float64",
                "greater_than_or_equal_to": {"value": 0},
                "less_than_or_equal_to": {"value": 1000, "raise_warning": True},
            },
            "note": {"dtype": "String", "nullable": True},
        },
    },
}

#: What the schema above reports on, in document order.
LABELS = [
    "dtype_id",
    "not_nullable_id",
    "unique_id",
    "dtype_amount",
    "not_nullable_amount",
    "greater_than_or_equal_to_amount",
    "less_than_or_equal_to_amount",
    "dtype_note",
]


def reordered(backend):
    """Return the backend's document with its columns in another order."""
    document = DOCUMENTS[backend]
    return dict(document, columns=dict(reversed(list(document["columns"].items()))))


def document(backend, **columns):
    """Return the backend's document with ``columns`` changed on it."""
    changed = copy.deepcopy(DOCUMENTS[backend])
    for name, said in columns.items():
        if said is None:
            del changed["columns"][name]
        else:
            changed["columns"][name].update(said)
    return changed


def frame(backend, rows=None, without=()):
    """Return a frame of ``rows`` on ``backend``, minus the ``without`` columns."""
    rows = {
        name: values for name, values in (rows or ROWS).items() if name not in without
    }
    if backend == "pandas":
        return pd.DataFrame(rows)
    return pl.DataFrame(rows)


def rule(backend, document=None):
    """Return a rule whose schema is the backend's document."""
    return ConfigFactory.from_dict(
        {
            "name": f"{backend}-orders",
            "engine": "pandera",
            "schema": document or DOCUMENTS[backend],
        }
    )
