# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the profile schema and its validator (SPEC-019).

A profile's shape has two readers: the published schema, for whoever reads
a profile outside this package, and ``Profile.from_document``, which reads
it here. They are held to two standards. Every profile the profilers write
is valid against the schema: a schema that refused what the package writes
would be the one that is wrong. And on the shape of a document the two
agree, each way. What a shape cannot say -- which measures a type has,
that bounds come in pairs, that a least value is not above its greatest --
is the reader's alone, and is listed here so the division is written down.

The validator is the one the rule-pack schema uses, in ``dq.schema``; here
it is checked against the reference ``jsonschema`` implementation whenever
that library is installed.
"""

import copy
import json

import pytest

from dq.exceptions import ConfigurationError
from dq.profile.profile import FAMILIES, SCHEMA_VERSION, SOURCES, Profile
from dq.schema import (
    PROFILE_SCHEMA_PATH,
    SUPPORTED_KEYWORDS,
    profile_schema,
    profile_schema_errors,
    schema_keywords,
)

#: A profile as a profiler writes one, with a column of two families.
BASE = {
    "schema_version": "dq-profile/v1",
    "dataset": {"name": "orders", "snapshot_sha256": "a" * 64},
    "source": "arrow",
    "rows": 3,
    "columns": [
        {
            "name": "id",
            "type": "integer",
            "nulls": 0,
            "distinct": 3,
            "distinct_exact": True,
            "min": 1,
            "max": 3,
            "min_length": None,
            "max_length": None,
            "precision": None,
            "scale": None,
            "not_a_number": None,
        },
        {
            "name": "note",
            "type": "text",
            "nulls": 1,
            "distinct": 2,
            "distinct_exact": False,
            "min": None,
            "max": None,
            "min_length": 0,
            "max_length": 4,
            "precision": None,
            "scale": None,
            "not_a_number": None,
        },
    ],
}


def changed(mutate):
    """Return ``BASE`` with ``mutate`` applied to a copy of it."""
    document = copy.deepcopy(BASE)
    mutate(document)
    return document


def first_column(document):
    return document["columns"][0]


def second_column(document):
    return document["columns"][1]


#: What a shape cannot say. The reader refuses each; the schema cannot.
ONLY_THE_READER = {
    "a-bound-on-a-text-column": lambda document: second_column(document).update(
        min="a", max="b"
    ),
    "one-bound-only": lambda document: first_column(document).update(max=None),
    "least-above-greatest": lambda document: first_column(document).update(
        min=5, max=1
    ),
    "a-length-on-an-integer": lambda document: first_column(document).update(
        min_length=1, max_length=2
    ),
    "a-precision-on-an-integer": lambda document: first_column(document).update(
        precision=10, scale=2
    ),
    "a-not-a-number-count-on-an-integer": lambda document: first_column(
        document
    ).update(not_a_number=0),
    "a-column-named-twice": lambda document: document["columns"].append(
        dict(first_column(document))
    ),
    "more-nulls-than-rows": lambda document: first_column(document).update(nulls=4),
    "a-value-a-type-cannot-take": lambda document: first_column(document).update(
        min="one", max="three"
    ),
}

#: Every other way a profile is not one: the shape says so, and both say it.
NOT_A_PROFILE = {
    "no-version": lambda document: document.pop("schema_version"),
    "another-version": lambda document: document.update(schema_version="dq-profile/v2"),
    "an-unknown-key": lambda document: document.update(profiled_at="2026-11-28"),
    "no-dataset": lambda document: document.pop("dataset"),
    "a-dataset-that-is-not-one": lambda document: document.update(dataset="orders"),
    "an-unknown-key-of-the-dataset": lambda document: document["dataset"].update(
        catalog="unity"
    ),
    "no-name": lambda document: document["dataset"].pop("name"),
    "a-blank-name": lambda document: document["dataset"].update(name=" "),
    "no-digest": lambda document: document["dataset"].pop("snapshot_sha256"),
    "a-short-digest": lambda document: document["dataset"].update(
        snapshot_sha256="a" * 63
    ),
    "a-digest-that-is-not-one": lambda document: document["dataset"].update(
        snapshot_sha256="z" * 64
    ),
    "a-digest-that-is-not-lowercase": lambda document: document["dataset"].update(
        snapshot_sha256="A" * 64
    ),
    "no-source": lambda document: document.pop("source"),
    "another-source": lambda document: document.update(source="excel"),
    "no-rows": lambda document: document.pop("rows"),
    "negative-rows": lambda document: document.update(rows=-1),
    "rows-as-text": lambda document: document.update(rows="3"),
    "a-fraction-of-a-row": lambda document: document.update(rows=3.5),
    "boolean-rows": lambda document: document.update(rows=True),
    "no-columns": lambda document: document.pop("columns"),
    "columns-not-a-list": lambda document: document.update(columns={}),
    "a-column-not-a-mapping": lambda document: document.update(columns=[1]),
    "a-column-with-no-name": lambda document: first_column(document).pop("name"),
    "a-column-with-a-blank-name": lambda document: first_column(document).update(
        name=" "
    ),
    "a-column-with-no-type": lambda document: first_column(document).pop("type"),
    "a-type-that-is-not-one": lambda document: first_column(document).update(
        type="string"
    ),
    "a-column-with-no-nulls": lambda document: first_column(document).pop("nulls"),
    "negative-nulls": lambda document: first_column(document).update(nulls=-1),
    "boolean-nulls": lambda document: first_column(document).update(nulls=False),
    "a-column-with-no-distinct": lambda document: first_column(document).pop(
        "distinct"
    ),
    "negative-distinct": lambda document: first_column(document).update(distinct=-1),
    "a-column-with-no-exactness": lambda document: first_column(document).pop(
        "distinct_exact"
    ),
    "exactness-as-text": lambda document: first_column(document).update(
        distinct_exact="yes"
    ),
    "an-unknown-measure": lambda document: first_column(document).update(mystery=1),
    "a-bound-that-is-a-list": lambda document: first_column(document).update(max=[3]),
    "a-bound-that-is-true": lambda document: first_column(document).update(max=True),
    "a-length-that-is-negative": lambda document: second_column(document).update(
        min_length=-1
    ),
    "a-length-that-is-text": lambda document: second_column(document).update(
        max_length="four"
    ),
    "a-precision-that-is-text": lambda document: first_column(document).update(
        precision="ten"
    ),
    "a-precision-of-nothing": lambda document: first_column(document).update(
        precision=0
    ),
    "a-scale-out-of-range": lambda document: first_column(document).update(scale=77),
    "a-summary-with-no-bound": lambda document: first_column(document).pop("max"),
    "a-summary-with-no-length": lambda document: second_column(document).pop(
        "min_length"
    ),
    "a-summary-with-no-precision": lambda document: second_column(document).pop(
        "precision"
    ),
    "a-summary-with-no-not-a-number": lambda document: second_column(document).pop(
        "not_a_number"
    ),
}


# ------------------------------------------------------------ the schema itself


def test_schema_is_shipped_in_the_package_and_is_draft_2020_12():
    schema = profile_schema()
    assert PROFILE_SCHEMA_PATH.parent.name == "schemas" and PROFILE_SCHEMA_PATH.exists()
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["required"] == [
        "schema_version",
        "dataset",
        "source",
        "rows",
        "columns",
    ]


def test_schema_uses_only_keywords_the_bundled_validator_supports():
    unsupported = schema_keywords(profile_schema()) - SUPPORTED_KEYWORDS
    assert not unsupported, f"the validator cannot evaluate {sorted(unsupported)}"


def test_schema_says_what_the_profile_module_says():
    """Read from the module, so the published schema and the package agree."""
    schema = profile_schema()
    properties = schema["properties"]
    assert properties["schema_version"]["const"] == SCHEMA_VERSION
    assert sorted(properties["source"]["enum"]) == sorted(SOURCES)
    column = schema["$defs"]["column"]
    assert sorted(column["properties"]["type"]["enum"]) == sorted(FAMILIES)
    # Every measure a column summary holds, and no other key.
    assert sorted(column["properties"]) == sorted(
        Profile.from_document(BASE).columns[0].to_document()
    )
    assert column["additionalProperties"] is False
    assert schema["additionalProperties"] is False


def test_the_base_is_a_profile():
    assert profile_schema_errors(BASE) == []
    assert Profile.from_document(BASE).to_document() == BASE


# ---------------------------------------------- what the profilers write is valid


def test_every_profile_the_arrow_profiler_writes_is_valid():
    pyarrow = pytest.importorskip(
        "pyarrow", reason="the Arrow profiler needs the arrow extra"
    )
    from dq.profile import arrow
    from dq.tests.profile_support import (
        FIXTURE,
        SMALL_TABLES,
        corner_frames,
        wide_table,
    )

    tables = [FIXTURE, wide_table(columns=5, rows=7), *corner_frames().values()]
    tables += [*SMALL_TABLES.values()]
    # Corners of the profiler's own: types with a measure the others lack.
    tables += [
        pyarrow.table({"c": pyarrow.array([None, None], pyarrow.null())}),
        pyarrow.table({"c": pyarrow.array([True, None], pyarrow.bool_())}),
    ]
    for table in tables:
        errors = profile_schema_errors(arrow.profile_table(table, "t").to_document())
        assert errors == [], (table.schema, errors[:3])


# ------------------------------------------------------------------ the division


@pytest.mark.parametrize("what", sorted(NOT_A_PROFILE))
def test_the_shape_of_it_is_refused_by_the_schema_and_by_the_reader(what):
    document = changed(NOT_A_PROFILE[what])
    assert profile_schema_errors(document), what
    with pytest.raises(ConfigurationError):
        Profile.from_document(document)


@pytest.mark.parametrize("what", sorted(ONLY_THE_READER))
def test_what_a_shape_cannot_say_is_refused_by_the_reader_alone(what):
    """The schema is for the shape; these are meanings, and the reader keeps them."""
    document = changed(ONLY_THE_READER[what])
    assert profile_schema_errors(document) == [], what
    with pytest.raises(ConfigurationError):
        Profile.from_document(document)


@pytest.mark.parametrize("what", sorted(NOT_A_PROFILE))
def test_every_listed_mutation_changes_the_base(what):
    """A mutation that changed nothing would pass both refusals for nothing."""
    # As text: ``False == 0`` and ``True == 1`` to a dict, and not to JSON.
    assert json.dumps(changed(NOT_A_PROFILE[what])) != json.dumps(BASE), what


def test_no_mutation_is_listed_twice():
    """In both lists it would contradict itself: refused by the schema, and not."""
    assert not set(NOT_A_PROFILE) & set(ONLY_THE_READER)
    assert all(
        json.dumps(changed(mutate)) != json.dumps(BASE)
        for mutate in ONLY_THE_READER.values()
    )


# ------------------------------------------------------------- the reference


def test_validator_agrees_with_the_reference_jsonschema_implementation():
    jsonschema = pytest.importorskip("jsonschema")
    validator = jsonschema.Draft202012Validator(profile_schema())
    documents = [
        BASE,
        *map(changed, NOT_A_PROFILE.values()),
        *map(changed, ONLY_THE_READER.values()),
    ]
    documents.append(json.loads(json.dumps(BASE)))  # as text, round-tripped
    for document in documents:
        ours = profile_schema_errors(document)
        theirs = [error.message for error in validator.iter_errors(document)]
        assert bool(ours) == bool(theirs), (ours[:2], theirs[:2])
    # And the schema itself is one the reference accepts.
    jsonschema.Draft202012Validator.check_schema(profile_schema())
