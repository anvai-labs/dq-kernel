# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The columnar importer (SPEC-013 G3): any tool's results in four columns.

CSV and JSON are two spellings of one format, so every rule is checked in
both. CSV is written through the ``csv`` module: quoting by hand is how a
test ends up testing something else.
"""

import csv
import io
import json

import pytest

from dq.exceptions import HarvestError
from dq.harvest import HARVESTERS, harvest
from dq.tests.harvest_support import (
    COLUMNAR_JSON_EXAMPLE,
    CSV_EXAMPLE,
    import_bytes,
    import_json,
)

columnar = HARVESTERS["columnar"]


def rows(*records, header=("check", "success", "criticality", "details")) -> bytes:
    """Return CSV for ``records``, each a tuple of cells in ``header`` order."""
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    if header is not None:
        writer.writerow(header)
    writer.writerows(records)
    return buffer.getvalue().encode("utf-8")


def without_provenance(outcomes):
    return [
        {
            **outcome,
            "details": {k: v for k, v in outcome["details"].items() if k != "origin"},
        }
        for outcome in outcomes
    ]


def test_the_csv_and_json_examples_import_to_the_same_outcomes():
    from_csv = harvest(CSV_EXAMPLE.read_bytes(), reference="checks")
    from_json = harvest(COLUMNAR_JSON_EXAMPLE.read_bytes(), reference="checks")
    assert without_provenance(from_csv) == without_provenance(from_json)
    assert [
        (o["check"], o["success"], o["details"]["criticality"]) for o in from_json
    ] == [
        ("orders_id_unique", False, "error"),
        ("orders_status_in_set", False, "warn"),
        ("orders_row_count", True, "error"),
    ]
    assert from_json[0]["details"]["observed"] == {"failed_rows": 3}
    assert "observed" not in from_json[2]["details"]


def test_the_minimal_record_is_a_check_and_a_boolean():
    (outcome,) = import_json([{"check": "a", "success": True}])
    assert outcome == {
        "check": "a",
        "success": True,
        "details": {"criticality": "error", "origin": outcome["details"]["origin"]},
    }


@pytest.mark.parametrize(
    "spelling, criticality",
    [
        ("warning", "warn"),
        ("Warn", "warn"),
        ("warn ", "warn"),
        ("err", "error"),
        ("ERROR", "error"),
    ],
)
def test_criticality_defaults_to_error_and_takes_every_spelling(spelling, criticality):
    (stated,) = import_json([{"check": "a", "success": False, "criticality": spelling}])
    (default,) = import_json([{"check": "a", "success": False}])
    assert stated["details"]["criticality"] == criticality
    assert default["details"]["criticality"] == "error"


def test_an_empty_criticality_cell_is_no_criticality():
    (outcome,) = import_bytes(rows(("a", "false", "  ", "")))
    assert outcome["details"]["criticality"] == "error"


def test_details_become_observed_in_both_spellings():
    (from_json,) = import_json([{"check": "a", "success": False, "details": {"n": 1}}])
    (from_csv,) = import_bytes(rows(("a", "false", "", json.dumps({"n": 1}))))
    assert (
        from_json["details"]["observed"] == from_csv["details"]["observed"] == {"n": 1}
    )
    assert from_json["details"]["origin"]["system"] == "columnar"


@pytest.mark.parametrize(
    "success, problem",
    [
        ("yes", "has success 'yes'"),
        ("true", "has success 'true'"),
        (1, "has success 1"),
        (None, "has success None"),
    ],
)
def test_a_record_without_a_boolean_success_is_rejected(success, problem):
    record = {"check": "orders_id_unique", "success": success}
    with pytest.raises(HarvestError) as refused:
        import_json([{"check": "first", "success": True}, record])
    error = refused.value
    assert "record 1: 'orders_id_unique' " + problem in str(error)
    assert (error.position, error.record, error.system) == (
        1,
        "orders_id_unique",
        "columnar",
    )


def test_a_record_without_a_success_at_all_is_rejected():
    with pytest.raises(HarvestError, match="record 0: 'a' has success None"):
        import_json([{"check": "a"}])


@pytest.mark.parametrize("cell", ["yes", "1", "", "passes", "t", "falsey"])
def test_a_csv_cell_that_does_not_read_true_or_false_is_rejected(cell):
    source = rows(("first", "true", "", ""), ("a", cell, "", ""))
    with pytest.raises(HarvestError) as refused:
        import_bytes(source)
    error = refused.value
    assert "row 3: 'a' has success" in str(error)
    assert (error.position, error.record) == (1, "a")


def test_a_csv_cell_may_spell_it_in_any_case():
    outcomes = import_bytes(rows(("a", "True ", "", ""), ("b", "FALSE", "", "")))
    assert [outcome["success"] for outcome in outcomes] == [True, False]


def test_an_unknown_column_is_rejected_so_nothing_is_dropped_silently():
    with pytest.raises(
        HarvestError, match=r"record 0 has columns the format does not define: notes"
    ):
        import_json([{"check": "a", "success": True, "notes": "dropped otherwise"}])
    with pytest.raises(
        HarvestError,
        match=r"the header has columns the format does not define: 'notes', 'owner'",
    ):
        import_bytes(
            rows(
                ("a", "true", "whatever", "me"),
                header=("check", "success", "notes", "owner"),
            )
        )


def test_a_header_without_a_required_column_is_rejected():
    """Nothing recognises the file; stated as columnar, it says what is wrong."""
    source = rows(("a", "error"), header=("check", "criticality"))
    with pytest.raises(HarvestError, match="not recognised"):
        import_bytes(source)
    with pytest.raises(HarvestError, match="the header has no 'success' column"):
        columnar.harvest(source, reference="checks")


def test_a_header_that_names_a_column_twice_is_rejected():
    with pytest.raises(HarvestError, match="the header names a column twice"):
        import_bytes(rows(("a", "true", "b"), header=("check", "success", "check")))


@pytest.mark.parametrize("check", ["", "   ", None, 7])
def test_a_check_that_is_not_a_name_is_rejected(check):
    with pytest.raises(HarvestError, match="record 0 has check"):
        import_json([{"check": check, "success": True}])


def test_two_checks_with_one_name_are_rejected_in_both_spellings():
    with pytest.raises(HarvestError, match="Duplicate check label 'a': checks 0 and 1"):
        import_json([{"check": "a", "success": True}, {"check": "a", "success": False}])
    with pytest.raises(HarvestError, match="Duplicate check label 'a': checks 0 and 1"):
        import_bytes(rows(("a", "true", "", ""), ("a", "false", "", "")))


@pytest.mark.parametrize(
    "source", [b"[]", b"[\n]\n", b"check,success\n", b"check,success\n\n \n"]
)
def test_a_file_with_no_record_is_rejected(source):
    with pytest.raises(HarvestError, match="holds no check results"):
        import_bytes(source)


def test_a_row_with_the_wrong_number_of_cells_is_rejected_naming_its_row():
    source = rows(("a", "true", "", ""), ("b", "true", "", ""))
    short = source.replace(b"a,true,,", b"a,true")
    with pytest.raises(HarvestError, match="row 2 has 2 cells and the header has 4"):
        import_bytes(short)
    with pytest.raises(HarvestError, match="row 4 has 5 cells and the header has 4"):
        import_bytes(source + b"b,true,error,,extra\n")


def test_a_blank_line_carries_no_record_and_rows_keep_their_file_lines():
    source = b"check,success\na,true\n\nb,maybe\n"
    with pytest.raises(HarvestError, match=r"row 4: 'b' has success 'maybe'"):
        import_bytes(source)
    outcomes = import_bytes(b"check,success\na,true\n\nb,false\n")
    assert [outcome["check"] for outcome in outcomes] == ["a", "b"]


@pytest.mark.parametrize(
    "cell, problem",
    [
        ("not json", "has details that are not JSON"),
        ("[1, 2]", "has details that are not a JSON object"),
        ('"text"', "has details that are not a JSON object"),
        ("NaN", "has details that are not JSON"),
    ],
)
def test_details_in_csv_must_be_a_json_object_in_one_cell(cell, problem):
    with pytest.raises(HarvestError, match="row 2: 'a' " + problem) as refused:
        import_bytes(rows(("a", "false", "error", cell)))
    assert (refused.value.position, refused.value.record) == (0, "a")


def test_an_empty_details_cell_means_no_details():
    (outcome,) = import_bytes(rows(("a", "false", "warn", "")))
    assert "observed" not in outcome["details"]


@pytest.mark.parametrize("details", ["x", [1], 7, ""])
def test_details_that_are_not_an_object_are_rejected(details):
    with pytest.raises(HarvestError, match="record 0: 'a' has details"):
        import_json([{"check": "a", "success": False, "details": details}])


@pytest.mark.parametrize("criticality", ["fatal", 1, ["error"]])
def test_a_criticality_that_is_not_a_severity_is_rejected(criticality):
    with pytest.raises(HarvestError, match="record 0: 'a' has criticality"):
        import_json([{"check": "a", "success": False, "criticality": criticality}])


def test_a_criticality_that_is_not_a_severity_is_rejected_in_csv_too():
    with pytest.raises(HarvestError, match="row 2: 'a' has criticality 'fatal'"):
        import_bytes(rows(("a", "false", "fatal", "")))


def test_a_utf_8_bom_and_windows_line_endings_are_tolerated():
    source = b"\xef\xbb\xbfcheck,success\r\na,true\r\nb,false\r\n"
    assert columnar.recognises(source) is True
    assert [o["check"] for o in import_bytes(source)] == ["a", "b"]


def test_columns_may_come_in_any_order_and_their_names_may_be_padded():
    source = b'success, details ,check\r\ntrue,"{""n"": 2}",a\r\n'
    (outcome,) = import_bytes(source)
    assert (outcome["check"], outcome["success"], outcome["details"]["observed"]) == (
        "a",
        True,
        {"n": 2},
    )


def test_a_check_name_is_trimmed():
    (outcome,) = import_bytes(rows(("  a ", "true", "", "")))
    assert outcome["check"] == "a"


def test_an_object_at_the_top_level_is_not_a_columnar_document():
    """It could be a vendor's document, so it is not read as one record."""
    source = b'{"check": "a", "success": true}'
    assert columnar.recognises(source) is False
    with pytest.raises(
        HarvestError,
        match="is not a columnar file: a JSON source is an array of records",
    ):
        columnar.harvest(source, reference="checks")


def test_a_record_that_is_not_an_object_is_rejected():
    with pytest.raises(HarvestError, match="record 1 is not an object"):
        import_json([{"check": "a", "success": True}, "b,true"])


@pytest.mark.parametrize(
    "source",
    [
        b"",
        b"\n",
        b"name,ok\na,true\n",  # neither required column
        b'{"check": "a"}',
        b'{"results": [], "suite_name": "s"}',
        b"\xff\xfe\x00",
    ],
)
def test_nothing_but_a_columnar_file_is_recognised(source):
    assert columnar.recognises(source) is False


def test_a_columnar_file_is_recognised_in_both_spellings():
    assert columnar.recognises(b'[{"check": "a", "success": true}]') is True
    assert columnar.recognises(rows(("a", "true", "", ""))) is True
    assert columnar.recognises(rows(("true", "a"), header=("success", "check"))) is True


@pytest.mark.parametrize("source", [b"", b"\n", b" \t\n", b"\xef\xbb\xbf"])
def test_a_file_with_no_header_row_is_not_a_columnar_file(source):
    with pytest.raises(
        HarvestError, match="is not a columnar file: it has no header row"
    ):
        columnar.harvest(source, reference="checks")


def test_a_json_document_must_be_an_array():
    """Stated as columnar, a document still has to be one."""
    with pytest.raises(HarvestError, match="is not a columnar file"):
        columnar.harvest(b'{"check": "a"}', reference="checks")
    with pytest.raises(HarvestError, match="the source is not valid JSON"):
        columnar.harvest(b'[{"check": "a", "success": true}', reference="checks.json")
