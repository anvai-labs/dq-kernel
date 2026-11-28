# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Spark profiler, the driver-side gate and the engines' own verdicts (SPEC-019).

Needs the JVM: the session fixture starts one. Gate G1 is measured through
Parquet, the one faithful way from Arrow to Spark; G2 intercepts every way
a DataFrame reaches the driver; G3 and G4 run each engine's draft on the
engine that will read it, and then the way a user runs a rule pack, through
the framework.
"""

import datetime as dt
import decimal
import json
from contextlib import contextmanager

import pytest

pytest.importorskip("pyspark", reason="the Spark profiler needs the spark extra")

pytestmark = pytest.mark.spark

from dq.dq_framework import DQFramework  # noqa: E402
from dq.engine.engine_loader import EngineLoader  # noqa: E402
from dq.exceptions import ConfigurationError  # noqa: E402
from dq.profile import arrow, cli, suggest  # noqa: E402
from dq.profile import spark as spark_profiler  # noqa: E402
from dq.profile.cli import profile_main, snapshot_digest, suggest_main  # noqa: E402
from dq.profile.profile import Profile  # noqa: E402
from dq.tests.profile_support import (  # noqa: E402
    FIXTURE,
    SAID,
    corner_frames,
    parquet_of,
    wide_table,
    with_a_broken_id,
)
from dq.schema import profile_schema_errors  # noqa: E402
from dq.utils.config_utils import load_config  # noqa: E402
from dq.validate import validate_config  # noqa: E402

# --------------------------------------------------------------------- G1


def test_spark_and_arrow_return_equal_profiles_over_the_same_data(spark, tmp_path):
    path = parquet_of(FIXTURE, tmp_path)
    on_arrow = arrow.profile_table(FIXTURE, "orders")
    on_spark = spark_profiler.profile_frame(spark.read.parquet(str(path)), "orders")

    assert on_spark == on_arrow
    assert on_spark.source == "spark"
    assert profile_schema_errors(on_spark.to_document()) == []  # published shape
    for column in on_spark.columns:
        family, nulls, distinct, least, most = SAID[column.name]
        said = (column.type, column.nulls, column.distinct, column.min, column.max)
        assert said == (family, nulls, distinct, least, most), column.name
        assert column.distinct_exact is True


def test_a_timestamp_with_a_zone_is_spelled_in_utc_whatever_the_session_s_zone(
    spark, tmp_path
):
    path = parquet_of(FIXTURE, tmp_path)
    frame = spark.read.parquet(str(path))
    before = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
        profile = spark_profiler.profile_frame(frame, "orders")
    finally:
        spark.conf.set("spark.sql.session.timeZone", before)

    by = {column.name: column for column in profile.columns}
    # Measured: Spark hands a zoned timestamp back as wall time in the zone
    # the session started in, whatever the setting says now -- a profiler
    # reading the setting would have put the instant eleven and a half
    # hours out. The profile asks for the moment, and says ten in the
    # morning, in UTC.
    assert by["zoned"].min == by["chicago"].min == "2026-11-25T10:00:00"
    # A timestamp without a zone is taken as it stands.
    assert by["at"].min == "2026-01-01T00:00:00"
    assert arrow.profile_table(FIXTURE, "orders") == profile


def test_binary_and_text_report_their_lengths(spark, tmp_path):
    profile = spark_profiler.profile_frame(
        spark.read.parquet(str(parquet_of(FIXTURE, tmp_path))), "o"
    )
    by = {column.name: column for column in profile.columns}
    assert (by["name"].min_length, by["name"].max_length) == (0, 3)
    assert (by["blob"].min_length, by["blob"].max_length) == (0, 2)
    assert by["name"].min is None and by["blob"].max is None


def test_approximate_counts_say_so_and_are_right_on_a_small_table(spark, tmp_path):
    frame = spark.read.parquet(str(parquet_of(FIXTURE, tmp_path)))
    exact = spark_profiler.profile_frame(frame, "o")
    approximate = spark_profiler.profile_frame(frame, "o", approximate=True)
    assert all(column.distinct_exact is False for column in approximate.columns)
    assert all(column.distinct_exact is True for column in exact.columns)
    assert [column.distinct for column in approximate.columns] == [
        column.distinct for column in exact.columns
    ]


def test_an_empty_frame_and_an_all_null_column_have_nothing_to_bound(spark, tmp_path):
    frame = spark.read.parquet(str(parquet_of(FIXTURE, tmp_path)))
    empty = spark_profiler.profile_frame(frame.filter("false"), "o")
    assert empty.rows == 0
    assert all(
        column.nulls == 0 and column.min is None and column.max is None
        for column in empty.columns
    )
    all_null = spark.createDataFrame([(None,)] * 3, "x int")
    (column,) = spark_profiler.profile_frame(all_null, "o").columns
    assert (column.nulls, column.distinct, column.min, column.max) == (3, 0, None, None)


# --------------------------------------------------------------------- G2


def watch_the_driver(monkeypatch) -> list:
    """Intercept every way a DataFrame reaches the driver; record what came.

    Each call is recorded as ``(the method, the frame's columns, how many
    rows came)``.
    """
    from pyspark.sql import DataFrame

    seen = []

    def guarding(method):
        original = getattr(DataFrame, method)

        def guarded(self, *arguments, **options):
            result = original(self, *arguments, **options)
            if method == "toPandas":
                rows = len(result)
            elif method == "toLocalIterator":
                result = list(result)
                rows = len(result)
            else:
                rows = len(result)
            seen.append((method, tuple(self.columns), rows))
            return result

        return guarded

    for method in ("collect", "take", "head", "first", "toPandas", "toLocalIterator"):
        monkeypatch.setattr(DataFrame, method, guarding(method))
    return seen


def test_profiling_a_wide_table_brings_one_row_to_the_driver_and_no_column_of_the_table(
    spark, tmp_path, monkeypatch
):
    """With a sentinel column: the driver is given one row, and it holds none
    of the table's columns -- the sentinel among them."""
    path = parquet_of(wide_table(columns=60, rows=4000), tmp_path)
    frame = spark.read.parquet(str(path))
    assert "sentinel" in frame.columns  # the control: it is a column of the table
    seen = watch_the_driver(monkeypatch)

    profile = spark_profiler.profile_frame(frame, "wide")

    assert profile.rows == 4000 and len(profile.columns) == 60
    assert len(seen) == 1, seen
    method, columns, rows = seen[0]
    assert method == "collect" and rows == 1
    assert not set(columns) & set(frame.columns)


def test_the_watch_sees_a_raw_row_when_one_is_taken(spark, monkeypatch):
    """The control: the test above is not vacuous."""
    seen = watch_the_driver(monkeypatch)
    spark.range(5).collect()
    assert seen == [("collect", ("id",), 5)]


def test_a_frame_whose_aggregate_came_back_as_more_than_one_row_is_refused(
    spark, tmp_path, monkeypatch
):
    from pyspark.sql import DataFrame

    frame = spark.read.parquet(str(parquet_of(FIXTURE, tmp_path)))
    aggregate = frame.agg({"id": "max"}).union(frame.agg({"id": "min"}))
    monkeypatch.setattr(DataFrame, "collect", lambda self: [object(), object()])
    with pytest.raises(ConfigurationError, match="collected 2 rows, not one"):
        spark_profiler.profile_frame(aggregate, "o")


# --------------------------------------------------------- G3 and G4

#: What each engine needs installed to run its draft.
NEEDS = {
    "deequ": "pydeequ",
    "dqx": "databricks.labs.dqx",
    "greatexpectations": "great_expectations",
}


@pytest.mark.parametrize("target", sorted(NEEDS))
def test_each_engine_runs_its_draft_and_reports_every_rule_as_a_warning(
    spark, tmp_path, target
):
    pytest.importorskip(NEEDS[target], reason=f"the {target} engine needs its extra")
    draft = tmp_path / f"draft.{target}.conf"
    draft.write_text(
        suggest.render(
            suggest.suggest(arrow.profile_table(FIXTURE, "orders"), target), "hocon"
        )
    )
    assert validate_config(
        str(draft)
    )  # the registry's payload key, and the pack's shape

    (rule,) = load_config(str(draft))["dqframework"]["dqrules"]
    engine = EngineLoader().load_engine(target, rule, 1)
    results = engine.apply(spark.read.parquet(str(parquet_of(FIXTURE, tmp_path))))

    assert [result["check"] for result in results] == [
        "id_complete",
        "id_unique",
        "id_non_negative",
    ]
    assert all(result["success"] for result in results)
    assert [result["details"]["criticality"] for result in results] == ["warn"] * 3


def test_the_portable_draft_runs_on_spark_and_reports_every_rule_as_a_warning(
    spark, tmp_path
):
    draft = tmp_path / "draft.portable.conf"
    pack = suggest.suggest(
        arrow.profile_table(FIXTURE, "orders"), "portable", snapshot_sha256="a" * 64
    )
    draft.write_text(suggest.render(pack, "hocon"))
    assert validate_config(str(draft))
    rules = load_config(str(draft))["dqframework"]["dqrules"]
    frame = spark.read.parquet(str(parquet_of(FIXTURE, tmp_path)))
    said = []
    for rule in rules:
        engine = EngineLoader().load_engine("portable", rule, 1)
        said += engine.apply(frame)
    assert [result["check"] for result in said] == [
        "id_complete",
        "id_non_negative.low",
    ]
    assert all(result["success"] for result in said)
    assert [result["details"]["criticality"] for result in said] == ["warn"] * 2


@pytest.mark.parametrize("target", sorted(NEEDS))
def test_the_framework_runs_each_draft_and_a_failed_warning_blocks_nothing(
    spark, tmp_path, target
):
    """A draft as dq-suggest writes it, run the way a user runs a rule pack:
    the dataset registered under its name, over data that breaks it."""
    pytest.importorskip(NEEDS[target], reason=f"the {target} engine needs its extra")
    pack = suggest.suggest(arrow.profile_table(FIXTURE, "orders"), target)
    draft = tmp_path / f"draft.{target}.conf"
    draft.write_text(suggest.render(pack, "hocon"))
    broken = spark.read.parquet(
        str(parquet_of(with_a_broken_id(FIXTURE), tmp_path / "broken"))
    )
    broken.createOrReplaceTempView("orders")

    def verdicts(outcomes):
        return [
            (outcome["check"], outcome["success"], outcome["details"]["criticality"])
            for outcome in outcomes
        ]

    framework = DQFramework(spark, str(draft))
    results = framework.run()

    assert verdicts(results) == [
        ("id_complete", False, "warn"),
        ("id_unique", False, "warn"),
        ("id_non_negative", True, "warn"),
    ]
    decision = framework.last_gate_decision
    assert decision.policy == "error"  # the draft's own
    assert decision.passed is True
    assert decision.advisory == ("id_complete", "id_unique")
    assert decision.blocking == ()

    # The control, measured on the same engine: the same draft without the
    # policy it names runs under the default, "any", and stops the pipeline.
    without = json.loads(json.dumps(pack))
    del without["dqframework"]["gate"]
    other = tmp_path / f"draft-without-a-policy.{target}.json"
    other.write_text(json.dumps(without))
    control = DQFramework(spark, str(other))
    assert verdicts(control.run()) == verdicts(results)
    assert control.last_gate_decision.policy == "any"
    assert control.last_gate_decision.passed is False
    assert control.last_gate_decision.blocking == ("id_complete", "id_unique")


# --------------------------------------- a draft passes on its own data


@pytest.mark.parametrize("target", suggest.TARGETS)
def test_a_draft_passes_on_the_data_it_was_drawn_from(spark, tmp_path, target):
    """Every corner of dq.tests.profile_support.SMALL_TABLES at once. A
    suggestion the profile warrants must be confirmed by the target's own
    check on the very data the profile was drawn from, or the draft is not
    a draft of that data. Measured here first: the portable completeness
    check counts a not-a-number missing, and Deequ's refuses a nested
    column; both are left out of the draft for that vocabulary."""
    if target in NEEDS:
        pytest.importorskip(
            NEEDS[target], reason=f"the {target} engine needs its extra"
        )
    for rows, table in corner_frames().items():
        frame = spark.read.parquet(str(parquet_of(table, tmp_path / str(rows))))
        profile = spark_profiler.profile_frame(frame, "corners")
        assert profile == arrow.profile_table(table, "corners"), rows

        pack = suggest.suggest(profile, target, snapshot_sha256="a" * 64)
        said = []
        for rule in pack["dqframework"]["dqrules"]:
            engine = EngineLoader().load_engine(
                target, dict(rule, dataframes=["default"]), 1
            )
            said += engine.apply(frame)

        failed = [outcome["check"] for outcome in said if not outcome["success"]]
        assert failed == [], (rows, target, failed)
        assert bool(said) == (rows > 0), (rows, target, len(said))


# --------------------------------------------- names that are not identifiers


def test_the_profilers_agree_on_names_that_are_not_identifiers(spark, tmp_path):
    """A name with a dot, a backtick or a space, beside a struct whose field
    is spelled like the dotted one."""
    import pyarrow as pa

    table = pa.table(
        {
            "a.b": pa.array([1, 2], pa.int64()),
            "s.t": pa.array([7, 8], pa.int64()),
            "a`b": pa.array([3, 4], pa.int64()),
            "order id": pa.array(["x", "yy"]),
            "s": pa.array([{"a": 1}, {"a": 2}]),
        }
    )
    on_arrow = arrow.profile_table(table, "odd")
    on_spark = spark_profiler.profile_frame(
        spark.read.parquet(str(parquet_of(table, tmp_path))), "odd"
    )

    assert on_spark == on_arrow
    by = {column.name: column for column in on_spark.columns}
    assert (by["s.t"].min, by["s.t"].max) == (7, 8)  # not the struct's field
    assert (by["a.b"].min, by["a.b"].max) == (1, 2)
    assert (by["a`b"].min, by["a`b"].max) == (3, 4)
    assert (by["order id"].min_length, by["order id"].max_length) == (1, 2)
    assert by["s"].type == "nested" and by["s"].distinct is None


def test_no_draft_names_a_column_no_rule_can_ask_for(spark, tmp_path):
    """The profilers profile a column whatever it is called; a draft is
    written only for one a rule can ask for by name. The control is the
    kernel's: asked for a rule that names the other column, it refuses the
    rule when the engine is loaded, before any data is read."""
    import pyarrow as pa

    table = pa.table(
        {
            "a.b": pa.array([1, 2], pa.int64()),
            "order id": pa.array(["x", "yy"]),
            "id": pa.array([7, 8], pa.int64()),
        }
    )
    frame = spark.read.parquet(str(parquet_of(table, tmp_path)))
    profile = spark_profiler.profile_frame(frame, "orders")
    assert [column.name for column in profile.columns] == ["a.b", "order id", "id"]

    for target in suggest.TARGETS:
        rules = suggest.suggest(profile, target, snapshot_sha256="a" * 64)
        rules = rules["dqframework"]["dqrules"]
        written = json.dumps(rules)
        assert '"id"' in written, target  # the plainly named column is still said
        assert "a.b" not in written and "order id" not in written, target

    pack = suggest.suggest(profile, "portable", snapshot_sha256="a" * 64)
    rule = pack["dqframework"]["dqrules"][0]
    rule["plan"].append(
        {
            "constraint": "isComplete",
            "constraint_name": "a.b_complete",
            "column": "a.b",
            "level": "Warning",
        }
    )
    # For the portable engine and for every other alike (SPEC-025).
    with pytest.raises(
        ConfigurationError, match="Spark reads any other string as an expression"
    ):
        EngineLoader().load_engine("portable", rule, 1)


def test_a_frame_that_names_a_column_twice_is_refused_before_anything_is_measured(
    spark, monkeypatch
):
    frame = spark.createDataFrame([(1, 2, 3)], "id int, x int, id int")
    seen = watch_the_driver(monkeypatch)
    with pytest.raises(ConfigurationError, match=r"names a column twice: \['id'\]"):
        spark_profiler.profile_frame(frame, "t")
    assert seen == []


def test_an_exact_count_never_copies_a_row_once_per_column(
    spark, tmp_path, monkeypatch
):
    """Spark's own way with several distinct counts in one aggregate is an
    Expand node, which copies every row once per count and judges every
    count on every copy: measured, 8.5 seconds for 60 columns of 20,000
    rows, and no answer in five minutes for 240. The profiler's plan has
    none of it, at any width; the control shows the test can see one."""
    from pyspark.sql import DataFrame, functions as F

    plans = []
    collecting = DataFrame.collect

    def collected(self):
        plans.append(str(self._jdf.queryExecution().executedPlan()))
        return collecting(self)

    monkeypatch.setattr(DataFrame, "collect", collected)
    frame = spark.read.parquet(
        str(parquet_of(wide_table(columns=8, rows=50), tmp_path))
    )
    spark_profiler.profile_frame(frame, "wide")

    (plan,) = plans
    assert "Expand" not in plan

    del plans[:]
    frame.agg(F.countDistinct("c1"), F.countDistinct("c2")).collect()
    (control,) = plans
    assert "Expand" in control


def test_a_table_wider_than_one_branch_is_profiled_whole(spark, tmp_path):
    """A branch projects BRANCH columns at a time, to bound one expression's
    size: a table past that bound is profiled across several."""
    assert spark_profiler.BRANCH >= 100
    columns = 2 * spark_profiler.BRANCH + 50
    table = parquet_of(wide_table(columns=columns, rows=200), tmp_path)
    profile = spark_profiler.profile_frame(spark.read.parquet(str(table)), "wide")
    assert profile.rows == 200 and len(profile.columns) == columns
    assert all(
        column.max == 199 and column.distinct == 200 for column in profile.columns
    )


# ------------------------------------------- what the engines do not read alike


@pytest.mark.parametrize(
    ("values", "make", "on_arrow", "on_spark"),
    [
        ([None, None], lambda pa: pa.null(), "unknown", "integer"),
        ([0, 2**64 - 1], lambda pa: pa.uint64(), "integer", "decimal"),
        (
            [dt.timedelta(days=1), None],
            lambda pa: pa.duration("us"),
            "unknown",
            "integer",
        ),
    ],
    ids=["null-type", "unsigned-long", "duration"],
)
def test_what_the_engines_read_differently_is_outside_g1(
    spark, tmp_path, values, make, on_arrow, on_spark
):
    """Measured on Arrow 26 and Spark 3.5, through one Parquet file. Where
    the two profilers disagree it is the engines that read the file
    differently, and gate G1 does not reach: each profiler says what its
    own engine was handed."""
    import pyarrow as pa

    table = pa.table({"c": pa.array(values, make(pa))})
    (mine,) = arrow.profile_table(table, "t").columns
    (theirs,) = spark_profiler.profile_frame(
        spark.read.parquet(str(parquet_of(table, tmp_path))), "t"
    ).columns
    assert mine.type == on_arrow and theirs.type == on_spark


@pytest.mark.parametrize(
    ("values", "make", "on_arrow"),
    [
        ([1.5, None], lambda pa: pa.float16(), "float"),
        ([decimal.Decimal("1.5"), None], lambda pa: pa.decimal256(40, 3), "decimal"),
        ([dt.time(1), None], lambda pa: pa.time32("s"), "unknown"),
    ],
    ids=["half-precision", "decimal256", "time"],
)
def test_what_spark_cannot_read_is_profiled_by_arrow_alone(
    spark, tmp_path, values, make, on_arrow
):
    """Arrow profiles what it is given; Spark 3.5 refuses the file."""
    import pyarrow as pa

    table = pa.table({"c": pa.array(values, make(pa))})
    assert arrow.profile_table(table, "t").columns[0].type == on_arrow
    with pytest.raises(
        Exception
    ) as refused:  # noqa: B017 - Spark's own, whatever its shape
        spark.read.parquet(str(parquet_of(table, tmp_path))).schema
    assert type(refused.value).__module__.startswith(("pyspark", "py4j"))


# ------------------------------------------------- what Spark itself refuses


def test_dq_profile_reads_with_spark_as_it_does_with_arrow_and_leaves_the_session(
    spark, tmp_path
):
    data = parquet_of(FIXTURE, tmp_path)
    written = tmp_path / "orders.profile.json"
    code = profile_main(
        [
            "--dataset",
            f"orders={data}",
            "--profiler",
            "spark",
            "--profile",
            str(written),
        ]
    )
    assert code == 0
    profile = Profile.from_json(written.read_text())
    assert profile.source == "spark"
    assert profile == arrow.profile_table(FIXTURE, "orders")
    assert profile.snapshot_sha256 == snapshot_digest(data)
    assert spark.range(1).count() == 1  # a session it did not start is left


def test_data_spark_cannot_read_is_the_profiler_s_refusal(spark, tmp_path):
    corrupt = tmp_path / "corrupt.parquet"
    corrupt.write_bytes(b"not a parquet file" * 40)
    with pytest.raises(ConfigurationError) as refused:
        cli.profile_file(corrupt, "orders", "spark")
    said = str(refused.value)
    assert said.startswith(
        "dataset 'orders' could not be read with Spark: [CANNOT_READ_FILE_FOOTER] "
        "Could not read footer"
    )
    assert "\tat " not in said and len(said) <= 700  # bounded: no stack trace


def test_an_error_spark_states_by_class_is_a_refusal_too(spark, tmp_path, monkeypatch):
    """The other family Spark raises in: an error stated by its class."""
    from pyspark.errors import AnalysisException

    def refusing(frame, name, **options):
        raise AnalysisException(
            message="[PATH_NOT_FOUND] Path does not exist: file:/tmp/nope.parquet."
        )

    monkeypatch.setattr(spark_profiler, "profile_frame", refusing)
    data = parquet_of(FIXTURE, tmp_path)  # readable: the reading is not what fails
    with pytest.raises(ConfigurationError) as refused:
        cli.profile_file(data, "orders", "spark")
    assert str(refused.value) == (
        "dataset 'orders' could not be read with Spark: [PATH_NOT_FOUND] Path does "
        "not exist: file:/tmp/nope.parquet."
    )
    assert type(refused.value.__cause__).__name__ == "AnalysisException"


def test_anything_else_spark_lets_through_is_an_error_not_a_verdict(
    spark, tmp_path, monkeypatch
):
    """A defect of the profiler is not reported as the data's fault."""
    monkeypatch.setattr(
        spark_profiler, "profile_frame", lambda frame, name, **options: None.unwrap()
    )
    data = parquet_of(FIXTURE, tmp_path)  # readable: the reading is not what fails
    with pytest.raises(AttributeError):
        cli.profile_file(data, "orders", "spark")


# ------------------------------------------------------------- the user guide


@contextmanager
def jvm_working_directory(spark, path):
    """Hold Spark's working directory at ``path``, as a user's shell has it.

    Spark looks a relative path up against the JVM's working directory,
    which for a user is the directory their shell is in. The shared session
    started in the repository, so the guide's ``orders.parquet`` would be
    looked for there.
    """
    hadoop = spark._jvm.org.apache.hadoop.fs
    filesystem = hadoop.FileSystem.get(spark._jsc.hadoopConfiguration())
    before = filesystem.getWorkingDirectory()
    filesystem.setWorkingDirectory(hadoop.Path(str(path)))
    try:
        yield
    finally:
        filesystem.setWorkingDirectory(before)


def test_the_guide_s_way_of_running_an_engine_draft_runs(spark, tmp_path, monkeypatch):
    """The Python the user guide shows for running a draft is the Python that
    runs, with the draft dq-suggest writes and the session it is shown with."""
    import pyarrow.parquet as pq

    from dq.tests.test_documented_snippets import DOCS, python_block_after

    pytest.importorskip("pydeequ", reason="the deequ engine needs its extra")
    monkeypatch.chdir(tmp_path)
    with jvm_working_directory(spark, tmp_path):
        pq.write_table(FIXTURE, "orders.parquet")
        profile = tmp_path / "orders.profile.json"
        assert (
            profile_main(
                ["--dataset", "orders=orders.parquet", "--profile", str(profile)]
            )
            == 0
        )
        assert (
            suggest_main(
                [str(profile), "--engine", "deequ", "--output", "orders.deequ.conf"]
            )
            == 0
        )

        guide = (DOCS / "index.adoc").read_text(encoding="utf-8")
        source = python_block_after(guide, "rule pack, once the frame is registered:")
        namespace = {"spark": spark}
        exec(  # noqa: S102 - the repository's own document
            compile(source, "index.adoc: running a draft", "exec"), namespace
        )
    assert [result["check"] for result in namespace["results"]] == [
        "id_complete",
        "id_unique",
        "id_non_negative",
    ]
    assert namespace["failed"] == []


# ------------------------------------------ how dq-profile reads with Spark


def test_dq_profile_reads_a_directory_a_csv_and_a_json_with_spark(spark, tmp_path):
    import json as text
    import pyarrow.parquet as pq

    directory = tmp_path / "dataset"
    pq.write_to_dataset(FIXTURE, str(directory))
    (profile,) = [cli.profile_file(directory, "orders", "spark")]
    assert profile == arrow.profile_table(FIXTURE, "orders")  # read as Parquet

    csv = tmp_path / "orders.csv"
    csv.write_text("id,amount\n1,1.5\n2,2.5\n")
    said = cli.profile_file(csv, "orders", "spark")
    assert said.rows == 2 and [column.name for column in said.columns] == [
        "id",
        "amount",
    ]
    assert [column.type for column in said.columns] == ["integer", "float"]

    events = tmp_path / "events.json"
    events.write_text(
        text.dumps({"id": 1, "day": "2026-01-01"})
        + "\n"
        + text.dumps({"id": 2, "day": "2026-01-02"})
        + "\n"
    )
    said = cli.profile_file(events, "events", "spark")
    # Spark's JSON reader infers no date: a day written as text stays text.
    assert said.rows == 2
    assert {column.name: column.type for column in said.columns} == {
        "id": "integer",
        "day": "text",
    }


@pytest.mark.parametrize("name", ["orders.arrow", "orders", "orders.txt"])
def test_dq_profile_refuses_with_spark_what_it_cannot_read_by_extension(
    spark, tmp_path, name
):
    unreadable = tmp_path / name
    unreadable.write_bytes(b"anything")
    with pytest.raises(
        ConfigurationError, match="the Spark profiler reads Parquet, CSV or JSON"
    ):
        cli.profile_file(unreadable, "orders", "spark")


def test_a_session_the_command_started_is_stopped_after_it(
    spark, tmp_path, monkeypatch
):
    """A session that was there is left alone; one dq-profile started for
    itself is not left running after."""
    from types import SimpleNamespace

    started = SimpleNamespace(read=spark.read, stops=0)

    def stop():
        started.stops += 1

    started.stop = stop
    active = []

    class Session:
        builder = SimpleNamespace(
            appName=lambda name: Session.builder,
            getOrCreate=lambda: (active.append(started), started)[1],
        )

        @staticmethod
        def getActiveSession():
            return active[0] if active else None

    real = spark_profiler.session_class
    monkeypatch.setattr(spark_profiler, "session_class", lambda: Session)
    assert cli.profile_file(parquet_of(FIXTURE, tmp_path), "orders", "spark").rows == 8
    assert started.stops == 1

    # And a session that was there before is the user's to stop.
    monkeypatch.setattr(spark_profiler, "session_class", real)
    assert cli.profile_file(parquet_of(FIXTURE, tmp_path), "orders", "spark").rows == 8
    assert spark.range(1).count() == 1


def test_types_only_spark_has_are_typed_as_the_profile_knows_them(spark):
    """An interval and a void have no family the profile knows: typed
    ``unknown``, asked only for their nulls, and nothing fails."""
    frame = spark.sql(
        "SELECT INTERVAL 1 DAY AS span, CAST(NULL AS VOID) AS nothing, "
        "CAST('ab' AS CHAR(5)) AS spelled"
    )
    profile = spark_profiler.profile_frame(frame, "t")
    assert [
        (column.type, column.nulls, column.distinct) for column in profile.columns
    ] == [
        ("unknown", 0, None),
        ("unknown", 1, None),
        ("text", 0, 1),
    ]
