# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The Spark profiler (SPEC-019).

Sums up a Spark DataFrame as a :class:`dq.profile.profile.Profile`, importing
no pyspark until it is used. One job, and one row back: that row holds the
row count and every column's measures, and is all the driver ever sees of
the data.

What the profile says is fixed in :mod:`dq.profile.profile`. Four things
need doing here rather than saying.

* *A column is asked for by its name* (:func:`_addressed`), in backticks.
* *An exact distinct count is not asked for in one aggregate.* Measured on
  Spark 3.5: ``agg(count(Distinct a), count(Distinct b), ..)`` answers N
  distinct counts by copying every row N + 1 times and judging all N
  counts on every copy -- 8.5 seconds for 60 columns of 20,000 rows, and
  no answer in five minutes for 240. Instead every value is laid beside its
  column's number, counted once per ``(column, value)`` and once per
  column, in a plan that grows with the column count: the same 60 columns
  in under a second, and 1,000 in seven.
* *A not-a-number is kept out of a float column's bounds*, which Spark's
  own ``min`` and ``max`` do not do.
* *A timestamp with a zone is asked for as the moment itself*, in
  microseconds since the epoch: PySpark hands such a timestamp back as
  wall time in a zone of its own choosing -- measured, not the session's
  current setting -- and the profile wants the moment, in UTC.
"""

from __future__ import annotations

from functools import reduce
from typing import Any, Dict, List, Optional, Tuple

from dq.exceptions import ConfigurationError
from dq.profile.profile import (
    BINARY,
    BOUNDED,
    BOOLEAN,
    DATE,
    DECIMAL,
    FLOAT,
    INTEGER,
    LENGTHENED,
    NESTED,
    TEXT,
    TIMESTAMP,
    UNKNOWN,
    Column,
    Profile,
    moment,
    named_twice,
    spell,
)

#: The supported Spark line, and what the extra is for.
_EXTRA = ("pyspark>=3.5,<3.6", "profiler")

#: The alias of the row count: no table column is asked for under a name
#: like it.
_ROWS = "__dq_profile_rows__"

#: What the columns counted with one another are gathered under, as one
#: sorted array; one such alias per type.
_SUMMARY = "__dq_profile_summary_{}__"

#: What the branches of the one job are joined on: each holds one row.
_ON = "__dq_profile_on__"

#: How many columns one branch projects at a time, so that the size of one
#: expression is bounded by this and not by the table's width.
BRANCH = 200

#: What a column's values are cast to, to lie beside other columns of its
#: family without a value lost: every integer in a long, every float in a
#: double, every text in a string.
_CASTS = {INTEGER: "long", FLOAT: "double", TEXT: "string"}

#: The measures a column can be asked for, as the profile spells them.
_MEASURES = (
    "present",
    "distinct",
    "not_a_number",
    "min",
    "max",
    "min_length",
    "max_length",
)


def _spark():
    """Return ``(pyspark.sql.functions, pyspark.sql.types)``, or name the extra."""
    from dq.utils.imports import require_extra

    pin, kind = _EXTRA
    functions = require_extra(
        "pyspark.sql.functions", "pyspark", "spark", pin=pin, kind=kind
    )
    types = require_extra("pyspark.sql.types", "pyspark", "spark", pin=pin, kind=kind)
    return functions, types


def session_class():
    """Return ``pyspark.sql.SparkSession``, or name the extra that provides it.

    For a command that starts a session of its own, as ``dq-profile`` does.
    """
    from dq.utils.imports import require_extra

    pin, kind = _EXTRA
    return require_extra(
        "pyspark.sql.session", "pyspark", "spark", pin=pin, kind=kind
    ).SparkSession


def own_exceptions() -> tuple:
    """Return what Spark raises when it cannot do what was asked of it.

    For a caller that reports a dataset Spark could not read as a refusal
    rather than as an error: anything else Spark let through is not Spark's
    verdict on the data.
    """
    import py4j.protocol

    from dq.utils.imports import require_extra

    pin, kind = _EXTRA
    errors = require_extra("pyspark.errors", "pyspark", "spark", pin=pin, kind=kind)
    return (errors.PySparkException, py4j.protocol.Py4JError)


def _addressed(name: str) -> str:
    """Return a column's name as Spark resolves it, and nothing else.

    Measured on Spark 3.5: ``col("s.t")`` names the field ``t`` of a struct
    column ``s`` where the frame has one -- and measures that field, without
    a word, beside a column actually called ``s.t`` -- and names nothing at
    all where the frame has not. In backticks a name is the column and only
    the column; a backtick inside a name is written twice.
    """
    return "`" + name.replace("`", "``") + "`"


def family_of(data_type, types) -> tuple:
    """Return a Spark type's ``(family, precision, scale)`` in the profile's words.

    Args:
        data_type: A ``pyspark.sql.types.DataType``.
        types: The ``pyspark.sql.types`` module.

    Returns:
        The type family -- one of ``profile.FAMILIES`` -- and, for a
        decimal, its precision and scale; ``None`` for both otherwise.
    """
    if isinstance(data_type, types.IntegralType):
        return INTEGER, None, None
    if isinstance(data_type, (types.FloatType, types.DoubleType)):
        return FLOAT, None, None
    if isinstance(data_type, types.DecimalType):
        return DECIMAL, data_type.precision, data_type.scale
    if isinstance(data_type, types.StringType):
        return TEXT, None, None
    if isinstance(data_type, types.BinaryType):
        return BINARY, None, None
    if isinstance(data_type, types.BooleanType):
        return BOOLEAN, None, None
    if isinstance(data_type, types.DateType):
        return DATE, None, None
    if isinstance(data_type, (types.TimestampType, *_without_zone(types))):
        return TIMESTAMP, None, None
    if isinstance(data_type, (types.ArrayType, types.MapType, types.StructType)):
        return NESTED, None, None
    return UNKNOWN, None, None


def _without_zone(types) -> tuple:
    """Return the type of a timestamp without a zone, where Spark has it."""
    without = getattr(types, "TimestampNTZType", None)
    return (without,) if without is not None else ()


def profile_frame(
    frame,
    name: str,
    *,
    approximate: bool = False,
    snapshot_sha256: Optional[str] = None,
) -> Profile:
    """Return the profile of a Spark DataFrame, in one job.

    Args:
        frame: A ``pyspark.sql.DataFrame``.
        name: The dataset's name.
        approximate: To count distinct values approximately
            (``approx_count_distinct``) rather than exactly: lighter by a
            shuffle, for a table too wide or too cardinal for an exact
            count.
        snapshot_sha256: The digest of the bytes the frame was read from,
            where the caller read them; ``None`` when there is none.

    Returns:
        The :class:`Profile`, its ``source`` being ``spark``, and every
        distinct count exact unless ``approximate`` was asked for.

    Raises:
        ImportError: When pyspark is not installed, naming the extra.
        ConfigurationError: When the frame names a column twice, or when a
            job does not come back as one row.
    """
    functions, types = _spark()
    twice = named_twice([field.name for field in frame.schema.fields])
    if twice:
        raise ConfigurationError(f"the frame names a column twice: {twice}")
    plans = [
        _Plan(index, field, types) for index, field in enumerate(frame.schema.fields)
    ]
    measured, rows = _measured(frame, functions, name, plans, approximate)
    columns = tuple(
        _summarised(plan, rows, measured.get(plan.index, {}), exact=not approximate)
        for plan in plans
    )
    return Profile(
        dataset=name,
        snapshot_sha256=snapshot_sha256,
        rows=rows,
        columns=columns,
        source="spark",
    )


class _Plan:
    """One column: its family in the profile's words, and what to ask of it."""

    def __init__(self, index: int, field, types):
        self.index = index
        self.field = field
        self.family, self.precision, self.scale = family_of(field.dataType, types)
        #: A timestamp with a zone is asked for as an instant.
        self.zoned = self.family == TIMESTAMP and isinstance(
            field.dataType, types.TimestampType
        )
        self.cast = _CASTS.get(self.family, None)

    @property
    def key(self) -> str:
        """The prefix of the column's aliases in a row of aliases."""
        return f"__dq_profile_c{self.index}__"

    @property
    def asked(self) -> bool:
        """Whether more than its nulls is asked of the column.

        Nothing more is asked of a nested column or of one of a type the
        profile does not know: either has a value the profile has no
        measure for.
        """
        return self.family not in (NESTED, UNKNOWN)

    @property
    def group(self) -> tuple:
        """The columns it can lie beside without a value lost.

        Those of its family where a cast to the family's widest type loses
        nothing; otherwise those of its own type, a decimal's scale and a
        timestamp's zone among them.
        """
        if self.cast:
            return self.family, self.cast
        return self.field.dataType.simpleString(), None


# ------------------------------------------------------------------- the job


def _measured(
    frame, functions, name: str, plans: List[_Plan], approximate: bool
) -> Tuple[Dict[int, Dict[str, Any]], int]:
    """Return each column's measures by its number, and the row count.

    One job, whose branches each end in one row -- the row count with the
    present counts of the columns nothing more is asked of, and one row per
    type columns are counted with -- joined into the one row the driver
    collects.
    """
    only_nulls = [plan for plan in plans if not plan.asked]
    counted = frame.agg(
        functions.count(functions.lit(1)).alias(_ROWS),
        *[
            functions.count(_column(functions, plan)).alias(plan.key + "present")
            for plan in only_nulls
        ],
    ).withColumn(_ON, functions.lit(1))
    groups = _grouped(plans)
    for position, (_, group) in enumerate(groups):
        summary = _summary(
            frame, functions, group, _SUMMARY.format(position), approximate
        )
        counted = counted.join(summary.withColumn(_ON, functions.lit(1)), on=_ON)
    the = _one(counted.drop(_ON).collect(), name)

    measured = {
        plan.index: {"present": the[plan.key + "present"]} for plan in only_nulls
    }
    for position, _ in enumerate(groups):
        for entry in the[_SUMMARY.format(position)]:
            measured[entry["k"]] = entry.asDict()
    return measured, the[_ROWS]


def _grouped(plans: List[_Plan]) -> List[Tuple[tuple, List[_Plan]]]:
    """Return the columns more than nulls is asked of, by what they are counted with."""
    groups: Dict[tuple, List[_Plan]] = {}
    for plan in plans:
        if plan.asked:
            groups.setdefault(plan.group, []).append(plan)
    return list(groups.items())


def _column(functions, plan: _Plan):
    return functions.col(_addressed(plan.field.name))


def _summary(frame, functions, group: List[_Plan], alias: str, approximate: bool):
    """Return a one-row frame holding, under ``alias``, the group's measures.

    Every value is laid beside its column's number, so that columns of one
    type are counted together, and no aggregate takes a distinct: no row is
    copied a column's count of times. Counted exactly, a value is then
    counted once per ``(column, value)`` -- which is what a distinct count
    is -- and once per column; approximately, a sketch of each column's
    values is merged, and the shuffle carries sketches, not values. What
    comes back is one sorted array of one struct per column: its number,
    and its measures as the profile spells them.
    """
    family = group[0].family
    pairs = _pairs(frame, functions, group)
    value = functions.col("v")
    if approximate:
        present, distinct = functions.count(value), functions.approx_count_distinct(
            value
        )
        not_a_number = functions.count(functions.when(functions.isnan(value), 1))
    else:
        pairs = pairs.groupBy("k", "v").agg(
            functions.count(functions.lit(1)).alias("times")
        )
        times = functions.col("times")
        present = functions.sum(functions.when(value.isNotNull(), times).otherwise(0))
        distinct = functions.count(value)
        not_a_number = functions.sum(
            functions.when(functions.isnan(value), times).otherwise(0)
        )
    measures: List[Tuple[str, Any]] = [("present", present), ("distinct", distinct)]
    if family == FLOAT:
        measures.append(("not_a_number", not_a_number))
    if family in BOUNDED:
        ordered = value
        if family == FLOAT:
            # A not-a-number is a value, never a bound.
            ordered = functions.when(functions.isnan(value), None).otherwise(value)
        least, most = functions.min(ordered), functions.max(ordered)
        if group[0].zoned:
            # The moment itself, which no zone can spell two ways.
            least, most = functions.unix_micros(least), functions.unix_micros(most)
        measures += [("min", least), ("max", most)]
    if family in LENGTHENED:
        lengths = functions.length(value)
        measures += [
            ("min_length", functions.min(lengths)),
            ("max_length", functions.max(lengths)),
        ]
    per_column = pairs.groupBy("k").agg(
        *[column.alias(said) for said, column in measures]
    )
    # Gathered by the names the per-column aggregate gave its measures.
    gathered = functions.sort_array(
        functions.collect_list(functions.struct("k", *[said for said, _ in measures]))
    )
    return per_column.agg(gathered.alias(alias))


def _pairs(frame, functions, group: List[_Plan]):
    """Return ``(k, v)`` rows: every value of the group's columns beside its
    column's number, a null value included."""
    branches = []
    for start in range(0, len(group), BRANCH):
        elements = []
        for plan in group[start : start + BRANCH]:
            value = _column(functions, plan)
            if plan.cast:
                value = value.cast(plan.cast)
            elements.append(
                functions.struct(functions.lit(plan.index).alias("k"), value.alias("v"))
            )
        branches.append(
            frame.select(
                functions.explode(functions.array(*elements)).alias("pair")
            ).select(
                functions.col("pair.k").alias("k"), functions.col("pair.v").alias("v")
            )
        )
    return reduce(lambda left, right: left.union(right), branches)


# ------------------------------------------------------------- what comes back


def _one(collected: list, name: str) -> Dict[str, Any]:
    """Return the one row a job came back with, as a mapping, or refuse."""
    if len(collected) != 1:
        raise ConfigurationError(
            f"profiling {name!r} collected {len(collected)} rows, not one: "
            "the driver must never see the data"
        )
    return collected[0].asDict()


def _summarised(
    plan: _Plan, rows: int, measures: Dict[str, Any], *, exact: bool
) -> Column:
    """Return one column's summary from the measures its job brought back.

    A column the job said nothing of has no measures: an empty frame, where
    every count is zero and nothing has a bound.
    """
    said = dict(
        min=None,
        max=None,
        min_length=None,
        max_length=None,
        precision=plan.precision,
        scale=plan.scale,
        not_a_number=None,
    )
    if plan.family == FLOAT:
        said["not_a_number"] = int(measures.get("not_a_number") or 0)
    if plan.family in BOUNDED:
        low, high = measures.get("min"), measures.get("max")
        if plan.zoned:
            low, high = moment(low), moment(high)
        said["min"], said["max"] = spell(low, plan.family), spell(high, plan.family)
    if plan.family in LENGTHENED:
        said["min_length"] = measures.get("min_length")
        said["max_length"] = measures.get("max_length")
    # A column nothing came back of is one of an empty frame: no value, so
    # none distinct.
    distinct = measures.get("distinct", 0)
    return Column(
        plan.field.name,
        plan.family,
        rows - int(measures.get("present") or 0),
        None if not plan.asked else int(distinct),
        exact,
        **said,
    )
