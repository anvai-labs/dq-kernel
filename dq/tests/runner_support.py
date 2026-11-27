# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Files and rule packs for the Spark-free runner's tests (SPEC-015)."""

import decimal
from pathlib import Path

SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
OTHER_SHA = "b" * 64

#: One completeness check at Error level: it fails on the table ``orders_table``
#: builds, where one id in four is null.
COMPLETE = (
    '{ constraint = "isComplete", constraint_name = "order_id_complete", '
    'column = "id", level = "Error" }'
)
SIZE = (
    '{ constraint = "hasSize", constraint_name = "orders_present", '
    'assertion = "lambda size: size >= 1", level = "Warning" }'
)


def orders_table():
    """Four orders, one without an id: the completeness check fails on it."""
    import pyarrow as pa

    return pa.table(
        {
            "id": ["a", None, "c", "d"],
            "amount": pa.array(
                [
                    decimal.Decimal("1.25"),
                    None,
                    decimal.Decimal("3.50"),
                    decimal.Decimal("-0.50"),
                ],
                pa.decimal128(10, 2),
            ),
        }
    )


def write_parquet(table, path):
    import pyarrow.parquet as pq

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def write_arrow(table, path):
    """Write a table as an Arrow IPC *file* (``.arrow``, ``.feather``, ``.ipc``)."""
    import pyarrow as pa

    path = Path(path)
    with pa.OSFile(str(path), "wb") as sink:
        with pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
    return path


def write_orders(directory, name="orders.parquet"):
    return write_parquet(orders_table(), Path(directory) / name)


def rule(
    name="orders_counts",
    *,
    engine="portable",
    adapter='"arrow"',
    dataframes=("orders",),
    dataset="orders",
    sha=SHA,
    plan=None,
    extra="",
):
    """One rule as HOCON text. ``adapter`` is the value as HOCON writes it."""
    where = ", ".join(f'"{frame}"' for frame in dataframes)
    adapter_line = f"adapter = {adapter}" if adapter is not None else ""
    return f"""
    {{
      name = "{name}"
      engine = "{engine}"
      dataframes = [{where}]
      dataset = "{dataset}"
      snapshot_sha256 = "{sha}"
      {adapter_line}
      plan = [{plan if plan is not None else COMPLETE + ", " + SIZE}]
      {extra}
    }}"""


def rule_pack(*rules, dataset=None, **settings):
    """A rule pack as HOCON text. ``settings`` maps a dotted key to its HOCON value."""
    lines = ["dqframework {"]
    if dataset is not None:
        lines.append(f'  dataset = "{dataset}"')
    for key, value in settings.items():
        lines.append(f"  {key} = {value}")
    lines.append("  dqrules = [")
    lines.extend(rules)
    lines.extend(["  ]", "}"])
    return "\n".join(lines) + "\n"


def write_pack(directory, text, name="rules.conf"):
    path = Path(directory) / name
    path.write_text(text)
    return path
