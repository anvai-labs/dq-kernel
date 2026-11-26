# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""The former name of the durable sink, kept for existing imports (SPEC-007).

The PostgreSQL sink is :class:`dq.sqlalchemy_sink.SqlAlchemyOutcomeSink`,
which takes a SQLAlchemy ``Engine`` or ``Connection`` instead of a DB-API
connect callable. ``PostgresOutcomeSink`` is that class under its old name,
and ``DEFAULT_DSN_VARIABLE`` is the same setting.
"""

from dq.sqlalchemy_sink import DEFAULT_DSN_VARIABLE, SqlAlchemyOutcomeSink

#: The durable sink under the name it had before SPEC-007.
PostgresOutcomeSink = SqlAlchemyOutcomeSink

__all__ = ["DEFAULT_DSN_VARIABLE", "PostgresOutcomeSink", "SqlAlchemyOutcomeSink"]
