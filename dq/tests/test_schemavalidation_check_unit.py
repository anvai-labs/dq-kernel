# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""JVM-free unit contracts for SchemavalidationCheck helpers."""

import pytest

from dq.engine.schemavalidation.schemavalidation_check import SchemavalidationCheck


def _check():
    return SchemavalidationCheck(schema_config={}, single_check_mode=True)


@pytest.mark.parametrize("db", ["", "   "], ids=["empty", "blank"])
def test_fulltable_treats_blank_database_as_unqualified(db):
    assert _check()._get_fulltable(db=db, tbl="orders") == "orders"


def test_fulltable_qualifies_table_with_database():
    assert _check()._get_fulltable(db="sales", tbl="orders") == "sales.orders"


def test_fulltable_without_database_stays_unqualified():
    assert _check()._get_fulltable(db=None, tbl="orders") == "orders"
