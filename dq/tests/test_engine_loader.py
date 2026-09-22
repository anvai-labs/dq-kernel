# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from dq.engine.engine_loader import EngineLoader
from dq.engine.dq_engine import DQEngine
from pyhocon import ConfigFactory
import pytest
from dq.utils import constants


@pytest.fixture
def dqrule_schemavalidation_config():
    return ConfigFactory.parse_string("""{
            name = "dataset-rules-000"
            engine = "schemavalidation"
            schema = {
                catalog_type = "glue"
                table = "temp_table_1"
                unique_key_constraints = [["name","age"], ["name","email"]]
                foreign_key_constraints = [
                    {
                        src_column = "name"
                        ref_table = "temp_users"
                        ref_column = "name"
                    },
                    {
                        src_column = "email"
                        ref_table = "temp_emails"
                        ref_column = "email"
                    }
                ]
            }
        }
        """)


@pytest.fixture
def dqrule_deequ_config():
    return ConfigFactory.parse_string("""{
            name = "dataset-rules-001"
            engine = "deequ"
            async = true
            checks = [
                {
                    alias = "check-001"
                    constraint = "isUnique"
                    column = "id"
                    level = "Error"
                    description = "Check that ID is unique"
                },
                {
                    alias = "check-002"
                    constraint = "isComplete"
                    column = "name"
                    level = "Error"
                    description = "Check that name is not null"
                }
            ]
        }
        """)


@pytest.fixture
def dqrule_greatexpectations_config():
    return ConfigFactory.parse_string("""
    {
        name = "dataset-rule-002"
        engine = "greatexpectations"
        async = true
        expectations = [
            {
                type = "expect_column_values_to_be_unique"
                column = "id"
            },
            {
                type = "expect_column_values_to_not_be_null"
                column = "name"
            },
            {
                type = "expect_column_values_to_match_regex"
                column = "name"
                kwargs = {
                    regex = "^[A-Za-z]+$"
                }
            }
        ]
    }
    """)


@pytest.fixture
def dqrule_notdefined_config():
    return ConfigFactory.parse_string("""
    {
        name = "dataset-rule-002"
        engine = "unknown"
        somekey = [
            {
                alias= "irrelevant"
            }
        ]
    }
    """)


def process_engine_load_with_config(config):
    return EngineLoader().load_engine(
        config.get(constants.DQ_ENGINE_NAME, None), config
    )


def test_deequ_engine_load_success(dqrule_deequ_config):
    engine = process_engine_load_with_config(config=dqrule_deequ_config)
    assert isinstance(engine, DQEngine)


def test_greatexpectations_engine_load_success(dqrule_greatexpectations_config):
    engine = process_engine_load_with_config(config=dqrule_greatexpectations_config)
    assert isinstance(engine, DQEngine)


def test_schemavalidation_engine_load_success(dqrule_schemavalidation_config):
    engine = process_engine_load_with_config(config=dqrule_schemavalidation_config)
    assert isinstance(engine, DQEngine)


def test_notdefined_engine_load_exception(dqrule_notdefined_config):
    exceptionOccured = False
    importerror = False
    attributeerror = False

    try:
        engine = process_engine_load_with_config(config=dqrule_notdefined_config)
    except ImportError as e:
        exceptionOccured = True
        importerror = True
        print(e)
    except AttributeError as e:
        exceptionOccured = True
        attributeerror = True
        print(e)
    except Exception as e:
        exceptionOccured = True
        print(e)
    else:
        print("Non Exception Occured")
    finally:
        assert True == exceptionOccured
        assert True == (importerror or attributeerror)
