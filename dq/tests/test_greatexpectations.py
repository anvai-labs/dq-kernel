# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

# import pytest
# from unittest.mock import patch, MagicMock
# from pyspark.sql import SparkSession
# from pyhocon import ConfigFactory
# # import great_expectations as ge
# from dq.engine.greatexpectations.greatexpectations_engine import GreatexpectationsEngine
# from dq.engine.greatexpectations.greatexpectations_check import GreatexpectationsCheck
# import json

# @pytest.fixture
# def sample_dataframe(spark):
#     data = [("Alice", 34, "alice@example.com"), ("Bob", 45, "bob@example.com"), ("Catherine", 49, "cathy@example.com")]
#     columns = ["name", "age", "email"]
#     return spark.createDataFrame(data, columns)


# @pytest.fixture
# def great_expectations_config():
#     config_str = """
#     greatexpectations {
#         expectations = [
#             {
#                 type = "expect_column_values_to_be_unique"
#                 column = "name"
#             },
#             {
#                 type = "expect_column_values_to_not_be_null"
#                 column = "age"
#             }
#         ]
#     }
#     """
#     return ConfigFactory.parse_string(config_str).get("greatexpectations")

# def test_great_expectations_engine(sample_dataframe, great_expectations_config):
#     engine = GreatexpectationsEngine(great_expectations_config)

#     summarymetrics = engine.apply(sample_dataframe, repository = None)

#     overallsuccess = True
#     for metric in summarymetrics:
#         if not(metric['success']):
#             print("metric error: " + json.dumps(metric))
#             overallsuccess = False

#     assert overallsuccess == True , "Atleast one expectation failed."
