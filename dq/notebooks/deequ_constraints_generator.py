# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

# Databricks notebook source
import os

os.environ["SPARK_VERSION"] = "3.5"
dbutils.widgets.text("domain_name", "my_domain", "domain_name")
dbutils.widgets.text("dataset_name", "my_dataset", "dataset_name")
dbutils.widgets.text("dq_repository_path", "/tmp/dq_output", "dq_repository_path")
dbutils.widgets.text("dq_metrics_table", "my_domain_dq.dq_metrics", "dq_metrics_table")
dq_repository_path = dbutils.widgets.get("dq_repository_path")
domain_name = dbutils.widgets.get("domain_name")
dataset_name = dbutils.widgets.get("dataset_name")
dq_metrics_table = dbutils.widgets.get("dq_metrics_table")


# COMMAND ----------

from dq.engine.deequ.deequ_constraints_builder import DeequConstraintsBuilder
from dq.dq_framework import DQFramework

deequ_constraints_builder = DeequConstraintsBuilder()

# update the query to pick the correct dataset for analysis, in this case the whole table will be used for analysis
sql = f"select * from {domain_name}.{dataset_name}"

df = spark.sql(sql)


constraints = deequ_constraints_builder.build_constraints(spark, df)
# print(constraints)
file_name = deequ_constraints_builder.save_in_hocon_format(
    constraints,
    dataset_name,
    dq_metrics_table,
    f"{dq_repository_path}/{dataset_name}.conf",
)

# file_name = "/tmp/dq_output/Testfile-1.conf"
conf_file_tmp = f"file://{file_name}"


dqf = DQFramework(spark, conf_file_tmp, df)
cum_metris = dqf.run()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_domain_dq.dq_metrics
# MAGIC where name not like 'Histogram%';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_domain_dq.dq_metrics
# MAGIC where name not like 'Histogram%' and
# MAGIC dataset = 'my_dataset';
