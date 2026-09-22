# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

# pyspark_job.py

from pyspark.sql import SparkSession
from dq.dq_framework import DQFramework
import pydeequ


def main():
    # Initialize PySpark
    spark = (
        SparkSession.builder
        # .config("spark.driver.extraClassPath", classpath)
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .config("spark.driver.memory", "5g")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .appName("ExampleSparkDQJob")
        .getOrCreate()
    )

    print(pydeequ.deequ_maven_coord, pydeequ.f2j_maven_coord)

    # Create a sample dataframe
    data = [
        (1, "Alice", 34, "alice@example.com"),
        (2, "Bob", 45, "bob@example.com"),
        (3, "Charlie", 29, "charlie@example.com"),
        (4, "Delta", 18, "delta/78@notvalid"),
    ]

    df = spark.createDataFrame(data, ["id", "name", "age", "email"])
    df.createOrReplaceTempView("temp_table_1")
    temp_users_df = spark.sql("SELECT name  FROM temp_table_1")
    temp_emails_df = spark.sql("SELECT email FROM temp_table_1")
    temp_users_df.createOrReplaceTempView("temp_users")
    temp_emails_df.createOrReplaceTempView("temp_emails")

    # Inline HOCON configuration
    config = "file://./dqframework/tests/resources/example.conf"
    # Initialize Data Quality Framework
    dq_framework = DQFramework(spark, config, df)

    # Run the framework
    output = dq_framework.run()

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()
