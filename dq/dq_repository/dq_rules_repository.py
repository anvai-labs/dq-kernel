# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0


class DQRulesRepository:
    def __init__(self, repo_bucket_name):
        self.repo_bucket_name = repo_bucket_name

    # store DQ rules in S3 table
    def store_dq_rules(self, dq_engine_type, dataset_name, df):
        file_path = f"s3://{self.repo_bucket_name}/{dataset_name}/{dq_engine_type}/dq_rules.json"
        df.coalesce(1).write.format("json").save(file_path)
        return 0

    def retrieve_dq_rules(self, dq_engine_type, dataset_name):
        file_path = f"s3://{self.repo_bucket_name}/{dataset_name}/{dq_engine_type}/dq_rules.json"
        # df =
        df = None
        return df
