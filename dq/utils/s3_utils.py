# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import boto3


class S3Utils:
    def __init__(self):
        self.s3 = boto3.client("s3")

    def write_to_s3(self, data, bucket, key):
        """Write data to S3"""
        self.s3.put_object(Body=data, Bucket=bucket, Key=key)

    # function to read from S3
    def read_from_s3(self, bucket, key):
        """Read data from S3"""
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")

    # function to list files in S3 folder
    def list_s3_files(self, bucket, prefix):
        """List files in S3 folder"""
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        files = []
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(obj["Key"])
        return files
