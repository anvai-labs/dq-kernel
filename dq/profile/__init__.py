# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Profiling and rule suggestion (SPEC-019).

A profile is a bounded summary of a dataset -- strict JSON, one entry per
column -- written by a profiler over Spark or Arrow and read by suggesters
that turn it into a draft rule pack. Nothing here is applied to a pipeline:
every suggested rule is a warning until a human promotes it.
"""
