# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""An OpenTelemetry observer wired to the SDK's in-memory providers (SPEC-014).

Nothing leaves the process: spans end in an in-memory exporter and metrics
are read by an in-memory reader. That is what lets the adapter be exercised
through the real API in a test, with no export anywhere.
"""

from typing import NamedTuple

import pytest


class Observatory(NamedTuple):
    """An observer built through the real API, and where it records."""

    observer: object
    exporter: object
    reader: object
    meter_provider: object

    @property
    def spans(self) -> list:
        """The spans ended so far, in the order they ended."""
        return list(self.exporter.get_finished_spans())

    def named(self, name: str) -> list:
        """The ended spans called ``name``, in the order they ended."""
        return [span for span in self.spans if span.name == name]

    def outcomes_counted(self, counter: str) -> dict:
        """Return ``counter``'s points: each attribute set mapped to its value."""
        self.meter_provider.force_flush()
        data = self.reader.get_metrics_data()
        if data is None:  # nothing was ever counted
            return {}
        return {
            tuple(sorted(point.attributes.items())): point.value
            for resource in data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == counter
            for point in metric.data.data_points
        }


def in_memory_observatory(instrumentation: str = "test") -> Observatory:
    """Build an :class:`Observatory` through the real API, recording in memory.

    Skips the test when the OpenTelemetry SDK is not installed.
    """
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    from dq.observability import OpenTelemetryObserver

    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    observer = OpenTelemetryObserver(
        tracer=tracer_provider.get_tracer(instrumentation),
        meter=meter_provider.get_meter(instrumentation),
    )
    return Observatory(observer, exporter, reader, meter_provider)
