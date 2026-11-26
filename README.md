# dq-kernel

Certifiable data-quality evidence engine for lakehouse pipelines. Semantically
versioned rules, exact-arithmetic evaluation, and machine-checkable evidence —
engine-agnostic by design.

## Install

```bash
pip install dq-kernel[spark]
```

Requires Python 3.12/3.13, Java 17, and a Spark 3.5.9 runtime.

Evidence can be kept in a SQL database instead of files. That sink and its Arrow path are
optional extras: `pip install dq-kernel[sqlalchemy,arrow]`.

Evidence can also come from other tools, without a Spark session: `dq.harvest` imports dbt
test results, Great Expectations validation results, or a four-column CSV or JSON file, and
`dq-import` applies the gate to them and writes the same `dq-report/v1`.

## Quick start

```python
from dq.dq_framework import DQFramework

results = DQFramework(spark, config, default_dataframe=df).run()
```

Each result carries `check`, `success`, and bounded `details`. Evidence is
bound to dataset and rule-set digests as `dq-report/v1`.

A run explains itself: with nothing configured it is logged, one line per
notification, naming the run and each rule (`DQ_LOG_FORMAT=json` for JSON
lines). `dq.observability` is the port, with an OpenTelemetry adapter behind
the `otel` extra.

## Three semantic versions

| Version | Semantics | Example |
|---|---|---|
| counts/v1 | Completeness (present/rows) | id is 80% complete |
| groups/v1 | Grouped distinct-count bounds | Each region has ≥ 2 distinct ids |
| ranges/v1 | Value-range bounds | All values ≥ 0 and ≤ 100 |

Plans are single-semantics: compose them with `RulesetEnvelope` for mixed
rulesets and execute the whole ruleset with one call —
`dq.spark_adapter.execute_envelope(envelope, datasets)` returns the
aggregate verdict.

## Documentation

- [Quickstart](docs/quickstart.adoc)
- [Error catalog](docs/error-catalog.adoc)
- [ADRs](docs/decisions/adr/)
- [Roadmap and spec tracker](docs/roadmap.adoc) — every spec with status and evidence
- [Writing an engine](docs/writing-an-engine.adoc) — the engine contract, conformance suite, and a worked example
- [SPECs](docs/specs/)
- [Modernization plan](docs/modernization-plan.adoc)
- [Compute engines beyond Spark](docs/compute-engines.adoc)

## License

Apache-2.0
