# Data Quality Framework Examples

This directory contains example configurations and scripts demonstrating how to use the Data Quality Framework.

## Configuration Examples

- `basic_deequ_validation.conf` - Basic data quality checks using the Deequ engine
- `basic_deequ_validation.yaml`, `basic_deequ_validation.json` - The same rule pack in YAML and JSON: every format loads, validates and runs identically
- `dqdl_validation.conf` - Vendor-specific DQDL compatibility rules through PyDeequ
- `schema_validation.conf` - Schema validation with datatype, nullable, and unique constraints
- `businessrules_constraints.conf` - Businessrules engine constraints (distinctness, rate-of-change, negative values)
- `greatexpectations_validation.conf` - GX Core expectations (snake-case `type` entries) through the greatexpectations engine
- `dqx_validation.conf` - Databricks Labs DQX metadata checks through the dqx engine (needs the `dqx` extra; Python <3.13 only)
- `multi_engine_pipeline.conf` - Multi-engine pipeline combining Deequ and schema validation
- `gate_policy.conf` - Gate policy (`fail_on`), notifiers, and severity-aware outcomes (ADR-013)
- `observability.conf` - Observing a run (SPEC-014): observers configured like notifiers. With nothing configured a run is logged, one line per notification; `DQ_LOG_FORMAT=json` writes JSON, and `otel` opens OpenTelemetry spans (needs the `otel` extra)
- `overlays/` - Per-environment overlays: `prod.conf` includes `base.conf` and overrides keys; environment values arrive through `${?VAR}`
- `unity_catalog.conf` - Configuration using Databricks Unity Catalog
- `glue_catalog.conf` - Configuration using AWS Glue Data Catalog

## Sample Scripts

- `sample_spark_job.py` - Complete working example with a local Spark session
- `portable_counts.py` - Dependency-free portable count-plan demonstration using fixture counts
- `portable_spark_counts.py` - Native Spark execution of a translated HOCON count-plan subset

## Import Examples

Sources for `dq-import` and `dq.harvest`, which read another tool's results as evidence. They need no Spark session and no vendor library.

- `import/run_results.json` - A dbt `run_results.json` as dbt-core 1.12.7 wrote it, trimmed to seven records: a model (not a quality result) and six data tests covering `pass`, `warn` (by severity and by threshold), `fail`, `error` and `skipped`
- `import/validation_result.json` - A Great Expectations validation result as GX 1.24.1 wrote it, trimmed to six expectations
- `import/checks.csv`, `import/checks.json` - The same three checks in the columnar format, in CSV and in JSON

These are result sources, not rule packs: `dq-validate` does not read them. The tests import all four.

```bash
dq-import examples/import/run_results.json --dataset-id orders \
    --dataset-sha256 <64 hex digits> --report-json evidence.json
dq-import examples/import/checks.csv --fail-on error
```

## Running Examples

Ensure you have the framework installed with Spark and Deequ support:

```bash
pip install dq-kernel[spark,deequ]
```

The Great Expectations example additionally needs the `greatexpectations` extra
(`pip install dq-kernel[spark,greatexpectations]`, GX Core >=1.24,<2).

Then run the sample job:

```bash
python examples/sample_spark_job.py
```

The local sample expects `lib/deequ-2.0.21-spark-3.5.jar`; download the matching artifact
from Maven Central or use the Maven-package setup in the root README. DQDL also requires
`software.amazon.glue:dqdl:1.0.0` on the classpath; see the
[operator reference](../docs/index.adoc). The sample intentionally contains a missing email
and exits nonzero to demonstrate failed-check handling. Configuration validation checks
structure; catalog examples still require the named tables, credentials, and compatible data.

CI parses every checked-in `.conf` file with the same structural validator used by
`dq-validate`. DQDL provides compatibility for existing Deequ rulesets; portable typed
rules are introduced through the engine-neutral kernel described in ADR-002. The
portable path currently covers size and completeness only (`dq.plan`,
`dq.spark_adapter`, and the HOCON subset translator `dq.portable_config`); other
constraints keep using their existing engines and fail closed on translation.
