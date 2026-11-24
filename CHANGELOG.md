# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Shared **check seam** (`dq.engine.checkspec`): the engine-neutral axes
  all wrapped engines express differently -- per-rule identity (`name`,
  derived `<function>_<index>` when absent; explicit names unique,
  fail-closed) and per-rule severity (`criticality`, normalized across
  DQX's `error`/`warn`, Deequ's `Error`/`Warning`, and common short
  forms). Engine check-function vocabularies deliberately stay
  engine-native and pass through untouched. Engines declare a
  `CAPABILITIES` manifest (`criticality`: native/derived/absent,
  `row_filter`, `failed_rows`) so consumers branch on capability, not
  engine name; shared strict-JSON sanitizer (`dq.utils.sanitization`),
  shared optional-dependency import helper (`dq.utils.imports`),
  shared result constructor (`checkspec.build_result`) and shared
  run-artifact frames (hoisted to the `DQEngine` base class) remove the
  per-engine duplications
- `dqx` engine (Databricks Labs DQX): validates Spark DataFrames against
  `databricks-labs-dqx` metadata checks (`checks = [{check = {function =
  "is_not_null", arguments = {column = "id"}}}]`). Entries pass through to
  DQX's native schema with kernel guarantees layered on top -- `criticality`
  defaults to `error` (must be `error`/`warn`; Deequ-style spellings
  accepted), explicit deterministic check names (`<function>_<index>` when
  absent, duplicates fail closed) so results map 1:1 to configured checks,
  and DQX structural validation runs before any data flows.
  `for_each_column` passes through to DQX and reports one aggregated result
  per entry. Two client modes: `client.mode = "stub"` (default) runs on
  plain local Spark with no Databricks workspace; `client.mode =
  "workspace"` builds a real `WorkspaceClient` (optional `client.profile`).
  DQX results (`_errors`/`_warnings`) are aggregated into one metric per
  configured check with `failed_rows` and capped sample messages. New `dqx`
  extra: DQX 0.17.x requires Python <3.13, so the extra carries a Python
  marker and is skipped on 3.13 (its `databricks-sdk` requirement overlaps
  the `databricks` extra; the two are co-installable)
- `greatexpectations` engine: entries accept optional `name` (labels the
  metric result; default label stays the expectation `type` as before) and
  `criticality` (defaulting to `error`, surfaced in every result's
  `details`); opt-in `derive_names = true` labels results
  `<type>_<column>`; results are zipped with suite labels `strict=True`
- `deequ` engine: `level` accepts canonical spellings (`error`/`warn`)
  alongside `Error`/`Warning` (normalized fail-closed), and every result's
  `details` carries `criticality` derived from the check result level
- CI: the main test matrix installs the `dqx` extra as well (Poetry skips
  it on the 3.13 job) and so runs the live DQX round trips; the `build`
  gate now requires every lane listed in `needs` to succeed, so a red lane
  can no longer merge; CI runs on every pull request regardless of base
  branch (stacked pull requests are tested); lint checks
  `poetry check --lock` and runs actionlint
- pytest `addopts` no longer pass `--no-summary`: pytest 9 skips the whole
  terminal summary under that flag, which hid every failure traceback, in
  CI as well. A subprocess contract test pins the traceback visibility

## [0.2.0] - 2026-10-23

### Changed

- **Breaking**: the `custom` engine is renamed to `businessrules` --
  `engine = "custom"` no longer resolves. Update rule packs to
  `engine = "businessrules"` (constraint names are unchanged;
  `dq-validate` rejects the old keyword with a migration hint). Code
  surface moves to `dq.engine.businessrules.businessrules_engine.BusinessrulesEngine`
  and tests/examples follow the new name

### Added

- `dq.cdf_adapter` (ADR-011 role B): validates Delta table increments
  through the portable kernel -- `read_changes` reads a Change Data
  Feed window (change batch of insert/update_postimage rows; counts
  measure change volume, one row per commit), fail-closed on missing
  `delta.enableChangeDataFeed`, and `execute_cdf_plan` evaluates
  counts/groups/ranges semantics scoped to the change batch under the
  `spark-cdf` adapter, reusing the Spark adapter's metric computation;
  new `delta` extra provides delta-spark for OSS Spark

- `PipelineExpectationsHarvester`
  (`dq.engine.databricks.pipeline_expectations`): harvests Lakeflow
  Declarative Pipeline expectation metrics from the Unity Catalog
  event log (`flow_progress` events via the `event_log(TABLE(...))`
  function or the pipeline's configured event-log table) into the
  framework outcome contract, joining platform-native checks with the
  engines' evidence stream; ADR-011 records the DQ-federation and
  JVM-bound deployment strategy

- The `greatexpectations` engine is now fully fledged on GX Core
  (`great_expectations >=1.24,<2`, install with the `greatexpectations`
  extra): modern ephemeral-context Spark validation, snake-case
  expectation `type` resolution that fails closed on typos, a
  strict-JSON result sanitizer (depth/size bounded) so GX payloads can
  never breach the outcome limits, metrics+verifications persistence
  under one run key, live and mocked test suites in CI, and
  `examples/greatexpectations_validation.conf`
- `dq.spark_adapter.execute_envelope`: one entry point that computes every
  subplan of a `RulesetEnvelope` with its own semantics and capabilities and
  returns the envelope's aggregate verdict; Spark-backed end-to-end tests
  pin the dispatch across counts/v1, groups/v1, and ranges/v1


### Performance

- Test-suite CI cost cut by 13 Spark executions (~12s per lane) with
  production coverage byte-identical to the pre-cut baseline: the dtype
  failure axis runs for one representative dtype, an exact duplicate
  persistence test and two legacy alias tests are removed (paths covered
  elsewhere), and five dead conftest fixtures are deleted

### Removed

- **Breaking** (for external consumers only): compiler-graph-guided
  dead-code removal (victor tier-3 SCIP index, 1,836 compiler-proven
  reference edges; every target verified two ways -- zero incoming
  graph edges, zero repo references): the unused `dq.utils.catalog_utils`
  module, the relic `dq.dq_repository` package, the unused
  `dq.utils.s3_utils` wrapper, and the uncalled
  `DQFramework.pivot_configuration` method. None were referenced inside
  this repository; downstream code importing them must migrate

### Fixed

- Businessrules checks omitting `level` now default to `"Error"`: the
  previous `None` default inferred a NullType `check_level` column that
  broke sink persistence
- `DQEngine._persist` no longer requires pydeequ (epoch-millis run key
  computed directly), so any extras combination can persist artifacts
- `GroupedDistinctBounds` and `ConsecutivePercentChange` custom constraints
  accept a bare string for `columns` and `group_by` as the one-column form
  (previously the string was iterated character by character and died inside
  Spark with a cryptic `UNRESOLVED_COLUMN 'r'`); mis-typed parameters now
  fail before Spark with a `ValueError` naming the constraint and parameter,
  and `sort_by` accepts exactly one ordering column in either form (#25)

## [0.1.0]

### Added

- Semantic kernel with three sibling versions: counts/v1 (completeness),
  groups/v1 (grouped distinct-count bounds), ranges/v1 (value-range bounds),
  each with exact-arithmetic rules, capability negotiation, and canonical
  fingerprints
- `RulesetEnvelope` composing mixed-semantics plans into one certified
  ruleset identity with an aggregate verdict
- Native Spark adapter executing all three versions through shared
  aggregates, with differential certification against PyDeequ
- Translation subset converting six legacy constraints into portable plans
- Outcome sink port with `InMemorySink`, `RepositorySink`, and durable
  `PostgresOutcomeSink` (namespaces, retention, prune, verify_schema)
- Gate-event notifier port with logging, webhook, and composite adapters
- Error catalog covering 154 raise sites across 19 modules with an
  enforcing AST contract test
- Centralized identifier value objects at every catalog and SQL boundary
- 10 architecture decision records and 7 specifications

### Changed

- Fail-closed boundaries: strict JSON outcomes, validated identifiers,
  capability negotiation, semantic-version pinning
- Gate events delivered fail-open (notification failures never alter run
  outcomes)
- No telemetry, no phone-home, no license checks
