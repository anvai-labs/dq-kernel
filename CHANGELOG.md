# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Spec lifecycle and progress tracker** (SPEC-008):
  `docs/roadmap.adoc` is the one place that says what is planned, in
  flight, and done. Spec format v2 adds a `Milestone`, optional
  `Depends on` and `Gate` header fields, and a per-gate `Progress`
  checklist; a status of `Verified` requires every gate checked with an
  evidence citation. The tracker table is generated from the spec files
  by `scripts/roadmap.py` and held to them by a contract test, which also
  proves (by mutating a real spec) that it rejects each kind of drift
- Seventeen new specs, SPEC-008 through SPEC-024, across five milestones
  (CI gates, configuration, engine SDK, gate policy, SQLAlchemy sink,
  evidence importers, observability, Arrow and SQL compute, the pandera
  and Soda engines, profiling, history, row quarantine, streaming, and
  platform/distribution work), each with testable acceptance gates
- ADR-013 (gate policy and severity), ADR-014 (one run core, many
  runners) and ADR-015 (row outcomes and the data-plane sink) as proposals
  recording the decisions those specs depend on
- **Gate policy** (SPEC-012, ADR-013): `dqframework.gate.fail_on` decides
  which failed checks block a pipeline -- `any` (the default, which keeps
  today's behaviour), `error` (checks whose `criticality` is `warn` are
  reported but do not block) or `never`. `dq.gate.evaluate_gate` is the one
  place the decision is made, and `DQFramework.last_gate_decision` records
  it for each run
- Notifier adapters `slack` and `teams`, and a recording `memory` notifier
  (`InMemoryNotifier`); a `report` reference on `GateEvent`; and
  `DQFramework(..., notifier=..., report_reference=...)` for injecting a
  notifier and naming the run's report. An unknown gate policy now fails
  before any engine runs
- `examples/gate_policy.conf`, and a "Gate policy" section in the operator
  reference

### Changed

- **`dq-kernel` exit codes**: 0 when the quality gate passed, 1 when it
  failed or the run errored, and 2 when the configuration is invalid or a
  dependency is missing. Every failure used to return 1, so a caller could
  not tell a data problem from a typo; scripts that test specifically for
  exit code 1 on a configuration error will see 2
- The notifier configuration key is `dqframework.notifiers` (it was
  `dqframework.notify`); the webhook adapter makes three attempts with
  capped exponential backoff (it made one); and the gate event is emitted
  after the run scope has exited, as ADR-008 requires. The first wiring
  emitted it inside the scope, so notification joined the durability
  transaction
- `dq-report/v1.summary` gains the additive counts `blocked` and `warned`.
  `status` is unchanged and never follows a gate policy
- SPEC-001 through SPEC-007 are reconciled against the code: 001, 004 and
  006 are `Released — 0.2.0` with a test cited for every gate; the two
  gates SPEC-002 lacked are delivered by SPEC-012; SPEC-003 and SPEC-007
  are accepted and unbuilt in part or whole; SPEC-005 is deferred because
  its control plane moved to a closed repository
- The historical planning documents state that they are proposals and
  point at the tracker; every proposal they make has a recorded disposition
- `docs/index.adoc` links the tracker instead of a hand-maintained list of
  specs, lists ADR-011 through ADR-015, and no longer links two review
  files in a `docs/reviews/` directory that does not exist

## [0.3.0] - 2026-11-24

### Added

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
- **Engine plugin contract** (ADR-012): template-method `apply` on the
  `DQEngine` ABC (capability guard -> shared log preamble -> `_prepare` ->
  abstract `_execute` -> `_persist_outcomes`) with `CAPABILITIES` v2
  (`persistence` joins `criticality`/`row_filter`/`failed_rows`); all
  built-in engines migrated to hooks -- artifact table schemas per engine
  are preserved exactly (deequ-family raw vendor frames, businessrules
  native frames) and pinned by shape-guard tests
- **Declarative engine registry** (`dq.engine.registry`): one `EngineSpec`
  per engine (module, class, payload key/type, extra, renamed-from);
  `EngineLoader` resolves registry -> `dq_kernel.engines` entry-point
  group (third-party engines on PyPI) -> naming-convention fallback;
  `dq-validate` reads payload rules from the registry (the hardcoded
  table is gone); a drift-guard test pins every engine directory to a
  registry entry; `EngineNotFoundError(DQFrameworkError, ImportError)`
  replaces raw ImportError/AttributeError leaks
- **`portable` engine**: the kernel's portable plans run through
  `DQFramework.run()` like any engine (`engine = "portable"`; payload
  `plan` + `dataset` + caller-certified `snapshot_sha256` -- the digest
  is required and never auto-derived). Fail-closed translation only
  (ADR-002)
- Shared **check seam** (`dq.engine.checkspec`): rule identity and
  per-rule severity (`criticality`, normalized across DQX's
  `error`/`warn`, Deequ's `Error`/`Warning` and common short forms)
  normalize in one fail-closed place; engine check vocabularies stay
  native. Shared helpers remove the per-engine duplications:
  `checkspec.build_result` (the one result constructor),
  `dq.utils.imports.require_extra` (the one lazy-import install hint),
  `dq.utils.sanitization` (strict-JSON sanitizer), run-artifact frames on
  the `DQEngine` base class, and the shared pydeequ VerificationSuite
  scaffold (`dq.engine.deequ.verification`)
- Notifier wired into `DQFramework.run()` (config-driven
  logging/webhook/composite): one `GateEvent` per run with an additive
  severity split (`blocked`/`warned` from `details.criticality`; absent
  severity blocks conservatively); `status`/`failed` semantics unchanged
- Light-core import contract extended to the orchestrator and the engine
  contract: `import dq.dq_framework` and `from dq.engine.dq_engine import
  DQEngine` succeed with pyspark/pydeequ/boto3/databricks blocked
- `docs/compute-engines.adoc`: vendor support matrix and recipes for
  non-Spark compute (pandas/Arrow engine skeleton, SQLAlchemy/DuckDB via GX)

### Changed

- `greatexpectations` engine: entries accept optional `name` (labels the
  metric result; the default label stays the expectation `type`) and
  `criticality` (default `error`, surfaced in every result's `details`);
  opt-in `derive_names = true` labels results `<type>_<column>`; results
  are zipped with suite labels `strict=True`
- `deequ` engine: `level` accepts canonical spellings (`error`/`warn`)
  alongside `Error`/`Warning`, and every result's `details` carries
  `criticality`
- One run key per run: `_persist` uses the framework-injected `dqts`
  (epoch-millis fallback for directly constructed engines), so artifact
  writes, `run_scope`, metric `ts` and `GateEvent.run_key` share one key
- `dq.utils.constants` removed: import `dq.utils.keys` (HOCON key
  constants) or `dq.engine.schemavalidation.datatypes` (pydeequ datatype
  maps) -- the foundation layer imports no vendor libraries
- schemavalidation subclasses `DQEngine` directly (composition through the
  shared deequ scaffold replaces engine-to-engine inheritance)
- CI: the main test matrix installs the `dqx` extra too (Poetry skips it on
  the 3.13 job) and runs the live DQX round trips; the `build` gate
  requires every lane listed in `needs` to succeed; CI runs on every pull
  request regardless of base branch, so stacked pull requests are tested;
  lint runs `poetry check --lock` and a sha256-pinned actionlint

### Deprecated

- Overriding `DQEngine.apply`: engines implement the `_prepare`/`_execute`
  hooks. External overrides keep working through MRO with a
  `DeprecationWarning` (a trivial `_execute` stub is required to
  instantiate)

### Fixed

- pytest `addopts` no longer pass `--no-summary`: pytest 9 skips the whole
  terminal summary under that flag, which hid every failure traceback, in
  CI as well. Subprocess contract tests pin the traceback visibility, and
  a workflow contract test pins the CI gate rules

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
