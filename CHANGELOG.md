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
- **Rule packs in YAML and JSON** (SPEC-010), alongside HOCON. The format
  follows the file extension (`.conf`/`.hocon`, `.yaml`/`.yml`, `.json`)
  for local files and for `s3://`, `abfss://` and `http(s)://` sources.
  The same rule pack loads to the same configuration and produces the same
  run outcomes in all three formats. YAML is loaded with `safe_load` only
- A published JSON Schema for the rule-pack envelope,
  `dq/schemas/rule-pack.schema.json`, shipped in the wheel (CI verifies it
  is there) for editor validation. Engine payloads stay open; each built-in
  engine's required payload key is declared in the registry
- A typed, validated `RulePack` snapshot (`dq.config`). The whole rule
  pack is checked before any engine runs, so a broken rule no longer costs
  the earlier rules' Spark work. Messages name the rule index and the key
- `dq-validate` validates every format, and in stages: syntax, the
  envelope against the schema, the shape of each rule, then each built-in
  engine's payload. It reports every problem of the first failing stage.
  Its `--format json` output gains `format` and `errors`
- `examples/basic_deequ_validation.yaml` and `.json` (twins of the HOCON
  example) and `examples/overlays/` (per-environment overlays)
- **Engine SDK** (SPEC-011): `dq.sdk.conformance` runs an engine through
  the engine contract -- manifest, template, outcome boundary, unique
  labels, canonical severity, determinism, attribution under reordering,
  persistence honesty, run key -- and reports each violation by the check
  that found it; every built-in engine passes. The reordering check takes a
  factory for the same engine with its checks in another order, and
  requires every check to keep its outcome under its own name: that is what
  finds an engine that reads a vendor's result positions as identities,
  which no other check can see. Without it the report says attribution was
  not checked. `dq.sdk.fixtures` is one semantic corpus for all compute
  adapters: 19 cases across counts/v1, groups/v1 and ranges/v1, each stating
  the metrics an adapter must compute and the decisions that follow. The
  decisions are checked against the kernel with no engine, and the metrics
  against Spark, with a Deequ cross-check on every counts/v1 rule where the
  two semantics coincide
- `dq-engines`: lists the engines this installation can resolve, built-in
  and entry-point, with each engine's capability manifest and how to
  install a missing optional dependency
- `scripts/new_engine.py NAME`: scaffolds an engine that passes the
  formatter, its own unit tests and the repository's guards as generated
- `docs/writing-an-engine.adoc`: the engine contract with a complete example
  that is executed as a test
- **Run observability** (SPEC-014): a run tells an observer five things --
  that it started, that each rule started and finished against each dataset
  and how long that took, that it finished, and what the gate decided --
  through `dq.observability.RunObserver`. Every notification is an immutable
  record, and an observer that raises is logged and swallowed, so observing a
  run can never change it. With nothing configured the run is logged under
  `dq.run_events`, one line per notification, naming the run and, where one
  applies, the rule and the engine; `DQ_LOG_FORMAT=json` writes each line as
  JSON. `dqframework.observers` is configured like `dqframework.notifiers`
  (`logging`, `memory`, `otel`), and `DQFramework(..., observer=...)` injects
  one. A rule or a run that raises is still announced as finished, with the
  error
- An `otel` extra, `pip install dq-kernel[otel]`: an `OpenTelemetryObserver`
  with one span per run and one per rule application, and one counter of
  outcomes by status and criticality. It depends on `opentelemetry-api` only;
  the application brings its own SDK, and until its providers are configured
  the observer records nothing
- `examples/observability.conf`, and "Observing a run" in the operator
  reference
- **Evidence importers** (SPEC-013): `dq.harvest` reads another tool's
  quality results as this framework's own evidence, and imports no vendor
  library to do it. Three sources: dbt's `run_results.json` (its data tests),
  a Great Expectations validation result, and a columnar file -- CSV, or a
  JSON array of records with `check`, `success`, an optional `criticality`
  and an optional `details` -- that any tool can write. Every imported
  outcome carries `details.origin`: the source system, the reference, the
  digest of the bytes that were parsed, and the run the source names. A
  source that cannot be imported -- a status the importer does not know, no
  check in it, a malformed record, two checks with one name -- is refused
  with a `HarvestError` naming the record and its position
- `dq-import SOURCE --dataset-id ID --dataset-sha256 SHA --report-json PATH`:
  reads a source once, applies the gate policy (`--fail-on`) and writes a
  `dq-report/v1` document. It exits 0, 1 or 2 as `dq-kernel` does
- `examples/import/`: a `run_results.json` as dbt-core 1.12.7 wrote it, a
  validation result as Great Expectations 1.24.1 wrote it, and the columnar
  format in CSV and JSON. The tests import all four
- "Importing evidence" in the operator reference: what each source becomes,
  the columnar format, and the refusals
- Property-based tests (Hypothesis) for the pure kernel: the sanitizer
  always produces strict JSON, gate decisions partition the failures, exact
  thresholds stay exact where doubles would not, labels stay unique
- **Durable SQL sink on SQLAlchemy Core** (SPEC-007, ADR-010):
  `dq.sqlalchemy_sink.SqlAlchemyOutcomeSink` takes an injected SQLAlchemy
  `Engine` or `Connection`. Statements are `text()` with named binds;
  schema, table and namespace names are validated by `dq.identifiers` and
  quoted only where the database's dialect requires it, so a mixed-case
  schema keeps its case. The DDL is compiled for the engine's own dialect
  (`JSONB` on PostgreSQL), and `ddl_statements()`, `verify_schema()`,
  `prune()` and `run_scope()` keep their meaning (ADR-006, SPEC-001, SPEC-006).
  A `Connection` the caller owns is never closed, and a run started inside
  the caller's transaction is a savepoint
- Optional extras `sqlalchemy` (`>=2.0,<2.2`) and `arrow` (`pyarrow`).
  The durable sink's tests run its statements on a real database;
  `dq/tests/test_sqlalchemy_sink_postgres.py` runs them against a real
  PostgreSQL when `DQ_TEST_POSTGRES_URL` names one
- `examples/durable_evidence.conf`, and a "Durable SQL sink" section in the
  operator reference
- **Arrow compute adapter** (SPEC-015, ADR-010, ADR-014):
  `dq.arrow_adapter` executes counts/v1, groups/v1 and ranges/v1 plans over
  Arrow tables and hands the values to the kernel's own evaluation, so a
  decision is made in one place whichever engine computed. It accepts a
  pyarrow table, a record batch, anything that exports Arrow's stream
  interface (Polars, DataFusion, pandas 2.2 and later), a DataFusion frame's
  `to_arrow_table()`, a pandas DataFrame, or a Spark DataFrame through its
  Arrow export, and imports no library but pyarrow. It computes what the
  Spark adapter computes: the same semantic corpus, and on real Spark the
  two agree on every metric, decision and refusal. A column is brought to a
  form Arrow can compute on before it is measured, each step lossless.
  Dictionary-encoded columns are decoded: undecoded, a float column's type
  says `dictionary` and a present count cannot tell that a NaN is in it.
  Polars' `string_view` strings become strings; half and single floats are
  widened to double, where adding `0.0` also makes a signed zero one value,
  as Spark counts it; `decimal32` and `decimal64` become `decimal128`. What
  Arrow cannot compute on -- a list or a struct -- is refused, naming the
  dataset, the column and the metric
- A portable rule chooses its compute. `adapter = "arrow"` runs its plan on
  the Arrow adapter, inside the Spark facade or with no Spark at all; the
  key is `spark` when absent, and any other value is refused before data is
  read
- **A Spark-free runner** (ADR-014): `dq.runner.run(rule_pack, {name: path},
  report=...)` and the `dq-run` command read datasets with pyarrow --
  Parquet, Arrow IPC, CSV and newline-delimited JSON by a file's extension,
  a directory as the dataset it holds -- and run portable rules on Arrow
  through the same run core as the Spark facade, gate policy, observers and
  notifiers included: `dq-run rules.conf --dataset orders=orders.parquet
  --report report.json`. No Spark session and no sink: the evidence is a
  `dq-report/v1` document with `runtime.spark` null. Everything a run over
  files cannot do is refused before a dataset is read and all at once: a
  rule that needs Spark, a portable rule that does not name the Arrow
  adapter, a configured sink, a dataset missing, unused, remote or
  unreadable
- The run core, `dq.run.run_rules`, extracted from `DQFramework.run()`: the
  rule loop, outcome normalization, the gate policy, notifiers and observers
  in one place for every runner. `DQFramework` keeps its signature and its
  outcomes
- `dq.compute_support`: what every compute adapter shares -- which metric
  kinds each semantic version defines, the exact-column and exact-fraction
  refusals, the order a plan's datasets and groupings are visited, and a
  plan's execution (validate, check the bindings, compute, evaluate) -- so
  adapters cannot drift apart in what they accept or in what they say. A new
  compute adapter is three callables and one entry in `ADAPTER_MODULES`
- `examples/arrow_rule.conf`, one rule per semantic version for `dq-run`,
  and a "Running over files, without Spark" section in the operator
  reference, whose Python is run by a test
- `polars` and `datafusion` as development dependencies, so the interchange
  tests run in CI instead of skipping
- **SQL compute adapter** (SPEC-016, ADR-011): `dq.sql_adapter` compiles a
  counts/v1, groups/v1 or ranges/v1 plan into one aggregate `SELECT` per
  dataset, has the database run it, and hands the row back to the kernel's
  own evaluation. The data does not move. A dataset is bound as a
  `SqlDataset`: a SQLAlchemy table whose columns declare their types, and
  the `Engine` or `Connection` that reaches it; `adapter = "sql"` on a
  portable rule names it. DuckDB is the certified lane: the corpus passes
  there, and the Spark and SQL adapters agree on every metric over the same
  rows. Every corpus statement is held as a PostgreSQL snapshot, and was run
  once by hand on a real PostgreSQL 14
- Everything checkable is refused before any query runs, naming the rule
  that asked: a column not there exactly; a dialect the adapter does not
  know; a type it cannot measure; a bound of a text, temporal, binary or
  UUID column; a float on a dialect that cannot keep NaN out; a parameter of
  a bound subquery with no value. The order matters because DuckDB has no
  savepoint and leaves a failed statement's transaction aborted
- Two DuckDB behaviours the adapter undoes: a decimal read through a typed
  expression comes back rounded to a double, so the statement is run as
  text; and PostgreSQL has no `min` or `max` of a boolean, so a boolean's
  bounds are taken of it cast to an integer there
- `engine_from_env` builds a bind from a URL the environment holds: a
  credential never enters a rule pack
- `duckdb` and `duckdb-engine` as development dependencies, so the lane runs
  in CI; and a "The SQL adapter" section in the compute-engines reference,
  whose Python is run by a test

### Changed

- **The whole lock is re-resolved**, with every constraint in `pyproject.toml`
  unchanged: 41 movements, where updating one dependency at a time had left
  the transitive ones behind. Great Expectations 1.24.1 → 1.25.0, pandas
  3.0.5 → 3.1.1, and 38 more, most of them patches. Two packages are now held
  in two versions, split by Python version, which the lock format allows and
  an incremental lock never produces: SQLAlchemy is 2.0.58 on Python 3.12,
  where DQX requires below 2.1, and 2.1.9 on 3.13; protobuf likewise 6.33.6
  and 7.36.2. The one major movement, `importlib-metadata` 9 → 10, is in the
  `delta` extra, which no CI lane installs
- **Every check has its own name, and its own verdict.** The engine
  conformance suite found that the Deequ, Great Expectations, businessrules
  and schema-validation engines reported several checks under one name
  (`Schema Validation`, `Compliance`, an expectation type), so their evidence
  could not be attributed to a rule. Every engine now labels a check by its
  configured name -- `constraint_name`, `alias` or `description` for Deequ;
  `name` for Great Expectations and DQX; `constraint_name` or the constraint
  for businessrules -- else by its kind and columns (`isComplete_id`,
  `expect_column_values_to_not_be_null_id`). A rule whose two checks would
  report the same label is refused. Labels therefore change for existing
  rule packs that relied on the old defaults; set an explicit name to keep
  a particular label. The Great Expectations `derive_names` flag is
  withdrawn: naming by type and column is the default
- **A constraint's verdict is its own.** Where several constraints share
  one Deequ Check -- the `single_check_mode` flag, and schema validation's
  default -- a passing constraint was reported as failed as soon as a
  sibling failed. Each is now reported by its own status
- **`PostgresOutcomeSink` takes a SQLAlchemy `Engine` or `Connection`, not
  a DB-API `connect` callable** (SPEC-007). The name still resolves to the
  sink; what it is given changes. A callable is refused with a message that
  says to build an engine with `sqlalchemy.create_engine(url)`. Rule packs
  need no change: `repository { type = "postgres" ... }` builds the sink
  from the same `dsn_env` setting as before
- **The database driver must be installed when the sink is built.** It used
  to be imported at the first connection; SQLAlchemy imports it when the
  engine is created. A missing driver, or a URL naming a dialect nothing
  provides, is a configuration error (exit code 2) that names what to
  install
- The value of `DQ_POSTGRES_DSN` is read as a SQLAlchemy URL. One written
  `postgresql://` means the psycopg 3 driver, as it always did for this
  sink, and a URL naming another driver (`postgresql+psycopg2://`) keeps it.
  A libpq keyword/value DSN is still read by psycopg 3
- **An artifact row is stored as strict JSON.** A NaN becomes `null`, and a
  decimal or a timestamp its text. The row used to be serialized with
  `json.dumps` as it was, which wrote `NaN` -- text a `jsonb` column
  refuses -- and could not store a decimal at all
- **Artifacts are read through Arrow when pyarrow is installed**: a Spark
  DataFrame through the Arrow export it has, a pyarrow table or anything
  that exports the Arrow stream interface directly. Otherwise the frame's
  own rows are read, as before. The stored payload is the same either way,
  so evidence does not depend on whether pyarrow is installed. Set
  `repository { arrow = false }` to turn it off. Classic PySpark 3.5 has no
  public Arrow export, so its `_collect_as_arrow` is used when present
- `prune()` deletes a namespace's old artifacts with a correlated `EXISTS`
  where it used PostgreSQL's `DELETE ... USING`; the decision is the same
  and the statement is no longer PostgreSQL-only
- `WriteResult.targets` names the sink's table as `postgresql:dq.artifacts`
  (it was `postgres:"dq"."artifacts"`)
- An engine's capability manifest is inherited: a subclass states only what
  differs. An invalid manifest, and an engine that cannot persist handed a
  repository, are refused when the engine loads, before any rule
- The portable engine reports a canonical `criticality` from the kernel's
  severity. Without it a Warning-level portable rule blocked under the
  `error` gate policy
- **Fixed: `dq-kernel rules.conf` failed on a bare file path.** The
  framework parsed any string without a URI scheme as inline HOCON text, so
  the most natural invocation died with a parser dump, while
  `dq-validate rules.conf` accepted the same path. A scheme-less reference
  that is not inline HOCON is now read as a file; `file://` still works
- **Configuration errors are readable.** A HOCON parse failure showed
  thousands of characters of parser internals. It now states the file, the
  line and column, and -- when the cause is known, such as several keys on
  one line -- what to change
- One rule-pack loader serves the framework and `dq-validate`, replacing
  two copies of the source-dispatch logic. `load_from_uri` raises
  `ConfigurationError` on a non-200 response instead of returning `None`
- A rule with `dataframes = []` fails closed before any engine runs. It
  used to validate nothing and return no outcomes
- Ten tests that used the Spark session without the `spark` marker are
  marked now; they had been starting a JVM inside the unit lane, which is
  documented to run without one
- **The coverage gate refuses a report that is not this repository's.** A
  test that runs pytest in a scratch copy of the tree had that copy measured
  and merged into the parent's report, as a second, untested `dq` package:
  with pytest-cov 6 it took line coverage from 92.8% to 37% with no code
  changed. The children are no longer armed, and `scripts/check_coverage.py`
  stops on any reported path outside the checkout before it reads a floor
- The semantic corpus refuses a cell no engine can store as written. Spark
  stores a decimal `NaN` as null and rounds `1.234` in a `decimal(10,2)`
  column to `1.23` without a word, so a case could pass validation and then
  run on data other than the one it documents
- **The coverage floors can fail again.** They had been 55% combined, 57%
  line and 46% branch since v0.1.0 while CI measured 93%; they are now 90%,
  90% and 88% — a few points under the measured value, so a real drop fails
  the build. The owner's private CI lane now installs the `dqx` extra like
  the public lane, so the DQX engine is tested on the owner's own pull
  requests, where that lane runs instead of the public one
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
- The rule-pack schema declares `dqframework.observers`, so a misspelled
  observer type is refused before any rule runs, and `RulePack.observers`
  reads it back
- `dq.utils.imports.require_extra` takes a `kind`, so something that is not
  an engine names itself in the missing-dependency message. Every existing
  message is unchanged
- `dq.engine.dq_engine.outcome_frames(spark, outcomes)` frames outcome
  dictionaries for the sinks, as the engine base has always framed its own
  (`_artifact_frames` now calls it). The schema is stated rather than
  inferred: given outcome dictionaries whose `details` differ in shape,
  `spark.createDataFrame` infers a map of strings and stringifies every
  nested value, without an error
- A DQX check without a `name` is labelled by its function and its columns
  (`is_not_null_id`, `is_unique_a_b` for `for_each_column`), as every other
  engine labels a check. It was `is_not_null_0`: a name that changed
  whenever the checks were reordered, so evidence recorded under it changed
  meaning
- `docs/index.adoc` links the tracker instead of a hand-maintained list of
  specs, lists ADR-011 through ADR-015, and no longer links two review
  files in a `docs/reviews/` directory that does not exist
- The shared semantic corpus has 24 cases instead of 19: the two signed-zero
  cases (as a value and as a grouping key), an empty ranges input, a `-0.0`
  bound that is zero, and boolean bounds. Each was verified on a real Spark
  session before it was written
- A portable rule reads an optional `adapter` key (`spark`, the default, or
  `arrow`)
- The SQL sink reads a frame's rows through `dq.arrow_adapter.table_of`, the
  one place that knows how to get an Arrow table out of a frame. What it
  stores is unchanged

### Fixed

- The `greatexpectations` engine attributed GX's results to checks by
  position. GX groups expectations by column and returns results in that
  order, not the order they were defined in, so in any rule that mixed
  column-level and table-level expectations a check was reported with
  another's verdict. Criticality travels with the label, so a failed
  `error` check could be reported as passed, a passing one as failed at
  `warn`, and a gate set to `fail_on = error` opened. Each result is now
  matched on the expectation it carries (type, arguments and severity), and
  a run that cannot be attributed is refused with the position and the
  check names. Two expectations GX treats as the same (one type and
  arguments) are refused when the suite is built: GX silently ran only one
  of them, and the engine then failed with a bare `zip` length error
- `examples/portable_rule.conf` no longer translated: its size check was
  written as `min = 1`, which the translator refuses. It is repaired, and a
  test now runs it. `docs/index.adoc` said the `build` check was not yet
  required and that numeric ranges were outside the portable translator;
  both statements were out of date

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
