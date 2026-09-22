# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `dq.spark_adapter.execute_envelope`: one entry point that computes every
  subplan of a `RulesetEnvelope` with its own semantics and capabilities and
  returns the envelope's aggregate verdict; Spark-backed end-to-end tests
  pin the dispatch across counts/v1, groups/v1, and ranges/v1

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
