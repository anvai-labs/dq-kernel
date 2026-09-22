# dq-kernel

Certifiable data-quality evidence engine for lakehouse pipelines. Semantically
versioned rules, exact-arithmetic evaluation, and machine-checkable evidence —
engine-agnostic by design.

## Install

```bash
pip install dq-kernel[spark]
```

Requires Python 3.12/3.13, Java 17, and a Spark 3.5.9 runtime.

## Quick start

```python
from dq.dq_framework import DQFramework

results = DQFramework(spark, config, default_dataframe=df).run()
```

Each result carries `check`, `success`, and bounded `details`. Evidence is
bound to dataset and rule-set digests as `dq-report/v1`.

## Three semantic versions

| Version | Semantics | Example |
|---|---|---|
| counts/v1 | Completeness (present/rows) | id is 80% complete |
| groups/v1 | Grouped distinct-count bounds | Each region has ≥ 2 distinct ids |
| ranges/v1 | Value-range bounds | All values ≥ 0 and ≤ 100 |

Plans are single-semantics: compose them with `RulesetEnvelope` for mixed
rulesets.

## Documentation

- [Quickstart](docs/quickstart.adoc)
- [Error catalog](docs/error-catalog.adoc)
- [ADRs](docs/decisions/adr/)
- [SPECs](docs/specs/)
- [Modernization plan](docs/modernization-plan.adoc)

## License

Apache-2.0
