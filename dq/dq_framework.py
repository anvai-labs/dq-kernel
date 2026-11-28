# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Main orchestrator for the Data Quality Framework."""

import logging
import time


from dq.config import parse_rule_pack
from dq.engine.engine_loader import EngineLoader
from dq.catalog.catalog_factory import CatalogFactory
from dq.utils.config_utils import load_config, plain_key
from dq.exceptions import DataFrameNotFoundError
from dq.gate import policy_from_config
from dq.identifiers import IdentifierError, TableName
from dq.notifier import GateNotifier, notifier_from_config
from dq.observability import RunObserver, observer_from_config
from dq.outcomes import MAX_BATCH_BYTES, MAX_OUTCOMES
from dq.run import run_rules
from dq.sinks import sink_from_config

logger = logging.getLogger(__name__)


class DQFramework:
    """Main orchestrator that loads configurations, manages DataFrames,
    and coordinates engine execution for data quality validation.

    Args:
        spark: Active SparkSession.
        config: Configuration string (HOCON), or URI (file://, s3://, abfss://, http://).
        default_dataframe: Optional default DataFrame for rules that don't
            specify explicit dataframe references.
        notifier: Optional :class:`GateNotifier` to inject in place of the
            configured one (``dqframework.notifiers``).
        report_reference: Optional reference to the run's ``dq-report/v1``
            document, carried on the gate event.
        observer: Optional :class:`dq.observer.RunObserver` to inject in
            place of the configured ones (``dqframework.observers``); see
            :mod:`dq.observability`.

    Configuration supports a ``catalog_type`` key to control how table
    references are resolved:

    - ``"spark"`` (default): Uses Spark's built-in catalog.
    - ``"unity"``: Uses Databricks Unity Catalog (three-part names).
    - ``"glue"``: Uses AWS Glue Data Catalog.
    - ``"hive"``: Uses Hive metastore via Spark.

    Example::

        dqframework {
          catalog_type = "unity"

          dataframes {
            orders = "main.sales.orders"
            customers = "main.sales.customers"
          }

          dqrules = [...]
        }

    Or with explicit catalog components::

        dqframework {
          catalog_type = "glue"

          dataframes {
            orders {
              database = "sales_db"
              table = "orders"
            }
          }

          dqrules = [...]
        }
    """

    # Class-level defaults: tests and embedders sometimes assemble a
    # framework by hand (``__new__`` plus attributes) instead of loading a
    # rule pack, and such an instance must still run.
    _notifier = None
    _observer = None
    _report_reference = None
    last_gate_decision = None
    #: The clock the run measures rule durations by; a test may set another.
    _clock = staticmethod(time.perf_counter)

    def __init__(
        self,
        spark,
        config,
        default_dataframe=None,
        *,
        notifier: GateNotifier = None,
        report_reference: str = None,
        observer: RunObserver = None,
    ):
        self._spark = spark
        self._config = self._load_config(config)
        self.default_dataframe = default_dataframe
        self._notifier = notifier
        self._observer = observer
        self._report_reference = report_reference
        #: The :class:`GateDecision` of the last ``run()``, or ``None``
        #: before the first run.
        self.last_gate_decision = None
        self._catalog_type = self._config.get("dqframework.catalog_type", None)
        self._catalog_provider = CatalogFactory.get_provider(
            spark, catalog_type=self._catalog_type
        )
        self.dataframes = self._load_dataframes()

    @property
    def gate_policy(self) -> str:
        """The configured gate policy (``dqframework.gate.fail_on``)."""
        return policy_from_config(self._config)

    def _load_config(self, config):
        """Load a rule pack from inline text, a path, or a URI.

        Accepts HOCON, YAML and JSON; see
        :func:`dq.utils.config_utils.load_config`.

        Args:
            config: Rule-pack text, a path (with or without ``file://``),
                a supported URI, a mapping, or an already parsed tree.

        Returns:
            Parsed ConfigTree.

        Raises:
            ConfigurationError: If the rule pack cannot be read or parsed.
        """
        return load_config(config)

    def _load_dataframes(self):
        """Load DataFrames from configuration.

        Supports both simple string references (table names) and
        structured references with catalog/database/table components.

        Returns:
            Dict mapping logical names to DataFrames.
        """
        dataframes = {}
        config_dataframes = self._config.get("dqframework.dataframes", {})

        for said, table_ref in config_dataframes.items():
            # pyhocon keeps a quoted key in quotes; the name is what was written.
            df_name = plain_key(said)
            # An explicitly injected default is already the authoritative source for
            # the logical "default" dataset. Avoid resolving a shadowed catalog
            # reference (commonly a placeholder in reusable rule configurations).
            if df_name == "default" and self.default_dataframe is not None:
                dataframes[df_name] = self.default_dataframe
                continue
            try:
                df = self._resolve_dataframe(df_name, table_ref)
                if df is not None:
                    dataframes[df_name] = df
            except Exception as e:
                raise DataFrameNotFoundError(
                    f"Could not load configured DataFrame '{df_name}': {e}"
                ) from e

        if self.default_dataframe is not None and "default" not in dataframes:
            dataframes["default"] = self.default_dataframe

        return dataframes

    def _resolve_dataframe(self, df_name, table_ref):
        """Resolve a DataFrame reference using the configured catalog.

        Handles both string references ("catalog.schema.table") and
        structured config references with database/table/catalog keys.

        Args:
            df_name: Logical name for the DataFrame.
            table_ref: String table name or ConfigTree with components.

        Returns:
            Spark DataFrame or None.
        """
        if isinstance(table_ref, str):
            # Simple string reference: could be "table", "db.table", or "catalog.db.table"
            return self._catalog_provider.get_dataframe(table_ref)
        elif hasattr(table_ref, "get"):
            # Structured reference with explicit components
            table = table_ref.get("table", df_name)
            database = table_ref.get("database", None)
            catalog = table_ref.get("catalog", None)
            return self._catalog_provider.get_dataframe(
                table, database=database, catalog=catalog
            )
        else:
            # Fallback: treat as string
            return self._catalog_provider.get_dataframe(str(table_ref))

    def run(self):
        """Execute all configured data quality rules.

        Returns:
            List of metric dictionaries with keys:
                - ``check``: Check description
                - ``success``: Boolean result
                - ``details``: Detailed check output
                - ``ts``: Timestamp in milliseconds
                - ``jobid``: Spark application ID

        The run is :func:`dq.run.run_rules` (ADR-014), the one run core,
        which the Spark-free runner shares. What is this facade's own is
        Spark's: loading the configuration, resolving a dataset's logical
        name through a catalog, and the application id.

        The core tells an observer what the run did (SPEC-014,
        :mod:`dq.observability`); applies the gate policy
        (``dqframework.gate``, ADR-013) to the outcomes and returns the
        decision, recorded as ``last_gate_decision``; and announces one
        :class:`GateEvent` per run -- after the run scope has exited, so
        notification never joins the durability transaction (ADR-008). A
        notifier that fails to deliver is logged and swallowed; it can never
        alter the run's outcome. So is an observer that raises.
        """
        current_time_in_millis = time.time_ns() // 1_000_000
        sink = sink_from_config(self._config.get("dqframework.repository", {}))

        # Validate the whole rule pack before any engine runs, so a broken
        # rule is reported before the earlier rules' work is done (ADR-001).
        pack = parse_rule_pack(self._config)
        # Resolve the gate policy before any engine runs: an invalid policy
        # is a configuration error, and must not cost a run's Spark work.
        policy = self.gate_policy
        # The observer too: a configuration it cannot read is the same kind
        # of error, and must cost no engine work either.
        observer = self._observer_for_run()

        result = run_rules(
            pack.rules,
            load_engine=lambda rule, run_key: EngineLoader().load_engine(
                rule.engine, rule.definition, run_key
            ),
            dataset=self.get_dataframe,
            run_key=current_time_in_millis,
            job_id=self._spark.sparkContext.applicationId,
            dataset_name=self._dataset_name(sink),
            policy=policy,
            observer=observer,
            notifier_factory=self._notifier_for_run,
            clock=self._clock,
            sink=sink,
            report=self._report_reference,
            max_outcomes=MAX_OUTCOMES,
            max_bytes=MAX_BATCH_BYTES,
        )
        self.last_gate_decision = result.decision
        return result.outcomes

    def _observer_for_run(self) -> RunObserver:
        """Return the run's observer: the injected one, else the configured."""
        observer = self._observer
        if observer is None:
            observer = observer_from_config(
                self._config.get("dqframework.observers", None)
            )
            self._observer = observer
        return observer

    def _notifier_for_run(self) -> GateNotifier:
        """Return the run's notifier: the injected one, else the configured."""
        notifier = self._notifier
        if notifier is None:
            notifier = notifier_from_config(
                self._config.get("dqframework.notifiers", None)
            )
            self._notifier = notifier
        return notifier

    def _dataset_name(self, sink) -> str:
        """Return the name the run's evidence belongs to."""
        return (
            self._config.get("dqframework.dataset", None)
            or getattr(sink, "dataset", None)
            or "unknown"
        )

    def get_dataframe(self, df_name):
        """Retrieve a DataFrame by logical name.

        Checks in order:
        1. Default DataFrame (if df_name is "default")
        2. Pre-loaded DataFrames from config
        3. Spark catalog (temp views / tables)
        4. Configured catalog provider

        Args:
            df_name: Logical DataFrame name.

        Returns:
            Spark DataFrame.

        Raises:
            DataFrameNotFoundError: If DataFrame cannot be found.
        """
        if df_name == "default" and self.default_dataframe is not None:
            return self.default_dataframe
        elif df_name in self.dataframes:
            return self.dataframes[df_name]
        elif df_name in [t.name for t in self._spark.catalog.listTables()]:
            try:
                TableName.parse(df_name)
            except IdentifierError as error:
                raise DataFrameNotFoundError(
                    f"Invalid table name format: '{df_name}'"
                ) from error
            return self._spark.table(df_name)
        else:
            # Try the catalog provider as last resort
            try:
                return self._catalog_provider.get_dataframe(df_name)
            except Exception:
                raise DataFrameNotFoundError(
                    f"DataFrame '{df_name}' not found in config, Spark catalog, "
                    f"or {self._catalog_type or 'default'} catalog."
                )
