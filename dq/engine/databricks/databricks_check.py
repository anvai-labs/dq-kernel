# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime
from urllib.parse import urlsplit

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunType, RunResultState
from pyspark.sql import functions as F
from time import time
from cstriggers.core.trigger import QuartzCron

logger = logging.getLogger(__name__)


class DatabricksCheck:
    """Monitors Databricks job timeliness and invocation metrics.

    Connects to a Databricks workspace to inspect scheduled job runs
    and compare actual vs expected invocations.
    """

    def __init__(
        self, databricks_token=None, databricks_url=None, workspace_client=None
    ):
        """Create a job monitor using Databricks unified authentication.

        ``databricks_token`` remains available for backwards compatibility, but
        production automation should omit it and let ``WorkspaceClient`` use
        OAuth, workload identity federation, or another unified-auth provider.
        A client can be injected for tests without supplying credentials.
        """
        if workspace_client is not None:
            if databricks_token is not None or databricks_url is not None:
                raise ValueError(
                    "workspace_client cannot be combined with explicit credentials"
                )
            self._workspace_client = workspace_client
            return

        options = {}
        if databricks_url is not None:
            options["host"] = self._validate_workspace_url(databricks_url)
        if databricks_token is not None:
            options["token"] = self._validate_token(databricks_token)
        self._workspace_client = WorkspaceClient(**options)

    @staticmethod
    def _validate_workspace_url(value):
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Databricks workspace URL must not be blank")
        parsed = urlsplit(value.strip())
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Databricks workspace URL must be an HTTPS origin")
        path = parsed.path.rstrip("/")
        if path:
            raise ValueError("Databricks workspace URL must not contain a path")
        return value.strip().rstrip("/")

    @staticmethod
    def _validate_token(value):
        if not isinstance(value, str) or not value or any(ch.isspace() for ch in value):
            raise ValueError(
                "Databricks token must be nonblank and contain no whitespace"
            )
        return value

    def check_scheduled_jobs_timeliness(
        self, spark, domain, jobs_to_monitor, job_created_by, last_run
    ):
        """Check whether scheduled Databricks jobs ran on time.

        Args:
            spark: Active SparkSession.
            domain: Domain label used for metric table naming.
            jobs_to_monitor: List of job names to check.
            job_created_by: Username filter for job ownership.
            last_run: Epoch timestamp (milliseconds) marking the lookback start.

        Returns:
            None. Results are written to a Delta table.
        """
        last_run_epoch = last_run / 1000
        dt_object = datetime.fromtimestamp(last_run_epoch)

        w = self._workspace_client

        _timelines_metrics = []

        dq_metrics_table = f"{domain}_dq.dq_metrics"
        end_date = datetime.now()
        logger.info("Monitoring jobs: %s", jobs_to_monitor)
        for domain_job_name in jobs_to_monitor:
            logger.debug("Checking job: %s", domain_job_name)
            jobs_itertor = w.jobs.list(name=domain_job_name)
            for domain_job_def in jobs_itertor:
                if domain_job_def.creator_user_name == job_created_by:
                    job_id = domain_job_def.job_id
                    schedule = getattr(domain_job_def.settings, "schedule", None)
                    cron_expression = getattr(schedule, "quartz_cron_expression", None)
                    if not cron_expression:
                        logger.warning(
                            "Skipping unscheduled Databricks job: %s",
                            domain_job_name,
                        )
                        continue
                    logger.info(
                        "Job name: %s, schedule: %s",
                        domain_job_name,
                        cron_expression,
                    )
                    cron_obj = QuartzCron(
                        schedule_string=cron_expression,
                        start_date=dt_object,
                        end_date=end_date,
                    )
                    triggers = cron_obj.next_triggers(100, isoformat=True)
                    expected_runs = len(list(triggers))

                    run_list = w.jobs.list_runs(
                        job_id=job_id,
                        start_time_from=last_run,
                        run_type=RunType.JOB_RUN,
                    )
                    actual_runs = 0
                    for run in run_list:
                        _timelines_metrics.append(
                            [
                                domain_job_name,
                                f"Job id: {job_id}, run id : {run.run_id}",
                                "Timeliness.IngestionTime",
                                run.run_duration,
                            ]
                        )
                        _value = 0
                        result_state = getattr(
                            getattr(run, "state", None), "result_state", None
                        )
                        if RunResultState.SUCCESS == result_state:
                            _value = 1
                        _timelines_metrics.append(
                            [
                                domain_job_name,
                                f"Job id: {job_id}, run id : {run.run_id}",
                                "Timeliness.Invocations",
                                _value,
                            ]
                        )
                        actual_runs += 1
                        logger.debug(
                            "%s duration: %s exec: %s run_id: %s success: %s",
                            run.run_name,
                            run.run_duration,
                            run.execution_duration,
                            run.run_id,
                            RunResultState.SUCCESS == result_state,
                        )
                    if expected_runs > actual_runs:
                        _timelines_metrics.append(
                            [
                                domain_job_name,
                                f"Job id: {job_id}, expected to run {expected_runs} but actual run was {actual_runs}",
                                "Timeliness.MissedInvocations",
                                (expected_runs - actual_runs),
                            ]
                        )

        if len(_timelines_metrics) > 0:
            df_metrics_results = spark.createDataFrame(
                _timelines_metrics, ["entity", "instance", "name", "value"]
            )
            current_time_in_millis = time() * 1000
            partition_year = F.year(F.from_unixtime(F.lit(time())))
            nextdf = (
                df_metrics_results.withColumn("dqts", F.lit(current_time_in_millis))
                .withColumn("dataset", F.lit(domain))
                .withColumn("year", F.lit(partition_year))
            )
            doesTableExist = spark.catalog.tableExists(dq_metrics_table)

            if doesTableExist:
                nextdf.coalesce(1).write.mode("append").format("delta").option(
                    "mergeSchema", "true"
                ).insertInto(dq_metrics_table)
            else:
                nextdf.coalesce(1).write.mode("overwrite").format("delta").saveAsTable(
                    dq_metrics_table
                )
