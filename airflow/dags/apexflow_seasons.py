"""Discover and ingest selected Formula 1 seasons into ApexFlow Bronze."""

import os
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

BUCKET = os.getenv("APEXFLOW_BUCKET", "apexflow-raw-data")
PROJECT_ROOT = "/opt/airflow/apexflow"


@dag(
    dag_id="apexflow_season_ingestion",
    description="Discover, ingest, and transform selected OpenF1 seasons",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "years": Param(
            [2025],
            type="array",
            items={"type": "integer", "minimum": 2018, "maximum": 2100},
            minItems=1,
            uniqueItems=True,
            title="Seasons to ingest",
            description="Enter one season per line, for example 2024 and 2025.",
        )
    },
    default_args={
        "owner": "apexflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["apexflow", "openf1", "season", "bronze", "dbt"],
)
def apexflow_season_ingestion():
    @task
    def discover_sessions() -> list[int]:
        from ingestors.season import discover_race_sessions

        years = get_current_context()["params"]["years"]
        session_keys = []
        for year in sorted(set(int(value) for value in years)):
            session_keys.extend(discover_race_sessions(year, BUCKET))
        return session_keys

    @task(execution_timeout=timedelta(hours=6))
    def ingest_session(session_key: int) -> dict[str, int]:
        from ingestors.main import ingest_race_session

        return ingest_race_session(session_key, BUCKET)

    @task(execution_timeout=timedelta(hours=2), retries=1)
    def refresh_bigquery_models() -> None:
        subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                f"{PROJECT_ROOT}/apexflow_dbt",
                "--profiles-dir",
                f"{PROJECT_ROOT}/airflow/dbt",
            ],
            check=True,
        )

    # Runtime mapping keeps task count aligned with the selected calendars.
    ingested_sessions = ingest_session.expand(session_key=discover_sessions())
    ingested_sessions >> refresh_bigquery_models()


apexflow_season_ingestion()
