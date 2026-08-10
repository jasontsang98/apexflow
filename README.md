# ApexFlow

ApexFlow ingests Formula 1 telemetry from OpenF1 into Google Cloud Storage and
uses BigQuery and dbt to build Silver and Gold analytics models.

## Ingestion configuration

The ingestion entry point uses Application Default Credentials for GCS and
supports these environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `APEXFLOW_BUCKET` | `apexflow-raw-data` | Bronze GCS bucket |
| `APEXFLOW_SESSION_KEY` | `9693` | OpenF1 session to ingest |
| `OPENF1_REQUEST_DELAY` | `2.1` | Delay before each API request, in seconds |
| `OPENF1_REQUEST_TIMEOUT` | `30` | Per-request timeout, in seconds |
| `OPENF1_MAX_RETRIES` | `3` | Retries for rate limits and transient failures |

Run the ingestion from the repository root:

```bash
uv run python -m ingestors.main
```

## Airflow: ingest one or more seasons

The `apexflow_season_ingestion` DAG accepts a validated list of years when it
is triggered, discovers the official race sessions from OpenF1, and maps one
observable task per race. Race tasks run serially to respect the shared OpenF1
rate limit. Writes use deterministic GCS paths, so failed tasks can be retried
without creating duplicate objects. After every race succeeds, Airflow runs
`dbt build` to refresh and test the BigQuery Silver and Gold models.

The local stack follows the Airflow 3.1 Docker layout and requires Docker
Compose plus Google Application Default Credentials:

```bash
gcloud auth application-default login
gcloud config set project apexflow-f1
mkdir -p airflow/logs
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose -f docker-compose.airflow.yml build
docker compose -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.airflow.yml up -d
```

Read the generated local login password:

```bash
cat airflow/logs/simple_auth_manager_passwords.json.generated
```

Open `http://localhost:8080`, sign in as `airflow`, unpause
`apexflow_season_ingestion`, and trigger it. The form defaults to `[2025]`;
enter another year or multiple years as needed. The DAG is manual because
these runs are historical backfills, not a recurring feed.

If ADC is stored elsewhere, add its absolute path to `.env`:

```bash
GOOGLE_APPLICATION_CREDENTIALS_HOST_PATH=/absolute/path/application_default_credentials.json
```

Useful commands:

```bash
docker compose -f docker-compose.airflow.yml ps
docker compose -f docker-compose.airflow.yml logs -f airflow-scheduler
docker compose -f docker-compose.airflow.yml down
```

## Validation

Run the unit tests:

```bash
uv run python -m unittest discover -s tests -v
```

Compile the dbt project:

```bash
uv run dbt compile --project-dir apexflow_dbt
```

## Dashboard

The Streamlit dashboard reads from the analytics-ready BigQuery Gold layer.
Application Default Credentials must have permission to run BigQuery jobs and
read the `apexflow_gold` and `apexflow_silver` datasets.

Run it from the repository root:

```bash
uv run streamlit run dashboard/app.py
```

By default the dashboard uses the `apexflow-f1` Google Cloud project. Override
it when needed:

```bash
export APEXFLOW_BQ_PROJECT="your-project-id"
uv run streamlit run dashboard/app.py
```

BigQuery results are cached for 15 minutes to limit repeated query costs.
