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

## Validation

Run the unit tests:

```bash
uv run python -m unittest discover -s tests -v
```

Compile the dbt project:

```bash
uv run dbt compile --project-dir apexflow_dbt
```
