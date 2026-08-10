import json
import os

from google.cloud import storage

from .api_client import OpenF1Client
from .schemas import (
    DriverData,
    LapData,
    LocationData,
    PitData,
    SessionResultData,
    StintData,
    TelemetryData,
)
from .utils import get_logger

logger = get_logger("Main-Ingestor")


def upload_to_gcs(bucket_name: str, destination_blob_name: str, data: list) -> str:
    """Upload records as NDJSON and return the resulting GCS URI.

    Exceptions deliberately propagate so callers and orchestrators cannot report
    a successful ingestion after a failed write.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    jsonl_lines = []
    for record in data:
        if hasattr(record, "model_dump_json"):
            jsonl_lines.append(record.model_dump_json(exclude_none=True))
        else:
            jsonl_lines.append(json.dumps(record, default=str))

    blob.upload_from_string(
        data="\n".join(jsonl_lines),
        content_type="application/x-ndjson",
    )
    uri = f"gs://{bucket_name}/{destination_blob_name}"
    logger.info("Uploaded: %s", uri)
    return uri


def _ingest_driver_data(
    fetch,
    schema,
    filename: str,
    bucket_name: str,
    session_key: int,
    driver_number: int,
) -> bool:
    raw_data = fetch(session_key, driver_number)
    if not raw_data:
        return False

    validated_data = [schema(**record) for record in raw_data]
    path = (
        f"bronze/telemetry/session_key={session_key}/"
        f"driver_number={driver_number}/{filename}"
    )
    upload_to_gcs(bucket_name, path, validated_data)
    return True

def ingest_session_results(client, bucket_name: str, session_key: int) -> bool:
    raw_data = client.get_session_results(session_key)
    if not raw_data:
        return False

    validated_data = [SessionResultData(**record) for record in raw_data]
    path = f"bronze/telemetry/session_key={session_key}/session_result.json"
    upload_to_gcs(bucket_name, path, validated_data)
    return True


def ingest_driver_telemetry(client, bucket_name, session_key, driver_number):
    return _ingest_driver_data(
        client.get_car_data,
        TelemetryData,
        "car_data.json",
        bucket_name,
        session_key,
        driver_number,
    )


def ingest_driver_laps(client, bucket_name, session_key, driver_number):
    return _ingest_driver_data(
        client.get_car_laps,
        LapData,
        "laps.json",
        bucket_name,
        session_key,
        driver_number,
    )


def ingest_driver_stints(client, bucket_name, session_key, driver_number):
    return _ingest_driver_data(
        client.get_stints,
        StintData,
        "stints.json",
        bucket_name,
        session_key,
        driver_number,
    )


def ingest_driver_pit_stops(client, bucket_name, session_key, driver_number):
    return _ingest_driver_data(
        client.get_pit_stops,
        PitData,
        "pits.json",
        bucket_name,
        session_key,
        driver_number,
    )


def ingest_driver_locations(client, bucket_name, session_key, driver_number):
    return _ingest_driver_data(
        client.get_locations,
        LocationData,
        "locations.json",
        bucket_name,
        session_key,
        driver_number,
    )


def run_ingestion():
    request_delay = float(os.getenv("OPENF1_REQUEST_DELAY", "2.1"))
    request_timeout = float(os.getenv("OPENF1_REQUEST_TIMEOUT", "30"))
    max_retries = int(os.getenv("OPENF1_MAX_RETRIES", "3"))
    bucket_name = os.getenv("APEXFLOW_BUCKET", "apexflow-raw-data")
    session_id = int(os.getenv("APEXFLOW_SESSION_KEY", "9693"))

    client = OpenF1Client(
        sustained_delay=request_delay,
        timeout=request_timeout,
        max_retries=max_retries,
    )
    logger.info("Starting full-grid ingestion for session %d", session_id)

    raw_drivers = client.get_drivers(session_id)
    drivers = [DriverData(**record) for record in raw_drivers]
    if not drivers:
        raise RuntimeError(f"No drivers returned for session {session_id}")

    ingest_session_results(client, bucket_name, session_id)
    tasks = (
        ingest_driver_telemetry,
        ingest_driver_laps,
        ingest_driver_stints,
        ingest_driver_pit_stops,
        ingest_driver_locations,
    )
    for driver in drivers:
        logger.info("Processing driver %d (%s)", driver.driver_number, driver.name_acronym)
        for task in tasks:
            task(client, bucket_name, session_id, driver.driver_number)

    logger.info("Full-grid ingestion complete")


if __name__ == "__main__":
    run_ingestion()
