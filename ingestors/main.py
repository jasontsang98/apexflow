import json
from google.cloud import storage
from .utils import get_logger
from .api_client import OpenF1Client
from .schemas import (
    TelemetryData, 
    DriverData, 
    LapData, 
    StintData, 
    PitData,
    LocationData
)

logger = get_logger("Main-Ingestor")

def upload_to_gcs(bucket_name: str, destination_blob_name: str, data: list):
    """Uploads data to GCS using Application Default Credentials (ADC)."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        jsonl_lines = []
        for record in data:
            if hasattr(record, 'model_dump_json'):
                # Pydantic v2 path: uses your validators + strips extra nulls
                jsonl_lines.append(record.model_dump_json(exclude_none=True))
            else:
                # Fallback for standard dicts
                jsonl_lines.append(json.dumps(record, default=str))
        
        blob.upload_from_string(
            data="\n".join(jsonl_lines),
            content_type='application/x-ndjson'
        )
        logger.info(f"🚀 Uploaded: gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        logger.error(f"❌ GCS Upload Failed: {e}")

def ingest_driver_telemetry(client, bucket_name, session_key, driver_number):
    """Specific task for high-frequency car sensor data."""
    raw_data = client.get_car_data(session_key, driver_number)
    if raw_data:
        # Validate sample
        validated_data = [TelemetryData(**d) for d in raw_data]
        path = f"bronze/telemetry/session_key={session_key}/driver_number={driver_number}/car_data.json"
        upload_to_gcs(bucket_name, path, validated_data)
        return True
    return False

def ingest_driver_laps(client, bucket_name, session_key, driver_number):
    """Specific task for lap timing metadata."""
    raw_data = client.get_car_laps(session_key, driver_number)
    if raw_data:
        # Validate sample
        validated_data = [LapData(**d) for d in raw_data]
        path = f"bronze/telemetry/session_key={session_key}/driver_number={driver_number}/laps.json"
        upload_to_gcs(bucket_name, path, validated_data)
        return True
    return False

def ingest_driver_stints(client, bucket_name, session_key, driver_number):
    """Specific task for stints metadata."""
    raw_data = client.get_stints(session_key, driver_number)
    if raw_data:
        # Validate sample
        validated_data = [StintData(**d) for d in raw_data]
        path = f"bronze/telemetry/session_key={session_key}/driver_number={driver_number}/stints.json"
        upload_to_gcs(bucket_name, path, validated_data)
        return True
    return False

def ingest_driver_pit_stops(client, bucket_name, session_key, driver_number):
    """Specific task for pit stop metadata."""
    raw_data = client.get_pit_stops(session_key, driver_number)
    if raw_data:
        # Validate sample
        validated_data = [PitData(**d) for d in raw_data]
        path = f"bronze/telemetry/session_key={session_key}/driver_number={driver_number}/pits.json"
        upload_to_gcs(bucket_name, path, validated_data)
        return True
    return False

def ingest_driver_locations(client, bucket_name, session_key, driver_number):
    """Specific task for high-frequency location data."""
    raw_data = client.get_locations(session_key, driver_number)
    if raw_data:
        # Validate sample
        validated_data = [LocationData(**d) for d in raw_data]
        path = f"bronze/telemetry/session_key={session_key}/driver_number={driver_number}/locations.json"
        upload_to_gcs(bucket_name, path, validated_data)
        return True
    return False

def run_ingestion():
    client = OpenF1Client(sustained_delay=0.35)
    bucket_name = "apexflow-raw-data"
    session_id = 9693 # Australian GP 2025
    
    logger.info(f"🏁 Starting Full Grid Ingestion for Session {session_id}")

    # 1. Identify the grid
    raw_drivers = client.get_drivers(session_id)
    drivers = [DriverData(**d) for d in raw_drivers]

    # 2. Combined Driver Loop
    for driver in drivers:
        d_num = driver.driver_number
        logger.info(f"👤 Processing Driver {d_num} ({driver.name_acronym})")
        
        # We run both tasks for the driver in sequence
        ingest_driver_telemetry(client, bucket_name, session_id, d_num)
        ingest_driver_laps(client, bucket_name, session_id, d_num)
        ingest_driver_stints(client, bucket_name, session_id, d_num)
        ingest_driver_pit_stops(client, bucket_name, session_id, d_num)
        ingest_driver_locations(client, bucket_name, session_id, d_num)

    logger.info("🏆 Full grid ingestion (Telemetry + Laps + Locations) complete!")

if __name__ == "__main__":
    run_ingestion()