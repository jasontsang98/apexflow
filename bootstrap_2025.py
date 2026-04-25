import json
from ingestors.api_client import OpenF1Client
from ingestors.utils import get_logger
from ingestors.main import upload_to_gcs
from ingestors.schemas import (
    MeetingData, 
    SessionData, 
    WeatherData, 
    RaceControlData,
    DriverData
)

logger = get_logger("Bootstrap-2025")

def run_bootstrap():
    client = OpenF1Client(0.35)
    bucket_name = "apexflow-raw-data"
    year = 2025

    logger.info(f"🏁 Starting 2025 Season Bootstrap...")

    # 1. MEETINGS: Discover the calendar
    raw_meetings = client.get_meetings(year)
    if raw_meetings:
        # Validate the entire list
        validated_meetings = [MeetingData(**m).model_dump(mode='json') for m in raw_meetings]
        
        path = f"bronze/metadata/year={year}/meetings.json"
        upload_to_gcs(bucket_name, path, validated_meetings)
        logger.info(f"✅ Landed {len(validated_meetings)} meetings.")

    # 2. SESSIONS: Get every 'Race' for the year
    raw_sessions = client.get_sessions(year, session_type="Race")
    if raw_sessions:
        validated_sessions = [SessionData(**s).model_dump(mode='json') for s in raw_sessions]
        
        path = f"bronze/metadata/year={year}/sessions.json"
        upload_to_gcs(bucket_name, path, validated_sessions)
        logger.info(f"✅ Landed {len(validated_sessions)} race sessions.")

    # 3. Session-Wide Data
    session_id = 9693
    
    driver = client.get_drivers(session_id)
    validated_drivers = [DriverData(**d).model_dump(mode='json') for d in driver]
    upload_to_gcs(bucket_name, f"bronze/telemetry/session_key={session_id}/drivers.json", validated_drivers)
    logger.info(f"✅ Landed {len(validated_drivers)} drivers.")
    
    weather = client.get_weather(session_id)
    validated_weather = [WeatherData(**w).model_dump(mode='json') for w in weather]
    upload_to_gcs(bucket_name, f"bronze/telemetry/session_key={session_id}/weather.json", validated_weather)
    logger.info(f"✅ Landed {len(validated_weather)} weather records.")
    
    race_control = client.get_race_controol(session_id)
    validated_race_control = [RaceControlData(**rc).model_dump(mode='json') for rc in race_control]
    upload_to_gcs(bucket_name, f"bronze/telemetry/session_key={session_id}/race_control.json", validated_race_control)
    logger.info(f"✅ Landed {len(validated_race_control)} race control records.")


if __name__ == "__main__":
    run_bootstrap()