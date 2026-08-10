"""Season discovery shared by Airflow and command-line ingestion."""

from .api_client import OpenF1Client
from .main import create_client_from_env, upload_to_gcs
from .schemas import MeetingData, SessionData
from .utils import get_logger

logger = get_logger("Season-Ingestor")


def discover_race_sessions(
    year: int,
    bucket_name: str,
    client: OpenF1Client | None = None,
) -> list[int]:
    """Land calendar metadata and return the year's unique race session keys."""
    if year < 2018:
        raise ValueError("OpenF1 season discovery supports years from 2018")

    client = client or create_client_from_env()
    raw_meetings = client.get_meetings(year)
    raw_sessions = client.get_sessions(year, session_type="Race")
    if not raw_meetings:
        raise RuntimeError(f"No meetings returned for {year}")
    if not raw_sessions:
        raise RuntimeError(f"No race sessions returned for {year}")

    meetings = [MeetingData(**record) for record in raw_meetings]
    sessions = [SessionData(**record) for record in raw_sessions]
    if any(session.year != year for session in sessions):
        raise ValueError(f"OpenF1 returned race sessions outside requested year {year}")

    upload_to_gcs(
        bucket_name,
        f"bronze/metadata/year={year}/meetings.json",
        meetings,
    )
    upload_to_gcs(
        bucket_name,
        f"bronze/metadata/year={year}/sessions.json",
        sessions,
    )

    session_keys = sorted({session.session_key for session in sessions})
    logger.info("Discovered %d race sessions for %d", len(session_keys), year)
    return session_keys
