import requests
import time
import logging

from .utils import get_logger

logger = get_logger("API-Client")

class OpenF1Client:
    BASE_URL = "https://api.openf1.org/v1"
    
    def __init__(self, sustained_delay: float = 2.1):
        # 30 requests per minute = 1 request every 2 seconds.
        # 2.1s gives us a small buffer to avoid 429 errors.
        self.delay = sustained_delay

    def fetch_data(self, endpoint: str, params: dict):
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            # The Throttler
            logger.info(f"⏳ Rate-limit pause ({self.delay}s)...")
            time.sleep(self.delay)
            
            response = requests.get(url, params=params)
            
            # If we STILL hit a 429, implement an exponential backoff
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"🚨 Rate limit hit! Sleeping for {retry_after}s...")
                time.sleep(retry_after)
                return self.fetch_data(endpoint, params) # Retry
                
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API Error: {e}")
            return []

    def get_car_data(self, session_key: int, driver_number: int):
        return self.fetch_data("car_data", {
            "session_key": session_key, 
            "driver_number": driver_number
        })

    def get_car_laps(self, session_key: int, driver_number: int):
        """Fetches the timing and duration for every lap of a driver."""
        return self.fetch_data("laps", {
            "session_key": session_key, 
            "driver_number": driver_number
        })

    def get_meetings(self, year: int = 2025):
        """Fetches all GP events for a given year."""
        return self.fetch_data("meetings", {"year": year})

    def get_sessions(self, year: int = 2025, session_type: str = "Race"):
        """Fetches session info (e.g., only 'Race' sessions for the season)."""
        return self.fetch_data("sessions", {
            "year": year,
            "session_name": session_type
        })

    def get_drivers(self, session_key: int):
        """Fetches the driver grid for a specific session."""
        return self.fetch_data("drivers", {"session_key": session_key})

    def get_stints(self, session_key: int, driver_number: int):
        return self.fetch_data("stints", {
            "session_key": session_key, 
            "driver_number": driver_number
        })

    def get_pit_stops(self, session_key: int, driver_number: int):
        return self.fetch_data("pit", {
            "session_key": session_key, 
            "driver_number": driver_number
        })

    def get_weather(self, session_key: int):
        return self.fetch_data("weather", {"session_key": session_key})

    def get_race_controol(self, session_key: int):
        return self.fetch_data("race_control", {"session_key": session_key})

    def get_positions(self, session_key: int, driver_number: int):
        return self.fetch_data("position", {
            "session_key": session_key, 
            "driver_number": driver_number
        })
        
    def get_locations(self, session_key: int, driver_number: int):
        return self.fetch_data("location", {
            "session_key": session_key, 
            "driver_number": driver_number
        })