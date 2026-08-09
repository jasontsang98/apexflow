import time
from collections.abc import Callable

import requests

from .utils import get_logger

logger = get_logger("API-Client")


class OpenF1Client:
    BASE_URL = "https://api.openf1.org/v1"
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(
        self,
        sustained_delay: float = 2.1,
        timeout: float = 30.0,
        max_retries: int = 3,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if sustained_delay < 0:
            raise ValueError("sustained_delay must be non-negative")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")

        self.delay = sustained_delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.sleep = sleep

    def _backoff_seconds(self, attempt: int, response=None) -> float:
        if response is not None and response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    return max(0.0, float(retry_after))
                except ValueError:
                    logger.warning("Invalid Retry-After header %r; using backoff", retry_after)

        return min(float(2**attempt), 60.0)

    def fetch_data(self, endpoint: str, params: dict) -> list:
        url = f"{self.BASE_URL}/{endpoint}"

        for attempt in range(self.max_retries + 1):
            if self.delay:
                logger.info("Rate-limit pause (%.2fs)...", self.delay)
                self.sleep(self.delay)

            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except requests.exceptions.RequestException as exc:
                if attempt == self.max_retries:
                    logger.error("OpenF1 request failed after %d attempts: %s", attempt + 1, exc)
                    return []

                backoff = self._backoff_seconds(attempt)
                logger.warning("OpenF1 request failed; retrying in %.1fs: %s", backoff, exc)
                self.sleep(backoff)
                continue

            if response.status_code in self.RETRYABLE_STATUS_CODES:
                if attempt == self.max_retries:
                    logger.error(
                        "OpenF1 returned HTTP %d after %d attempts",
                        response.status_code,
                        attempt + 1,
                    )
                    return []

                backoff = self._backoff_seconds(attempt, response)
                logger.warning(
                    "OpenF1 returned HTTP %d; retrying in %.1fs",
                    response.status_code,
                    backoff,
                )
                self.sleep(backoff)
                continue

            try:
                response.raise_for_status()
                payload = response.json()
            except (requests.exceptions.RequestException, ValueError) as exc:
                logger.error("Invalid OpenF1 response: %s", exc)
                return []

            if not isinstance(payload, list):
                logger.error("OpenF1 returned %s instead of a list", type(payload).__name__)
                return []
            return payload


    def get_car_data(self, session_key: int, driver_number: int):
        return self.fetch_data("car_data", {
            "session_key": session_key,
            "driver_number": driver_number,
        })

    def get_car_laps(self, session_key: int, driver_number: int):
        return self.fetch_data("laps", {
            "session_key": session_key,
            "driver_number": driver_number,
        })

    def get_meetings(self, year: int = 2025):
        return self.fetch_data("meetings", {"year": year})

    def get_sessions(self, year: int = 2025, session_type: str = "Race"):
        return self.fetch_data("sessions", {
            "year": year,
            "session_name": session_type,
        })

    def get_drivers(self, session_key: int):
        return self.fetch_data("drivers", {"session_key": session_key})

    def get_stints(self, session_key: int, driver_number: int):
        return self.fetch_data("stints", {
            "session_key": session_key,
            "driver_number": driver_number,
        })

    def get_pit_stops(self, session_key: int, driver_number: int):
        return self.fetch_data("pit", {
            "session_key": session_key,
            "driver_number": driver_number,
        })

    def get_weather(self, session_key: int):
        return self.fetch_data("weather", {"session_key": session_key})

    def get_race_control(self, session_key: int):
        return self.fetch_data("race_control", {"session_key": session_key})

    def get_positions(self, session_key: int, driver_number: int):
        return self.fetch_data("position", {
            "session_key": session_key,
            "driver_number": driver_number,
        })

    def get_locations(self, session_key: int, driver_number: int):
        return self.fetch_data("location", {
            "session_key": session_key,
            "driver_number": driver_number,
        })
