import os
import re
from collections.abc import Sequence

import pandas as pd
from google.cloud import bigquery

DEFAULT_PROJECT = "apexflow-f1"
_PROJECT_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


def dashboard_project() -> str:
    project = os.getenv("APEXFLOW_BQ_PROJECT", DEFAULT_PROJECT)
    if not _PROJECT_PATTERN.fullmatch(project):
        raise ValueError("APEXFLOW_BQ_PROJECT is not a valid Google Cloud project ID")
    return project


class DashboardRepository:
    def __init__(self, client: bigquery.Client | None = None, project: str | None = None):
        self.project = project or dashboard_project()
        if not _PROJECT_PATTERN.fullmatch(self.project):
            raise ValueError("project is not a valid Google Cloud project ID")
        self.client = client or bigquery.Client(project=self.project)

    def _query(self, sql: str, parameters: Sequence[bigquery.ScalarQueryParameter] = ()) -> pd.DataFrame:
        config = bigquery.QueryJobConfig(query_parameters=list(parameters))
        return self.client.query(sql, job_config=config).to_dataframe()

    def sessions(self) -> pd.DataFrame:
        return self._query(f"""
            SELECT
                session_key,
                meeting_name,
                session_name,
                season,
                circuit_short_name,
                country_name,
                location,
                session_start
            FROM `{self.project}.apexflow_gold.dim_sessions`
            ORDER BY session_start DESC
        """)

    def season_races(self, season: int) -> pd.DataFrame:
        return self._query(
            f"""
                SELECT
                    laps.session_key,
                    ANY_VALUE(laps.meeting_name) AS meeting_name,
                    ANY_VALUE(laps.circuit_short_name) AS circuit_short_name,
                    ANY_VALUE(laps.country_name) AS country_name,
                    ANY_VALUE(laps.location) AS location,
                    ANY_VALUE(sessions.session_start) AS session_start,
                    COUNT(DISTINCT laps.driver_number) AS driver_count,
                    COUNT(*) AS lap_count,
                    MIN(laps.lap_duration) AS fastest_lap,
                    ARRAY_AGG(
                        laps.name_acronym ORDER BY laps.lap_duration LIMIT 1
                    )[OFFSET(0)] AS fastest_driver,
                    MAX(laps.top_speed) AS top_speed,
                    LOGICAL_OR(laps.rained_during_lap) AS was_wet
                FROM `{self.project}.apexflow_gold.fct_dashboard_laps` AS laps
                JOIN `{self.project}.apexflow_gold.dim_sessions` AS sessions
                    USING (session_key)
                WHERE laps.season = @season
                GROUP BY laps.session_key
                ORDER BY session_start
            """,
            [bigquery.ScalarQueryParameter("season", "INT64", season)],
        )

    def season_drivers(self, season: int) -> pd.DataFrame:
        return self._query(
            f"""
                SELECT
                    driver_number,
                    ANY_VALUE(full_name) AS full_name,
                    ANY_VALUE(name_acronym) AS name_acronym,
                    ANY_VALUE(team_name) AS team_name,
                    ANY_VALUE(team_colour_hex) AS team_colour_hex,
                    COUNT(DISTINCT session_key) AS races,
                    COUNT(*) AS laps,
                    MIN(lap_duration) AS fastest_lap,
                    AVG(delta_to_driver_best) AS average_delta_to_best,
                    MAX(top_speed) AS top_speed
                FROM `{self.project}.apexflow_gold.fct_dashboard_laps`
                WHERE season = @season
                GROUP BY driver_number
                ORDER BY fastest_lap
            """,
            [bigquery.ScalarQueryParameter("season", "INT64", season)],
        )

    def season_winners(self, season: int) -> pd.DataFrame:
        return self._query(
            f"""
                SELECT
                    driver_number,
                    ANY_VALUE(full_name) AS full_name,
                    ANY_VALUE(name_acronym) AS name_acronym,
                    ANY_VALUE(team_name) AS team_name,
                    ANY_VALUE(team_colour_hex) AS team_colour_hex,
                    COUNTIF(finishing_position = 1) AS wins,
                    COUNTIF(finishing_position <= 3) AS podiums,
                    SUM(points) AS points
                FROM `{self.project}.apexflow_gold.fct_race_results`
                WHERE season = @season
                GROUP BY driver_number
                HAVING wins > 0
                ORDER BY wins DESC, points DESC
            """,
            [bigquery.ScalarQueryParameter("season", "INT64", season)],
        )

    def race_results(self, session_key: int) -> pd.DataFrame:
        return self._query(
            f"""
                SELECT
                    result_id,
                    session_key,
                    driver_number,
                    full_name,
                    name_acronym,
                    team_name,
                    team_colour_hex,
                    finishing_position,
                    number_of_laps,
                    points,
                    gap_to_leader,
                    result_status
                FROM `{self.project}.apexflow_gold.fct_race_results`
                WHERE session_key = @session_key
                ORDER BY finishing_position
            """,
            [bigquery.ScalarQueryParameter("session_key", "INT64", session_key)],
        )

    def drivers(self, session_key: int) -> pd.DataFrame:
        return self._query(
            f"""
                SELECT
                    driver_session_id,
                    session_key,
                    driver_number,
                    full_name,
                    name_acronym,
                    team_name,
                    team_colour_hex
                FROM `{self.project}.apexflow_gold.dim_drivers`
                WHERE session_key = @session_key
                ORDER BY team_name, driver_number
            """,
            [bigquery.ScalarQueryParameter("session_key", "INT64", session_key)],
        )

    def laps(self, session_key: int, driver_numbers: Sequence[int]) -> pd.DataFrame:
        if not driver_numbers:
            return pd.DataFrame()
        return self._query(
            f"""
                SELECT *
                FROM `{self.project}.apexflow_gold.fct_dashboard_laps`
                WHERE
                    session_key = @session_key
                    AND driver_number IN UNNEST(@driver_numbers)
                ORDER BY lap_number, driver_number
            """,
            [
                bigquery.ScalarQueryParameter("session_key", "INT64", session_key),
                bigquery.ArrayQueryParameter("driver_numbers", "INT64", list(driver_numbers)),
            ],
        )

    def tire_degradation(self, session_key: int, driver_numbers: Sequence[int]) -> pd.DataFrame:
        if not driver_numbers:
            return pd.DataFrame()
        return self._query(
            f"""
                SELECT *
                FROM `{self.project}.apexflow_gold.fct_tire_degradation`
                WHERE
                    session_key = @session_key
                    AND driver_number IN UNNEST(@driver_numbers)
                ORDER BY driver_number, lap_number
            """,
            [
                bigquery.ScalarQueryParameter("session_key", "INT64", session_key),
                bigquery.ArrayQueryParameter("driver_numbers", "INT64", list(driver_numbers)),
            ],
        )

    def v_min_points(self, session_key: int, driver_numbers: Sequence[int]) -> pd.DataFrame:
        if not driver_numbers:
            return pd.DataFrame()
        return self._query(
            f"""
                SELECT
                    session_key,
                    driver_number,
                    lap_number,
                    v_min_timestamp,
                    lap_v_min,
                    v_min_gear,
                    v_min_throttle,
                    v_min_brake,
                    x,
                    y,
                    tire_compound,
                    current_tire_age,
                    track_flag
                FROM `{self.project}.apexflow_gold.fct_corner_performance`
                WHERE
                    session_key = @session_key
                    AND driver_number IN UNNEST(@driver_numbers)
                ORDER BY driver_number, lap_number
            """,
            [
                bigquery.ScalarQueryParameter("session_key", "INT64", session_key),
                bigquery.ArrayQueryParameter("driver_numbers", "INT64", list(driver_numbers)),
            ],
        )

    def telemetry(self, session_key: int, driver_number: int, lap_number: int) -> pd.DataFrame:
        return self._query(
            f"""
                SELECT
                    telemetry_timestamp,
                    speed,
                    rpm,
                    n_gear,
                    throttle,
                    brake,
                    drs,
                    x,
                    y
                FROM `{self.project}.apexflow_silver.fct_telemetry_enriched`
                WHERE
                    session_key = @session_key
                    AND driver_number = @driver_number
                    AND lap_number = @lap_number
                ORDER BY telemetry_timestamp
            """,
            [
                bigquery.ScalarQueryParameter("session_key", "INT64", session_key),
                bigquery.ScalarQueryParameter("driver_number", "INT64", driver_number),
                bigquery.ScalarQueryParameter("lap_number", "INT64", lap_number),
            ],
        )

    def fastest_lap_telemetry(
        self,
        session_key: int,
        driver_numbers: Sequence[int],
    ) -> pd.DataFrame:
        if not driver_numbers:
            return pd.DataFrame()
        return self._query(
            f"""
                WITH fastest_laps AS (
                    SELECT
                        driver_number,
                        ARRAY_AGG(
                            lap_number ORDER BY lap_duration, lap_number LIMIT 1
                        )[OFFSET(0)] AS lap_number
                    FROM `{self.project}.apexflow_gold.fct_dashboard_laps`
                    WHERE
                        session_key = @session_key
                        AND driver_number IN UNNEST(@driver_numbers)
                        AND lap_duration IS NOT NULL
                        AND NOT is_pit_out_lap
                    GROUP BY driver_number
                )
                SELECT
                    telemetry.telemetry_timestamp,
                    telemetry.driver_number,
                    drivers.full_name,
                    drivers.name_acronym,
                    drivers.team_colour_hex,
                    telemetry.lap_number,
                    telemetry.speed,
                    telemetry.throttle,
                    telemetry.brake
                FROM `{self.project}.apexflow_silver.fct_telemetry_enriched` AS telemetry
                JOIN fastest_laps USING (driver_number, lap_number)
                JOIN `{self.project}.apexflow_gold.dim_drivers` AS drivers
                    USING (session_key, driver_number)
                WHERE telemetry.session_key = @session_key
                ORDER BY telemetry.driver_number, telemetry.telemetry_timestamp
            """,
            [
                bigquery.ScalarQueryParameter("session_key", "INT64", session_key),
                bigquery.ArrayQueryParameter("driver_numbers", "INT64", list(driver_numbers)),
            ],
        )
