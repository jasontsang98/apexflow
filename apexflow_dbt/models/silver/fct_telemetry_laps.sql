{{ config(materialized='table') }}

WITH lap_boundaries AS (
    SELECT 
        session_key,
        driver_number,
        lap_number,
        date_start AS lap_start,
        COALESCE(
            LEAD(date_start) OVER (
                PARTITION BY session_key, driver_number 
                ORDER BY lap_number
            ),
            TIMESTAMP_ADD(date_start, INTERVAL CAST(lap_duration * 1000 AS INT64) MILLISECOND)
        ) AS lap_end,
        lap_duration,
        is_pit_out_lap
    FROM {{ source('apexflow_bronze', 'laps_raw') }} -- dbt Dynamic Reference
)

SELECT 
    t.session_key,
    t.driver_number,
    l.lap_number,
    t.date AS telemetry_timestamp,
    t.speed,
    t.rpm,
    t.n_gear,
    t.throttle,
    t.brake,
    t.drs,
    l.is_pit_out_lap,
    l.lap_duration
FROM {{ source('apexflow_bronze', 'telemetry_raw') }} t
JOIN lap_boundaries l 
    ON t.session_key = l.session_key 
    AND t.driver_number = l.driver_number
WHERE t.date >= l.lap_start 
    AND t.date < l.lap_end