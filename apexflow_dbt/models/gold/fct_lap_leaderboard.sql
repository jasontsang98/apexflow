{{
    config(
        materialized='table',
        cluster_by=['session_key', 'driver_number']
    )
}}

WITH lap_aggregates AS (
    SELECT
        session_key,
        driver_number,
        lap_number,
        lap_duration,
        tire_compound,
        LOGICAL_OR(is_pit_out_lap) AS is_pit_out_lap,
        MAX(speed) AS top_speed,
        MIN(speed) AS v_min,
        AVG(throttle) AS avg_throttle,
        AVG(brake) AS avg_brake,
        AVG(track_temperature) AS avg_track_temp,
        LOGICAL_OR(is_raining) AS rained_during_lap,
        MAX(current_tire_age) AS tire_age_at_lap_end,
        MAX(pit_stop_duration_seconds) AS pit_stop_time
    FROM {{ ref('fct_telemetry_enriched') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    *,
    RANK() OVER (
        PARTITION BY session_key
        ORDER BY lap_duration ASC
    ) AS fastest_lap_rank,
    ROUND(avg_throttle / NULLIF(avg_brake, 0), 2) AS throttle_brake_ratio
FROM lap_aggregates
WHERE lap_duration > 0
