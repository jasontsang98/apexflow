{{ config(materialized='table') }}

WITH lap_aggregates AS (
    SELECT
        session_key,
        driver_number,
        lap_number,
        lap_duration,
        tire_compound,
        -- Aggregate Telemetry Highlights
        MAX(speed) AS top_speed,
        MIN(speed) AS v_min, -- Minimum speed in a lap (crucial for cornering analysis)
        AVG(throttle) AS avg_throttle,
        AVG(brake) AS avg_brake,
        -- Environment Context
        AVG(track_temperature) AS avg_track_temp,
        MAX(CAST(is_raining AS INT64)) AS rained_during_lap, -- 1 if it rained at any point
        -- Strategy Context
        MAX(current_tire_age) AS tire_age_at_lap_end,
        MAX(pit_stop_duration_seconds) AS pit_stop_time
    FROM {{ ref('fct_telemetry_enriched') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    *,
    -- The "Leaderboard" Logic: Rank every lap in the race by time
    RANK() OVER (
        PARTITION BY session_key 
        ORDER BY lap_duration ASC
    ) AS fastest_lap_rank,
    
    -- Calculate "Throttle Consistency" (Higher is usually better/smoother)
    ROUND(avg_throttle / NULLIF(avg_brake, 0), 2) AS throttle_brake_ratio
FROM lap_aggregates
WHERE lap_duration > 0 -- Filter out any data glitches