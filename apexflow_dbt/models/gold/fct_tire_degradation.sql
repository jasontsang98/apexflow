{{
    config(
        materialized='table',
        cluster_by=['session_key', 'driver_number', 'tire_compound']
    )
}}

WITH eligible_laps AS (
    SELECT *
    FROM {{ ref('fct_dashboard_laps') }}
    WHERE
        NOT is_pit_out_lap
        AND pit_stop_time IS NULL
        AND tire_compound IS NOT NULL
        AND tire_age_at_lap_end IS NOT NULL
),

stint_boundaries AS (
    SELECT
        *,
        CASE
            WHEN LAG(tire_compound) OVER driver_laps IS NULL THEN 1
            WHEN tire_compound != LAG(tire_compound) OVER driver_laps THEN 1
            WHEN tire_age_at_lap_end <= LAG(tire_age_at_lap_end) OVER driver_laps THEN 1
            ELSE 0
        END AS starts_new_stint
    FROM eligible_laps
    WINDOW driver_laps AS (
        PARTITION BY session_key, driver_number
        ORDER BY lap_number
    )
),

numbered_stints AS (
    SELECT
        *,
        SUM(starts_new_stint) OVER (
            PARTITION BY session_key, driver_number
            ORDER BY lap_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS strategy_stint_number
    FROM stint_boundaries
)

SELECT
    CONCAT(lap_id, '-stint-', CAST(strategy_stint_number AS STRING)) AS degradation_id,
    lap_id,
    driver_session_id,
    session_key,
    driver_number,
    full_name,
    name_acronym,
    team_name,
    team_colour_hex,
    lap_number,
    strategy_stint_number,
    ROW_NUMBER() OVER stint_laps AS lap_in_stint,
    tire_compound,
    tire_age_at_lap_end,
    lap_duration,
    lap_duration - MIN(lap_duration) OVER stint_laps AS delta_to_stint_best,
    AVG(lap_duration) OVER (
        PARTITION BY session_key, driver_number, strategy_stint_number
        ORDER BY lap_number
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_three_lap_average,
    avg_track_temp,
    rained_during_lap
FROM numbered_stints
WINDOW stint_laps AS (
    PARTITION BY session_key, driver_number, strategy_stint_number
    ORDER BY lap_number
)
