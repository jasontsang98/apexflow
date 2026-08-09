{{
    config(
        materialized='table',
        cluster_by=['session_key', 'driver_number']
    )
}}

SELECT
    CONCAT(
        CAST(laps.session_key AS STRING), '-',
        CAST(laps.driver_number AS STRING), '-',
        CAST(laps.lap_number AS STRING)
    ) AS lap_id,
    drivers.driver_session_id,
    laps.session_key,
    sessions.meeting_key,
    laps.driver_number,
    laps.lap_number,
    drivers.full_name,
    drivers.name_acronym,
    drivers.team_name,
    drivers.team_colour_hex,
    sessions.meeting_name,
    sessions.session_name,
    sessions.season,
    sessions.circuit_short_name,
    sessions.country_name,
    sessions.location,
    laps.lap_duration,
    laps.fastest_lap_rank,
    RANK() OVER (
        PARTITION BY laps.session_key, laps.driver_number
        ORDER BY laps.lap_duration
    ) AS driver_fastest_lap_rank,
    laps.lap_duration - MIN(laps.lap_duration) OVER (
        PARTITION BY laps.session_key
    ) AS delta_to_session_best,
    laps.lap_duration - MIN(laps.lap_duration) OVER (
        PARTITION BY laps.session_key, laps.driver_number
    ) AS delta_to_driver_best,
    laps.top_speed,
    laps.v_min,
    laps.avg_throttle,
    laps.avg_brake,
    laps.throttle_brake_ratio,
    laps.tire_compound,
    laps.tire_age_at_lap_end,
    laps.is_pit_out_lap,
    laps.pit_stop_time,
    laps.avg_track_temp,
    laps.rained_during_lap
FROM {{ ref('fct_lap_leaderboard') }} AS laps
JOIN {{ ref('dim_drivers') }} AS drivers
    USING (session_key, driver_number)
JOIN {{ ref('dim_sessions') }} AS sessions
    USING (session_key)
