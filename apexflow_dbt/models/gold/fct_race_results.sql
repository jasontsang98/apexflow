{{
    config(
        materialized='table',
        cluster_by=['season', 'session_key', 'finishing_position']
    )
}}

SELECT
    results.result_id,
    results.session_key,
    sessions.meeting_key,
    sessions.season,
    sessions.meeting_name,
    sessions.session_name,
    sessions.session_start,
    sessions.circuit_short_name,
    sessions.country_name,
    sessions.location,
    results.driver_number,
    drivers.full_name,
    drivers.name_acronym,
    drivers.team_name,
    drivers.team_colour_hex,
    results.finishing_position,
    results.number_of_laps,
    results.points,
    results.race_duration_seconds,
    results.gap_to_leader,
    results.gap_to_leader_seconds,
    results.result_status,
    results.dnf,
    results.dns,
    results.dsq
FROM {{ ref('stg_session_results') }} AS results
JOIN {{ ref('dim_sessions') }} AS sessions USING (session_key, meeting_key)
JOIN {{ ref('dim_drivers') }} AS drivers USING (session_key, driver_number)
