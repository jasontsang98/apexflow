{{ config(materialized='table') }}

WITH available_sessions AS (
    SELECT DISTINCT session_key
    FROM {{ ref('fct_lap_leaderboard') }}
)

SELECT
    s.session_key,
    s.meeting_key,
    s.session_name,
    s.session_type,
    s.session_start,
    s.session_end,
    s.season,
    m.meeting_name,
    m.circuit_key,
    m.circuit_short_name,
    m.circuit_type,
    m.country_name,
    m.location,
    m.meeting_start,
    m.meeting_end
FROM {{ ref('stg_sessions') }} AS s
JOIN available_sessions AS available USING (session_key)
LEFT JOIN {{ ref('stg_meetings') }} AS m USING (meeting_key)
