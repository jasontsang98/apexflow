SELECT *
FROM {{ ref('fct_dashboard_laps') }}
WHERE
    lap_duration <= 0
    OR delta_to_session_best < 0
    OR delta_to_driver_best < 0
    OR top_speed < 0
    OR v_min < 0
