SELECT *
FROM {{ ref('fct_tire_degradation') }}
WHERE
    strategy_stint_number < 1
    OR lap_in_stint < 1
    OR tire_age_at_lap_end < 0
    OR delta_to_stint_best < 0
    OR rolling_three_lap_average <= 0
