SELECT
    MD5(CONCAT_WS('|', s.restaurant_id, s.inspection_date::VARCHAR, s.inspection_type, COALESCE(s.violation_code, 'NO_VIOLATION'))) AS fct_violation_key,
    MD5(s.restaurant_id)                AS dim_restaurant_key,
    MD5(COALESCE(s.violation_code,''))  AS dim_violation_key,
    d.date_key                          AS inspection_date_key,
    MD5(CONCAT_WS('|', s.restaurant_id, s.inspection_date::VARCHAR, s.inspection_type)) AS fct_inspection_key,
    s.restaurant_id,
    s.inspection_date,
    s.inspection_type,
    s.action,
    s.violation_code,
    1                                   AS violation_count,
    s.is_critical_violation,
    s.inspection_score
FROM {{ ref('stg_inspections') }} s
LEFT JOIN {{ ref('dim_date') }} d ON s.inspection_date = d.calendar_date
WHERE s.violation_code IS NOT NULL
