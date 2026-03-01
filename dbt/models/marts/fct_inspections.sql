WITH inspection_base AS (
    SELECT
        restaurant_id, inspection_date, inspection_type, action, grade, grade_date,
        MAX(inspection_score)                                           AS inspection_score,
        COUNT(violation_code)                                           AS total_violations,
        COUNT(CASE WHEN is_critical_violation = TRUE  THEN 1 END)      AS critical_violations,
        COUNT(CASE WHEN is_critical_violation = FALSE THEN 1 END)      AS non_critical_violations
    FROM {{ ref('stg_inspections') }}
    WHERE inspection_date IS NOT NULL
      AND inspection_type IS NOT NULL   -- exclude new establishments not yet inspected
    GROUP BY restaurant_id, inspection_date, inspection_type, action, grade, grade_date
)
SELECT
    -- action added to key: same restaurant/date/type can have multiple actions
    MD5(CONCAT_WS('|', i.restaurant_id, i.inspection_date::VARCHAR, i.inspection_type, COALESCE(i.action,''))) AS fct_inspection_key,
    MD5(i.restaurant_id)        AS dim_restaurant_key,
    d.date_key                  AS inspection_date_key,
    i.restaurant_id,
    i.inspection_date,
    i.inspection_type,
    i.action,
    i.grade,
    i.grade_date,
    CASE i.grade WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 ELSE NULL END AS grade_sort_order,
    i.inspection_score,
    i.total_violations,
    i.critical_violations,
    i.non_critical_violations,
    CASE WHEN i.grade = 'A'             THEN TRUE ELSE FALSE END AS passed_with_grade_a,
    CASE WHEN i.critical_violations > 0 THEN TRUE ELSE FALSE END AS has_critical_violation
FROM inspection_base i
LEFT JOIN {{ ref('dim_date') }} d       ON i.inspection_date = d.calendar_date
LEFT JOIN {{ ref('dim_restaurant') }} r ON i.restaurant_id   = r.restaurant_id
