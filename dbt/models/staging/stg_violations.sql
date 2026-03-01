WITH distinct_violations AS (
    SELECT DISTINCT violation_code, violation_description, is_critical_violation
    FROM {{ ref('stg_inspections') }}
    WHERE violation_code IS NOT NULL AND TRIM(violation_code) != ''
),
prioritised AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY violation_code
            ORDER BY is_critical_violation DESC NULLS LAST
        ) AS rn
    FROM distinct_violations
)
SELECT violation_code, violation_description, is_critical_violation
FROM prioritised WHERE rn = 1
