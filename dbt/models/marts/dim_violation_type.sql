SELECT
    MD5(violation_code)         AS dim_violation_key,
    violation_code,
    violation_description,
    is_critical_violation,
    CASE
        WHEN is_critical_violation = TRUE  THEN 'Critical'
        WHEN is_critical_violation = FALSE THEN 'Not Critical'
        ELSE 'Not Applicable'
    END                         AS criticality_label
FROM {{ ref('stg_violations') }}
