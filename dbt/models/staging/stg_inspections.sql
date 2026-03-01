WITH source AS (
    SELECT * FROM {{ source('raw', 'inspections_raw') }}
),
cleaned AS (
    SELECT
        CAMIS                                                   AS restaurant_id,
        TRIM(UPPER(DBA))                                        AS restaurant_name,
        INITCAP(TRIM(BORO))                                     AS borough,
        TRIM(BUILDING)                                          AS building_number,
        INITCAP(TRIM(STREET))                                   AS street_name,
        LPAD(TRIM(ZIPCODE), 5, '0')                             AS zipcode,
        TRIM(PHONE)                                             AS phone,
        INITCAP(TRIM(CUISINE_DESCRIPTION))                      AS cuisine_description,
        TRY_TO_DECIMAL(TRIM(LATITUDE),  10, 7)                  AS latitude,
        TRY_TO_DECIMAL(TRIM(LONGITUDE), 10, 7)                  AS longitude,
        TRIM(NTA)                                               AS neighborhood_code,
        TRY_TO_DATE(INSPECTION_DATE, 'MM/DD/YYYY')              AS inspection_date,
        TRIM(INSPECTION_TYPE)                                   AS inspection_type,
        TRIM(ACTION)                                            AS action,
        TRIM(VIOLATION_CODE)                                    AS violation_code,
        TRIM(VIOLATION_DESCRIPTION)                             AS violation_description,
        CASE
            WHEN UPPER(TRIM(CRITICAL_FLAG)) = 'CRITICAL'     THEN TRUE
            WHEN UPPER(TRIM(CRITICAL_FLAG)) = 'NOT CRITICAL' THEN FALSE
            ELSE NULL
        END                                                     AS is_critical_violation,
        TRY_TO_NUMBER(TRIM(SCORE))                              AS inspection_score,
        NULLIF(TRIM(GRADE), '')                                 AS grade,
        TRY_TO_DATE(GRADE_DATE, 'MM/DD/YYYY')                   AS grade_date,
        TRY_TO_DATE(RECORD_DATE, 'MM/DD/YYYY')                  AS record_date,
        _LOADED_AT                                              AS loaded_at
    FROM source
    WHERE CAMIS IS NOT NULL AND TRIM(CAMIS) != ''
)
SELECT * FROM cleaned
