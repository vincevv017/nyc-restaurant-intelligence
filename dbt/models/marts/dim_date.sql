WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2000-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 11323))
)
SELECT
    calendar_date,
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD'))       AS date_key,
    YEAR(calendar_date)                                 AS year_number,
    QUARTER(calendar_date)                              AS quarter_number,
    YEAR(calendar_date) * 100 + QUARTER(calendar_date)  AS year_quarter,
    'Q' || QUARTER(calendar_date)::VARCHAR               AS quarter_label,
    MONTH(calendar_date)                                AS month_number,
    YEAR(calendar_date) * 100 + MONTH(calendar_date)    AS year_month,
    TO_CHAR(calendar_date, 'MMMM')                      AS month_name,
    TO_CHAR(calendar_date, 'MON')                       AS month_short,
    WEEKOFYEAR(calendar_date)                           AS week_of_year,
    DAYOFWEEK(calendar_date)                            AS day_of_week,
    TO_CHAR(calendar_date, 'DY')                        AS day_name_short,
    DAY(calendar_date)                                  AS day_of_month,
    DAYOFYEAR(calendar_date)                            AS day_of_year,
    CASE WHEN DAYOFWEEK(calendar_date) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN calendar_date = DATE_TRUNC('MONTH', calendar_date) THEN TRUE ELSE FALSE END AS is_month_start,
    CASE WHEN calendar_date = LAST_DAY(calendar_date) THEN TRUE ELSE FALSE END AS is_month_end
FROM date_spine
