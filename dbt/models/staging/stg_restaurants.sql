WITH ranked AS (
    SELECT
        restaurant_id, restaurant_name, borough, building_number,
        street_name, zipcode, phone, cuisine_description,
        latitude, longitude, neighborhood_code,
        ROW_NUMBER() OVER (
            PARTITION BY restaurant_id
            ORDER BY record_date DESC NULLS LAST, loaded_at DESC
        ) AS rn
    FROM {{ ref('stg_inspections') }}
    WHERE restaurant_id IS NOT NULL
)
SELECT
    restaurant_id, restaurant_name, borough, building_number,
    street_name, zipcode, phone, cuisine_description,
    latitude, longitude, neighborhood_code,
    CONCAT_WS(' ', building_number, street_name) AS street_address
FROM ranked WHERE rn = 1
