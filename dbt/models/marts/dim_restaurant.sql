SELECT
    MD5(restaurant_id)          AS dim_restaurant_key,
    restaurant_id,
    restaurant_name,
    borough,
    street_address,
    building_number,
    street_name,
    zipcode,
    phone,
    cuisine_description,
    latitude,
    longitude,
    neighborhood_code
FROM {{ ref('stg_restaurants') }}
