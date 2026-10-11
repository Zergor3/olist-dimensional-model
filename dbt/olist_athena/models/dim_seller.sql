SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY seller_id) AS integer) AS seller_key,
    seller_id,
    seller_city,
    seller_state
FROM {{ source('raw', 'sellers') }}