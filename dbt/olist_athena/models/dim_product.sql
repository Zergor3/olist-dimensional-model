-- OpenCSVSerde devuelve '' (no NULL) en campos vacios, por eso NULLIF antes de COALESCE/CAST.
WITH products AS (
    SELECT
        product_id,
        COALESCE(NULLIF(product_category_name, ''), 'unknown') AS product_category_name,
        TRY_CAST(NULLIF(product_weight_g, '') AS integer)      AS product_weight_g
    FROM {{ source('raw', 'products') }}
)

SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY p.product_id) AS integer) AS product_key,
    p.product_id,
    p.product_category_name,
    COALESCE(
        t.product_category_name_english,
        CASE p.product_category_name
            WHEN 'pc_gamer' THEN 'pc_gamer'
            WHEN 'portateis_cozinha_e_preparadores_de_alimentos' THEN 'portable_kitchen_food_preparers'
        END,
        'unknown'
    ) AS product_category_name_english,
    p.product_weight_g
FROM products p
LEFT JOIN {{ source('raw', 'product_category_name_translation') }} t
    ON t.product_category_name = p.product_category_name