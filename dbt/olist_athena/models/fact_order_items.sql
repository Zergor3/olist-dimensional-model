-- Grano: una fila por (order_id, order_item_id).
SELECT
    i.order_id,
    CAST(i.order_item_id AS integer)                       AS order_item_id,
    dc.customer_key,
    dp.product_key,
    ds.seller_key,
    {{ date_key('o.order_purchase_timestamp') }}           AS purchase_date_key,
    {{ date_key('o.order_approved_at') }}                  AS approved_date_key,
    {{ date_key('o.order_delivered_carrier_date') }}       AS shipped_date_key,
    {{ date_key('o.order_delivered_customer_date') }}      AS delivered_date_key,
    {{ date_key('o.order_estimated_delivery_date') }}      AS estimated_delivery_date_key,
    o.order_status,
    CAST(i.price AS decimal(10,2))                         AS price,
    CAST(i.freight_value AS decimal(10,2))                 AS freight_value
FROM {{ source('raw', 'order_items') }} i
JOIN {{ source('raw', 'orders') }} o
    ON o.order_id = i.order_id
JOIN {{ source('raw', 'customers') }} c
    ON c.customer_id = o.customer_id
JOIN {{ ref('dim_customer') }} dc
    ON dc.customer_unique_id = c.customer_unique_id
JOIN {{ ref('dim_product') }} dp
    ON dp.product_id = i.product_id
JOIN {{ ref('dim_seller') }} ds
    ON ds.seller_id = i.seller_id