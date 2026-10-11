-- Grano: una fila por pago (order_id, payment_sequential).
SELECT
    p.order_id,
    CAST(p.payment_sequential AS integer)          AS payment_sequential,
    dc.customer_key,
    {{ date_key('o.order_purchase_timestamp') }}   AS purchase_date_key,
    p.payment_type,
    CAST(p.payment_installments AS integer)        AS payment_installments,
    CAST(p.payment_value AS decimal(10,2))         AS payment_value
FROM {{ source('raw', 'order_payments') }} p
JOIN {{ source('raw', 'orders') }} o
    ON o.order_id = p.order_id
JOIN {{ source('raw', 'customers') }} c
    ON c.customer_id = o.customer_id
JOIN {{ ref('dim_customer') }} dc
    ON dc.customer_unique_id = c.customer_unique_id