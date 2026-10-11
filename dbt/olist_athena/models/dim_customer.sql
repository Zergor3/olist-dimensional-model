-- SCD tipo 1: una fila por persona (customer_unique_id) con la ubicacion de su pedido mas reciente.
WITH customers AS (
    SELECT customer_id, customer_unique_id, customer_city, customer_state
    FROM {{ source('raw', 'customers') }}
),

orders AS (
    SELECT customer_id, order_purchase_timestamp
    FROM {{ source('raw', 'orders') }}
),

ranked AS (
    SELECT
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY CAST(o.order_purchase_timestamp AS timestamp) DESC, c.customer_id
        ) AS rn
    FROM customers c
    JOIN orders o ON o.customer_id = c.customer_id
)

SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY customer_unique_id) AS integer) AS customer_key,
    customer_unique_id,
    customer_city,
    customer_state
FROM ranked
WHERE rn = 1