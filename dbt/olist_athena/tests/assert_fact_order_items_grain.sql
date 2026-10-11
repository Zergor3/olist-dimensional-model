SELECT order_id, order_item_id
FROM {{ ref('fact_order_items') }}
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
