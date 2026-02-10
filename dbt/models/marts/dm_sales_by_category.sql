WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)
SELECT
    p.category,
    sum(i.qty) AS total_qty_sold,
    sum(i.item_price * i.qty) AS total_revenue
FROM items i
JOIN products p ON i.product_id = p.product_id
JOIN orders o ON i.order_id = o.order_id
WHERE o.status = 'paid'
GROUP BY p.category
ORDER BY total_revenue DESC
