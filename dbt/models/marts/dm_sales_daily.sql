WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
)
SELECT
    toDate(o.order_ts) AS date,
    count(DISTINCT o.order_id) AS total_orders,
    sum(i.item_price * i.qty) AS total_revenue
FROM orders o
JOIN items i ON o.order_id = i.order_id
WHERE o.status = 'paid'
GROUP BY pd.toDate(o.order_ts)
ORDER BY date
