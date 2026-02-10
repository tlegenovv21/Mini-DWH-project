WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
customer_orders AS (
    SELECT
        customer_id,
        count(DISTINCT order_id) AS total_orders,
        min(order_ts) as first_order_date,
        max(order_ts) as last_order_date
    FROM orders
    WHERE status = 'paid'
    GROUP BY customer_id
),
customer_revenue AS (
    SELECT
        o.customer_id,
        sum(i.item_price * i.qty) AS total_spend
    FROM orders o
    JOIN items i ON o.order_id = i.order_id
    WHERE o.status = 'paid'
    GROUP BY o.customer_id
)
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    co.total_orders,
    cr.total_spend,
    co.first_order_date,
    co.last_order_date
FROM customers c
LEFT JOIN customer_orders co ON c.customer_id = co.customer_id
LEFT JOIN customer_revenue cr ON c.customer_id = cr.customer_id
