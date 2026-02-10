SELECT
    order_id,
    customer_id,
    toDateTime(order_ts) as order_ts,
    status
FROM {{ source('raw', 'raw_orders') }}
