SELECT
    order_id,
    product_id,
    qty,
    item_price
FROM {{ source('raw', 'raw_order_items') }}
