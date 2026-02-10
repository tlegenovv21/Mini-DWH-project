SELECT
    product_id,
    trim(name) as name,
    trim(category) as category,
    price
FROM {{ source('raw', 'raw_products') }}
