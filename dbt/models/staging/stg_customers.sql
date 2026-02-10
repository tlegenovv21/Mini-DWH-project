SELECT
    customer_id,
    trim(full_name) as full_name,
    lower(trim(email)) as email,
    toDateTime(created_at) as created_at
FROM {{ source('raw', 'raw_customers') }}
