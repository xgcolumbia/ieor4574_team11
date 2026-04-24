{{ config(materialized='view') }}
SELECT
    order_id,
    session_id,
    client_name,
    order_ts,
    CAST(order_ts AS DATE) AS order_date,
    payment_method,
    shipping_cost,
    tax_rate,
    state,
    returned_at,
    CASE
        WHEN returned_at IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_returned_flag,
    is_refunded,
    CASE
        WHEN returned_at IS NOT NULL
        THEN DATEDIFF('day', CAST(order_ts AS DATE), returned_at)
        ELSE NULL
    END AS return_days_after_order
FROM {{ ref('int_fact_order') }}

