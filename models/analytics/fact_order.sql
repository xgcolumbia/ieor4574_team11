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
    is_refunded
FROM DEV.XINAN_INTERMEDIATE.INT_FACT_ORDER