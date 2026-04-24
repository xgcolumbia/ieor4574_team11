WITH orders AS (
    SELECT
        order_id,
        session_id,
        client_name,
        order_ts,
        payment_method,
        shipping_cost,
        tax_rate,
        state
    FROM {{ ref('base_web__orders') }}
),

returns_dedup AS (
    SELECT
        order_id,
        returned_at,
        is_refunded,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY returned_at ASC
        ) AS rn
    FROM {{ ref('base_google__returns') }}
),

returns AS (
    SELECT
        order_id,
        returned_at,
        is_refunded
    FROM returns_dedup
    WHERE rn = 1
)

SELECT
    o.order_id,
    o.session_id,
    o.client_name,
    o.order_ts,
    o.payment_method,
    o.shipping_cost,
    o.tax_rate,
    o.state,
    r.returned_at,
    COALESCE(r.is_refunded, FALSE) AS is_refunded
FROM orders o
LEFT JOIN returns r
    ON o.order_id = r.order_id