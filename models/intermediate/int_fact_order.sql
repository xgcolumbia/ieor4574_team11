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
),

item_revenue AS (
    SELECT
        session_id,
        SUM(price_per_unit * add_to_cart_quantity) AS gross_item_revenue
    FROM {{ ref('base_web__item_views') }}
    GROUP BY session_id
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
    COALESCE(r.is_refunded, FALSE)            AS is_refunded,
    COALESCE(iv.gross_item_revenue, 0)        AS gross_item_revenue,
    COALESCE(iv.gross_item_revenue, 0)
        + o.shipping_cost                     AS revenue_before_tax,
    (COALESCE(iv.gross_item_revenue, 0)
        + o.shipping_cost) * (1 + o.tax_rate) AS total_revenue
FROM orders o
LEFT JOIN returns r
    ON o.order_id = r.order_id
LEFT JOIN item_revenue iv
    ON o.session_id = iv.session_id