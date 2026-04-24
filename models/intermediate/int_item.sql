WITH item_session_rollup AS (
    SELECT
        session_id,
        item_name,
        MIN(item_view_ts) AS first_item_view_ts,
        MAX(item_view_ts) AS last_item_view_ts,
        MAX(price_per_unit) AS price_per_unit,
        SUM(add_to_cart_quantity) AS add_to_cart_quantity,
        SUM(remove_from_cart_quantity) AS remove_from_cart_quantity
    FROM {{ ref('base_web__item_views') }}
    WHERE item_name IS NOT NULL
    GROUP BY
        session_id,
        item_name
)

SELECT
    session_id,
    item_name,
    first_item_view_ts,
    last_item_view_ts,
    price_per_unit,
    add_to_cart_quantity,
    remove_from_cart_quantity,
    add_to_cart_quantity - remove_from_cart_quantity AS net_cart_quantity
FROM item_session_rollup