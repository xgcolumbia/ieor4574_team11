{{ config(materialized='view') }}

SELECT
    session_id,
    item_name,
    first_item_view_ts,
    last_item_view_ts,
    price_per_unit,
    add_to_cart_quantity,
    remove_from_cart_quantity,
    net_cart_quantity,
    net_cart_quantity * price_per_unit AS item_revenue
FROM {{ ref('int_item') }}