SELECT
    session_id,
    item_view_ts,
    item_name,
    price_per_unit,
    add_to_cart_quantity,
    remove_from_cart_quantity,
    add_to_cart_quantity - remove_from_cart_quantity AS net_cart_quantity
FROM DEV.YITONG_BASE.BASE_WEB__ITEM_VIEWS