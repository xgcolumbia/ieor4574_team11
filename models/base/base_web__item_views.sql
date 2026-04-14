SELECT
    CAST("_fivetran_id" AS STRING) AS item_view_id,
    SESSION_ID AS session_id,
    ITEM_VIEW_AT AS item_view_ts,
    LOWER(TRIM(ITEM_NAME)) AS item_name,
    TRY_CAST(PRICE_PER_UNIT AS DOUBLE) AS price_per_unit,
    ADD_TO_CART_QUANTITY AS add_to_cart_quantity,
    REMOVE_FROM_CART_QUANTITY AS remove_from_cart_quantity,
    "_fivetran_synced"

FROM {{ source('web_schema', 'ITEM_VIEWS') }}

WHERE ITEM_VIEW_AT IS NOT NULL
AND "_fivetran_deleted" = FALSE