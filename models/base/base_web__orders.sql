SELECT
    ORDER_ID AS order_id,
    SESSION_ID AS session_id,
    CLIENT_NAME AS client_name,
    ORDER_AT AS order_ts,
    LOWER(TRIM(PAYMENT_METHOD)) AS payment_method,
    TRY_CAST(SHIPPING_COST AS DOUBLE) AS shipping_cost,
    TAX_RATE AS tax_rate,
    LOWER(TRIM(STATE)) AS state,
    "_fivetran_synced"

FROM {{ source('web_schema', 'ORDERS') }}

WHERE ORDER_AT IS NOT NULL
AND "_fivetran_deleted" = FALSE