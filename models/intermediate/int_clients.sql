SELECT
        s.CLIENT_ID,
        s.SESSION_ID,
        s.SESSION_TS,
        s.OS,
        s.IP,

        o.ORDER_ID,
        o.CLIENT_NAME,
        o.ORDER_TS,
        o.PAYMENT_METHOD,
        o.SHIPPING_COST,
        o.TAX_RATE,
        o.STATE

    FROM {{ ref('base_web__sessions') }} s
    LEFT JOIN {{ ref('base_web__orders') }} o
        ON s.SESSION_ID = o.SESSION_ID