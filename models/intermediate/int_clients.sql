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

    FROM DEV.SABRINA_BASE.BASE_WEB__SESSIONS s
    LEFT JOIN DEV.SABRINA_BASE.BASE_WEB__ORDERS o
        ON s.SESSION_ID = o.SESSION_ID