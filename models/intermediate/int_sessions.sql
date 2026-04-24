WITH session_flags AS (

    SELECT
        s.session_id,
        s.client_id,
        s.session_ts,

        MAX(CASE WHEN pv.page_name = 'shop_plants' THEN 1 ELSE 0 END) AS viewed_shop_plants,

        MAX(CASE WHEN iv.item_name IS NOT NULL THEN 1 ELSE 0 END) AS viewed_item,

        MAX(CASE WHEN iv.add_to_cart_quantity > 0 THEN 1 ELSE 0 END) AS added_to_cart,

        MAX(CASE WHEN o.order_id IS NOT NULL THEN 1 ELSE 0 END) AS purchased

    FROM {{ ref('base_web__sessions') }} s

    LEFT JOIN {{ ref('base_web__page_views') }} pv
        ON s.session_id = pv.session_id

    LEFT JOIN {{ ref('base_web__item_views') }} iv
        ON s.session_id = iv.session_id

    LEFT JOIN {{ ref('base_web__orders') }} o
        ON s.session_id = o.session_id

    GROUP BY 1,2,3
)

SELECT *,
    CASE 
        WHEN purchased = 1 THEN 'purchased'
        WHEN added_to_cart = 1 THEN 'cart'
        WHEN viewed_item = 1 THEN 'item_view'
        WHEN viewed_shop_plants = 1 THEN 'browse'
        ELSE 'bounce'
    END AS session_stage

FROM session_flags
ORDER BY session_ts DESC