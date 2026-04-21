{{ config(
    materialized = 'table'
) }}

WITH purchased_sessions AS (

    SELECT
        session_id
    FROM {{ ref('int_sessions') }}
    WHERE session_stage = 'purchased'

),

session_item_revenue AS (

    SELECT
        session_id,

        SUM(
            GREATEST(
                COALESCE(add_to_cart_quantity,0)
                - COALESCE(remove_from_cart_quantity,0),
                0
            )
        ) AS net_item_quantity,

        SUM(
            GREATEST(
                COALESCE(add_to_cart_quantity,0)
                - COALESCE(remove_from_cart_quantity,0),
                0
            ) * COALESCE(price_per_unit,0)
        ) AS item_revenue

    FROM {{ ref('base_web__item_views') }}

    GROUP BY session_id

),

order_level_revenue AS (

    SELECT

        CAST(o.order_ts AS DATE) AS finance_date,
        o.order_id,
        o.is_refunded,

        COALESCE(sir.item_revenue,0) AS item_revenue,

        COALESCE(o.shipping_cost,0) AS shipping_revenue,

        COALESCE(sir.item_revenue,0) * COALESCE(o.tax_rate,0) AS tax_amount

    FROM {{ ref('int_fact_order') }} o

    INNER JOIN purchased_sessions ps
        ON o.session_id = ps.session_id

    LEFT JOIN session_item_revenue sir
        ON o.session_id = sir.session_id

),

daily_order_revenue AS (

    SELECT

        finance_date,

        SUM(item_revenue) AS item_revenue,

        SUM(shipping_revenue) AS shipping_revenue,

        SUM(tax_amount) AS tax_amount,

        SUM(
            CASE
                WHEN is_refunded = TRUE
                THEN item_revenue + shipping_revenue + tax_amount
                ELSE 0
            END
        ) AS refunded_revenue,

        COUNT(DISTINCT order_id) AS order_count,

        COUNT(
            DISTINCT CASE
                WHEN is_refunded = TRUE
                THEN order_id
            END
        ) AS refunded_order_count

    FROM order_level_revenue

    GROUP BY finance_date

),

daily_expenses AS (

    SELECT

        expense_date AS finance_date,

        SUM(
            CASE WHEN LOWER(expense_type) = 'hr'
            THEN expense_amount ELSE 0 END
        ) AS hr_expense,

        SUM(
            CASE WHEN LOWER(expense_type) = 'tech tool'
            THEN expense_amount ELSE 0 END
        ) AS tech_tool_expense,

        SUM(
            CASE WHEN LOWER(expense_type) = 'warehouse'
            THEN expense_amount ELSE 0 END
        ) AS warehouse_expense,

        SUM(
            CASE WHEN LOWER(expense_type) = 'other'
            THEN expense_amount ELSE 0 END
        ) AS other_expense,

        SUM(expense_amount) AS total_expense

    FROM {{ ref('base_google__expenses') }}

    GROUP BY expense_date

),

final AS (

    SELECT

        COALESCE(r.finance_date, e.finance_date) AS finance_date,

        COALESCE(r.item_revenue,0) AS item_revenue,
        COALESCE(r.shipping_revenue,0) AS shipping_revenue,
        COALESCE(r.tax_amount,0) AS tax_amount,
        COALESCE(r.refunded_revenue,0) AS refunded_revenue,

        COALESCE(r.order_count,0) AS order_count,
        COALESCE(r.refunded_order_count,0) AS refunded_order_count,

        COALESCE(e.hr_expense,0) AS hr_expense,
        COALESCE(e.tech_tool_expense,0) AS tech_tool_expense,
        COALESCE(e.warehouse_expense,0) AS warehouse_expense,
        COALESCE(e.other_expense,0) AS other_expense,

        COALESCE(e.total_expense,0) AS total_expense

    FROM daily_order_revenue r

    FULL OUTER JOIN daily_expenses e
        ON r.finance_date = e.finance_date

)

SELECT *
FROM final
ORDER BY finance_date