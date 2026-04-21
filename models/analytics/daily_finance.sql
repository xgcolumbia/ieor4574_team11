{{ config(
    materialized = 'table'
) }}

SELECT
    finance_date,
    item_revenue,
    shipping_revenue,
    tax_amount,

    item_revenue + shipping_revenue + tax_amount AS gross_revenue,

    item_revenue + shipping_revenue + tax_amount - refunded_revenue AS net_revenue,

    refunded_revenue,
    order_count,
    refunded_order_count,

    hr_expense,
    tech_tool_expense,
    warehouse_expense,
    other_expense,
    total_expense,

    (item_revenue + shipping_revenue + tax_amount - refunded_revenue) - total_expense AS profit,

    (item_revenue + shipping_revenue + tax_amount) / NULLIF(order_count, 0) AS avg_order_value

FROM {{ ref('int_finance') }}
ORDER BY finance_date