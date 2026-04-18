{{ config(materialized='view') }}
SELECT
    MD5(item_name) AS item_id,
    item_name,
    MAX(price_per_unit) AS price_per_unit
FROM {{ ref('int_item') }}
WHERE item_name IS NOT NULL
GROUP BY item_name