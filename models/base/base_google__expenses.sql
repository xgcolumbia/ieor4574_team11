SELECT
    _file,
    _line,
    _modified,
    _fivetran_synced,
    TRY_TO_DATE(date) AS expense_date,
    LOWER(TRIM(expense_type)) AS expense_type,
    TRY_CAST(
        TRIM(REPLACE(expense_amount, '$', '')) AS DOUBLE
    ) AS expense_amount
FROM {{ source('google_drive', 'EXPENSES') }}
WHERE TRY_TO_DATE(date) IS NOT NULL
    AND TRY_CAST(
        TRIM(REPLACE(expense_amount, '$', '')) AS DOUBLE
    ) IS NOT NULL