SELECT
    _file,
    _line,
    _modified,
    _fivetran_synced,
    CAST(employee_id AS VARCHAR)    AS employee_id,
    TRY_TO_DATE(quit_date)          AS quit_date
FROM {{ source('google_drive', 'HR_QUITS') }}
WHERE TRY_TO_DATE(quit_date) IS NOT NULL
    AND employee_id IS NOT NULL