SELECT
    _file,
    _line,
    _modified,
    _fivetran_synced,
    TRY_TO_DATE(returned_at)                    AS returned_at,
    order_id,
    CASE
        WHEN LOWER(TRIM(is_refunded)) = 'yes' THEN TRUE
        WHEN LOWER(TRIM(is_refunded)) = 'no'  THEN FALSE
        ELSE NULL
    END                                          AS is_refunded
FROM {{ source('google_drive', 'RETURNS') }}
WHERE TRY_TO_DATE(returned_at) IS NOT NULL
    AND order_id IS NOT NULL