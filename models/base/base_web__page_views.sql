SELECT
    CAST("_fivetran_id" AS STRING) AS page_view_id,
    SESSION_ID AS session_id,
    VIEW_AT AS view_ts,
    LOWER(TRIM(PAGE_NAME)) AS page_name,
    "_fivetran_synced"

FROM {{ source('web_schema', 'PAGE_VIEWS') }}

WHERE VIEW_AT IS NOT NULL
AND "_fivetran_deleted" = FALSE