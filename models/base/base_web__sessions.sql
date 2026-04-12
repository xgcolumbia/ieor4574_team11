SELECT
    CAST(CLIENT_ID AS STRING) AS client_id,
    SESSION_ID AS session_id,
    TRY_TO_TIMESTAMP(SESSION_AT) AS session_ts,
    LOWER(TRIM(OS)) AS os,
    IP AS ip,
    "_fivetran_synced"
FROM {{ source('web_schema', 'SESSIONS') }}
WHERE "_fivetran_deleted" = FALSE