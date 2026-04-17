WITH ranked AS (

    SELECT
        CAST(CLIENT_ID AS STRING) AS client_id,
        SESSION_ID AS session_id,
        TRY_TO_TIMESTAMP(SESSION_AT) AS session_ts,
        LOWER(TRIM(OS)) AS os,
        IP AS ip,
        "_fivetran_synced",

        ROW_NUMBER() OVER (
            PARTITION BY SESSION_ID
            ORDER BY TRY_TO_TIMESTAMP(SESSION_AT)
        ) AS rn

    FROM {{ source('web_schema', 'SESSIONS') }}

    WHERE "_fivetran_deleted" = FALSE
)

SELECT *
FROM ranked
WHERE rn = 1