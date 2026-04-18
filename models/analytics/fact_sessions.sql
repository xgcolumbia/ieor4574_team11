SELECT
    session_id,
    client_id,
    session_ts,

    viewed_shop_plants,
    viewed_item,
    added_to_cart,
    purchased,

    session_stage

FROM {{ ref('int_sessions') }}