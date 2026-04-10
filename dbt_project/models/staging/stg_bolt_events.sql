{{ config(materialized='view') }}

SELECT
    content->>'device_id' AS device_id,
    content->>'vehicle_state' AS vehicle_state,
    (content->'event_types')->>0 AS event_type,
    (content->'location')->>'lat' AS lat,
    (content->'location')->>'lng' AS lon,
    -- Bolt timestamps are in milliseconds, so we convert them to standard timestamps
    TO_TIMESTAMP(CAST(content->>'timestamp' AS BIGINT) / 1000.0) AS reported_at
-- Make sure the source name matches what you use for Voi/Dott (e.g., 'raw' or 'micromobility_raw')
FROM {{ source('raw_mds', 'BOLT_EVENTS') }}