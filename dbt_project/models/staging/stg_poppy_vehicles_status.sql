{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        jsonb_array_elements(content->'data'->'bikes') AS item 
    FROM {{ source('raw_mds', 'POPPY_FREE_BIKE_STATUS') }}
)

SELECT DISTINCT
    item->>'bike_id' AS device_id,
    item->>'bike_id' AS vehicle_id,
    'car' AS vehicle_type,
    'Poppy' AS provider_name,
    'available' AS vehicle_state,
    'telemetry' AS event_type,
    (item->>'lat')::DOUBLE PRECISION AS lat,
    (item->>'lon')::DOUBLE PRECISION AS lon,
    NULL AS trip_id,
    -- Poppy timestamps are usually epoch seconds
    TO_TIMESTAMP((item->>'last_reported')::BIGINT) AS reported_at
FROM base_extraction
WHERE item IS NOT NULL