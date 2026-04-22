{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        file_ts, 
        (content->>'last_updated')::BIGINT AS root_timestamp,
        jsonb_array_elements(content->'data'->'bikes') AS item 
    FROM {{ source('raw_mds', 'POPPY_FREE_BIKE_STATUS') }}
)

SELECT DISTINCT
    s.item->>'bike_id' AS device_id,
    s.item->>'bike_id' AS vehicle_id,
    
    -- FIX: Pull the form_factor from the reference table, default to 'car' just in case
    COALESCE(vt.vehicle_type, 'car') AS vehicle_type,
    
    'Poppy' AS provider_name,
    'available' AS vehicle_state,
    'unspecified' AS event_type,
    (s.item->>'lat')::DOUBLE PRECISION AS lat,
    (s.item->>'lon')::DOUBLE PRECISION AS lon,
    NULL AS trip_id,
    
    -- The Ironclad Timestamp Fallback
    COALESCE(
        TO_TIMESTAMP((s.item->>'last_reported')::BIGINT),
        TO_TIMESTAMP(s.root_timestamp),
        s.file_ts
    ) AS reported_at

FROM base_extraction s
-- JOINING THE NEW REFERENCE TABLE HERE
LEFT JOIN {{ ref('stg_poppy_vehicles') }} vt 
    ON s.item->>'vehicle_type_id' = vt.vehicle_type_id
WHERE s.item IS NOT NULL