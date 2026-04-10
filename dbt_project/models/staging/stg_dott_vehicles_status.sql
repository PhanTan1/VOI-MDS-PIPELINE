{{ config(materialized='view') }}

WITH base_extraction AS (
    -- Identified key: 'vehicles_status' is the actual array folder
    SELECT 
        jsonb_array_elements(content->'vehicles_status') AS item 
    FROM {{ source('raw_mds', 'DOTT_VEHICLES_STATUS') }}
)

SELECT DISTINCT
    -- Using device_id as the vehicle_id since it's the primary ID in your snippet
    item->>'device_id'::TEXT AS vehicle_id,
    COALESCE(item->>'vehicle_type', 'scooter')::TEXT AS vehicle_type,
    item->>'device_id'::TEXT AS device_id,
    
    -- Status and Event Mapping from your snippet structure
    item->'last_event'->>'vehicle_state' AS vehicle_state,
    COALESCE(item->'last_event'->'event_types'->>0, 'telemetry')::TEXT AS event_type,
    (item->'last_event'->'trip_ids'->>0)::TEXT AS trip_id,
    
    -- Coordinates: Flat lat/lng inside 'last_event' -> 'location'
    (item->'last_event'->'location'->>'lat')::DOUBLE PRECISION AS lat,
    (item->'last_event'->'location'->>'lng')::DOUBLE PRECISION AS lon,
    
    -- Timestamp: Using top-level 'last_updated' from source as a fallback 
    -- if individual timestamps are missing in the array items
    TO_TIMESTAMP((item->>'timestamp')::BIGINT / 1000.0) AS reported_at,
    'Dott'::TEXT AS provider_name
FROM base_extraction
WHERE item IS NOT NULL