{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        -- Grab the root timestamp as a fallback
        (content->>'last_updated')::BIGINT AS root_timestamp,
        jsonb_array_elements(content->'vehicles_status') AS item 
    FROM {{ source('raw_mds', 'DOTT_VEHICLES_STATUS') }}
)

SELECT DISTINCT
    item->>'device_id'::TEXT AS vehicle_id,
    COALESCE(item->>'vehicle_type', 'scooter')::TEXT AS vehicle_type,
    item->>'device_id'::TEXT AS device_id,
    item->'last_event'->>'vehicle_state' AS vehicle_state,
    COALESCE(item->'last_event'->'event_types'->>0, 'telemetry')::TEXT AS event_type,
    (item->'last_event'->'trip_ids'->>0)::TEXT AS trip_id,
    
    (item->'last_event'->'location'->>'lat')::DOUBLE PRECISION AS lat,
    (item->'last_event'->'location'->>'lng')::DOUBLE PRECISION AS lon,
    
    -- TIMESTAMP FIX: If the item lacks a timestamp, use the root 'last_updated'
    TO_TIMESTAMP(
        COALESCE(
            (item->>'timestamp')::BIGINT, 
            (item->'last_event'->>'timestamp')::BIGINT,
            root_timestamp
        ) / 1000.0
    ) AS reported_at,
    'Dott'::TEXT AS provider_name
FROM base_extraction
WHERE item IS NOT NULL