{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        jsonb_array_elements(
            CASE 
                -- THE FIX: Extracting from the data.status_changes array
                WHEN content ? 'data' AND (content->'data') ? 'status_changes' THEN content->'data'->'status_changes'
                ELSE '[]'::jsonb
            END
        ) AS item
    FROM {{ source('raw_mds', 'DOTT_EVENTS') }}
)

SELECT DISTINCT
    item->>'device_id' AS device_id,
    item->>'vehicle_id' AS vehicle_short_id,
    item->>'vehicle_type' AS vehicle_type,
    item->>'vehicle_state' AS vehicle_state,
    
    -- Extract the primary event type from the array
    (item->'event_types')->>0 AS event_type,
    
    item->>'trip_id' AS trip_id,
    
    -- GeoJSON Coordinates: Index 0 is Longitude, Index 1 is Latitude
    (item->'event_location'->'geometry'->'coordinates'->>1)::DOUBLE PRECISION AS lat,
    (item->'event_location'->'geometry'->'coordinates'->>0)::DOUBLE PRECISION AS lon,
    
    (item->>'battery_pct')::DOUBLE PRECISION * 100 AS battery_percent,
    
    -- Convert milliseconds to timestamp
    TO_TIMESTAMP((item->>'event_time')::BIGINT / 1000.0) AS reported_at,
    'Dott' AS provider_name
FROM base_extraction
WHERE item IS NOT NULL