{{ config(materialized='view') }}

WITH raw_events AS (
    SELECT jsonb_array_elements(content->'events') AS item 
    FROM {{ source('raw_mds', 'DOTT_EVENTS') }}
)

SELECT DISTINCT
    item->>'event_id' AS event_id,
    item->>'device_id' AS device_id,
    item->>'vehicle_state' AS vehicle_state,
    
    -- Extract the primary event type from the array (e.g., 'trip_start')
    item->'event_types'->>0 AS event_type,
    
    -- Extract the first trip_id if it exists
    item->'trip_ids'->>0 AS trip_id,
    
    (item->'location'->>'lat')::DOUBLE PRECISION AS lat,
    (item->'location'->>'lng')::DOUBLE PRECISION AS lon,
    (item->>'battery_percent')::INTEGER AS battery_percent,
    
    -- Convert milliseconds to timestamp
    TO_TIMESTAMP((item->>'timestamp')::BIGINT / 1000.0) AS reported_at,
    'Dott' AS provider_name

FROM raw_events
WHERE item IS NOT NULL