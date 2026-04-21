{{ config(materialized='view') }}

WITH raw_events AS (
    SELECT jsonb_array_elements(
        CASE 
            WHEN content ? 'data' AND (content->'data') ? 'events' THEN content->'data'->'events'
            ELSE content->'events'
        END
    ) AS item 
    FROM {{ source('raw_mds', 'VOI_EVENTS') }}
)

SELECT DISTINCT
    item->>'device_id' AS device_id,
    -- Array extraction for event type
    item->'event_types'->>0 AS event_type,
    item->>'vehicle_state' AS vehicle_state,
    
    -- FIX: Extract from the 'trip_ids' array per the JSON sample
    item->'trip_ids'->>0 AS trip_id,
    
    (item->'location'->>'lat')::DOUBLE PRECISION AS lat,
    (item->'location'->>'lng')::DOUBLE PRECISION AS lon,
    -- Convert milliseconds to timestamp
    TO_TIMESTAMP((item->>'timestamp')::BIGINT / 1000.0) AS reported_at,
    'Voi' AS provider_name
FROM raw_events
WHERE item IS NOT NULL