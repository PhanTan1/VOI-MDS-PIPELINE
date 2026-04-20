{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        CASE 
            WHEN content ? 'data' AND (content->'data') ? 'events' THEN content->'data'->'events'
            WHEN content ? 'events' THEN content->'events'
            ELSE '[]'::jsonb
        END AS events_array
    FROM {{ source('raw_mds', 'BOLT_EVENTS') }}
),

unnested_events AS (
    SELECT item
    FROM base_extraction,
    LATERAL jsonb_array_elements(events_array) AS item
)

SELECT
    item->>'device_id' AS device_id,
    item->>'vehicle_state' AS vehicle_state,
    (item->'event_types')->>0 AS event_type,
    
    -- TRIP ID: Pulling the actual ID from the JSON item
    item->>'trip_id' AS trip_id, 
    
    (item->'location'->>'lat')::DOUBLE PRECISION AS lat,
    (item->'location'->>'lng')::DOUBLE PRECISION AS lon,
    TO_TIMESTAMP((item->>'timestamp')::BIGINT / 1000.0) AS reported_at
FROM unnested_events