{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        CASE 
            WHEN content ? 'data' AND (content->'data') ? 'events' THEN content->'data'->'events'
            WHEN content ? 'events' THEN content->'events'
            ELSE '[]'::jsonb
        END AS events_array
    FROM {{ source('raw_mds', 'DOTT_EVENTS') }}
),

unnested_events AS (
    SELECT item
    FROM base_extraction,
    LATERAL jsonb_array_elements(events_array) AS item
)

SELECT DISTINCT
    item->>'event_id' AS event_id,
    item->>'device_id' AS device_id,
    item->>'vehicle_state' AS vehicle_state,
    
    item->'event_types'->>0 AS event_type,
    item->'trip_ids'->>0 AS trip_id,
    
    (item->'location'->>'lat')::DOUBLE PRECISION AS lat,
    (item->'location'->>'lng')::DOUBLE PRECISION AS lon,
    (item->>'battery_percent')::INTEGER AS battery_percent,
    
    TO_TIMESTAMP((item->>'timestamp')::BIGINT / 1000.0) AS reported_at,
    'Dott' AS provider_name

FROM unnested_events
WHERE item IS NOT NULL