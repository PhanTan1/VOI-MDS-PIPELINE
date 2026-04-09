{{ config(materialized='view') }}

WITH raw_status AS (
    SELECT content FROM {{ source('raw_mds', 'DOTT_VEHICLES_STATUS') }}
),

unnested_status AS (
    -- Dott wraps the status array inside 'data' -> 'vehicles'
    SELECT jsonb_array_elements(content->'data'->'vehicles') AS item 
    FROM raw_status
)

SELECT DISTINCT
    s.item->>'vehicle_id' AS vehicle_short_id,
    COALESCE(s.item->>'vehicle_type', 'scooter') AS vehicle_type,
    s.item->>'device_id' AS device_id,
    s.item->'last_event'->>'vehicle_state' AS vehicle_state,
    s.item->'last_event'->'event_types'->>0 AS event_type,
    s.item->'last_event'->'trip_ids'->>0 AS trip_id,
    
    -- Dott GeoJSON coordinates are [lon, lat]
    (s.item->'last_telemetry'->'location'->'geometry'->'coordinates'->>1)::float AS lat,
    (s.item->'last_telemetry'->'location'->'geometry'->'coordinates'->>0)::float AS lon,
    
    -- Convert ms to timestamp
    TO_TIMESTAMP((s.item->'last_telemetry'->>'timestamp')::bigint / 1000.0) AS reported_at,
    'Dott' AS provider_name
FROM unnested_status s