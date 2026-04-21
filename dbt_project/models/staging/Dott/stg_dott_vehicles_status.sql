{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        (content->>'last_updated')::BIGINT AS root_timestamp,
        jsonb_array_elements(
            CASE 
                WHEN content ? 'data' AND (content->'data') ? 'vehicles' THEN content->'data'->'vehicles'
                WHEN content ? 'vehicles' THEN content->'vehicles'
                ELSE '[]'::jsonb
            END
        ) AS item 
    FROM {{ source('raw_mds', 'DOTT_VEHICLES_STATUS') }}
)

SELECT DISTINCT
    COALESCE(v.vehicle_id, s.item->>'device_id') AS vehicle_short_id,
    COALESCE(v.vehicle_type, s.item->>'vehicle_type', 'scooter') AS vehicle_type,
    s.item->>'device_id' AS device_id,
    
    -- THE FIX: Support both 1.2 and 2.0 state keys
    COALESCE(s.item->'last_event'->>'vehicle_state', s.item->>'last_vehicle_state') AS vehicle_state,
    COALESCE(s.item->'last_event'->'event_types'->>0, s.item->'last_event_types'->>0, 'telemetry') AS event_type,
    s.item->'last_event'->'trip_ids'->>0 AS trip_id,
    
    -- Coordinate extraction: Handle MDS 1.2 GeoJSON [lon, lat] and 2.0 Location objects
    COALESCE(
        (s.item->'last_telemetry'->'location'->>'lat')::DOUBLE PRECISION,
        (s.item->'last_event'->'location'->>'lat')::DOUBLE PRECISION,
        (s.item->'current_location'->'geometry'->'coordinates'->>1)::DOUBLE PRECISION
    ) AS lat,
    COALESCE(
        (s.item->'last_telemetry'->'location'->>'lng')::DOUBLE PRECISION,
        (s.item->'last_event'->'location'->>'lng')::DOUBLE PRECISION,
        (s.item->'current_location'->'geometry'->'coordinates'->>0)::DOUBLE PRECISION
    ) AS lon,
    
    TO_TIMESTAMP(
        COALESCE(
            (s.item->'current_location'->'properties'->>'timestamp')::BIGINT,
            (s.item->>'last_event_time')::BIGINT, 
            s.root_timestamp
        ) / 1000.0
    ) AS reported_at,
    'Dott' AS provider_name

FROM base_extraction s
LEFT JOIN {{ ref('stg_dott_vehicles') }} v ON s.item->>'device_id' = v.device_id