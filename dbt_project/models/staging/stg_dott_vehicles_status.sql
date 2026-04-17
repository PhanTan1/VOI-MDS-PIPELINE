{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        (content->>'last_updated')::BIGINT AS root_timestamp,
        jsonb_array_elements(content->'vehicles_status') AS item 
    FROM {{ source('raw_mds', 'DOTT_VEHICLES_STATUS') }}
)

SELECT DISTINCT
    -- 1. Grab readable IDs from the registry join
    COALESCE(v.vehicle_id, s.item->>'device_id') AS vehicle_short_id,
    COALESCE(v.vehicle_type, 'scooter') AS vehicle_type,
    
    s.item->>'device_id' AS device_id,
    s.item->'last_event'->>'vehicle_state' AS vehicle_state,
    COALESCE(s.item->'last_event'->'event_types'->>0, 'telemetry') AS event_type,
    s.item->'last_event'->'trip_ids'->>0 AS trip_id,
    
    -- 2. MDS 2.0 Schema Logic: Prefer telemetry location over event location
    COALESCE(
        (s.item->'last_telemetry'->'location'->>'lat')::DOUBLE PRECISION,
        (s.item->'last_event'->'location'->>'lat')::DOUBLE PRECISION
    ) AS lat,
    COALESCE(
        (s.item->'last_telemetry'->'location'->>'lng')::DOUBLE PRECISION,
        (s.item->'last_event'->'location'->>'lng')::DOUBLE PRECISION
    ) AS lon,
    
    -- 3. Timestamp Hierarchy: Telemetry -> Event -> Root
    TO_TIMESTAMP(
        COALESCE(
            (s.item->'last_telemetry'->>'timestamp')::BIGINT,
            (s.item->'last_event'->>'timestamp')::BIGINT, 
            (s.item->>'timestamp')::BIGINT,
            s.root_timestamp
        ) / 1000.0
    ) AS reported_at,
    'Dott' AS provider_name

FROM base_extraction s
LEFT JOIN {{ ref('stg_dott_vehicles') }} v 
    ON s.item->>'device_id' = v.device_id
WHERE s.item IS NOT NULL