{{ config(materialized='view') }}

WITH raw_status AS (
    SELECT content FROM {{ source('raw_mds', 'VOI_VEHICLES_STATUS') }}
),

unnested_status AS (
    SELECT jsonb_array_elements(content->'vehicles_status') AS item 
    FROM raw_status
)

SELECT DISTINCT
    v.vehicle_id AS vehicle_short_id,
    v.vehicle_type AS vehicle_type,
    s.item->>'device_id' AS device_id,
    s.item->'last_event'->>'vehicle_state' AS vehicle_state,
    
    -- FIX: Ignore historical events to prevent duplicate status rows
    'telemetry' AS event_type,
    NULL AS trip_id,
    
    (s.item->'last_telemetry'->'location'->>'lat')::float AS lat,
    (s.item->'last_telemetry'->'location'->>'lng')::float AS lon,
    TO_TIMESTAMP((s.item->'last_telemetry'->>'timestamp')::bigint / 1000.0) AS reported_at
FROM unnested_status s
LEFT JOIN {{ ref('stg_voi_vehicles') }} v 
    ON s.item->>'device_id' = v.device_id