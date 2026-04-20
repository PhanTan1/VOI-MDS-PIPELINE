{{ config(materialized='view') }}

WITH raw_trips AS (
    SELECT jsonb_array_elements(content->'trips') AS item 
    FROM {{ source('raw_mds', 'DOTT_TRIPS') }}
),

-- FIX: Deduplicate the vehicle registry to prevent SQL fan-out
unique_vehicles AS (
    SELECT 
        device_id,
        MAX(vehicle_id) AS vehicle_id,
        MAX(vehicle_type) AS vehicle_type
    FROM {{ ref('stg_dott_vehicles') }}
    GROUP BY 1
)

SELECT DISTINCT
    t.item->>'trip_id' AS trip_id,
    
    -- Join to the deduplicated CTE instead of the raw staging model
    COALESCE(r.vehicle_id, t.item->>'device_id') AS vehicle_short_id,
    COALESCE(r.vehicle_type, 'bicycle') AS vehicle_type,
    'Dott' AS provider_name,
    
    TO_TIMESTAMP((t.item->>'start_time')::bigint / 1000.0) AS start_ts,
    TO_TIMESTAMP((t.item->>'end_time')::bigint / 1000.0) AS end_ts,
    (t.item->>'duration')::numeric AS trip_duration,
    (t.item->>'distance')::float AS trip_distance_meters,
    
    (t.item->'start_location'->>'lat')::float AS start_lat,
    (t.item->'start_location'->>'lng')::float AS start_lon,
    (t.item->'end_location'->>'lat')::float end_lat,
    (t.item->'end_location'->>'lng')::float AS end_lon,

    -- The straight-line fallback
    COALESCE(
        ST_GeomFromGeoJSON(t.item->'route'),
        ST_MakeLine(
            ST_SetSRID(ST_MakePoint((t.item->'start_location'->>'lng')::float, (t.item->'start_location'->>'lat')::float), 4326),
            ST_SetSRID(ST_MakePoint((t.item->'end_location'->>'lng')::float, (t.item->'end_location'->>'lat')::float), 4326)
        )
    )::geometry(LineString, 4326) AS route_geom

FROM raw_trips t
-- Now we safely join 1-to-1 against the squashed CTE
LEFT JOIN unique_vehicles r ON t.item->>'device_id' = r.device_id