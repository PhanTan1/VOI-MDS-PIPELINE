{{ config(materialized='view') }}

WITH raw_trips AS (
    SELECT jsonb_array_elements(content->'trips') AS item 
    FROM {{ source('raw_mds', 'DOTT_TRIPS') }}
)

SELECT DISTINCT
    t.item->>'trip_id' AS trip_id,
    -- Joins to the new registry to get the readable ID!
    COALESCE(r.vehicle_id, t.item->>'device_id') AS vehicle_short_id,
    COALESCE(r.vehicle_type, 'bicycle') AS vehicle_type,
    'Dott' AS provider_name,
    
    TO_TIMESTAMP((t.item->>'start_time')::bigint / 1000.0) AS start_ts,
    TO_TIMESTAMP((t.item->>'end_time')::bigint / 1000.0) AS end_ts,
    (t.item->>'duration')::numeric AS trip_duration,
    (t.item->>'distance')::float AS trip_distance_meters,
    
    (t.item->'start_location'->>'lat')::float AS start_lat,
    (t.item->'start_location'->>'lng')::float AS start_lon,
    (t.item->'end_location'->>'lat')::float AS end_lat,
    (t.item->'end_location'->>'lng')::float AS end_lon,

    -- The straight-line fallback (Telemetry will override this in the Gold layer)
    COALESCE(
        ST_GeomFromGeoJSON(t.item->'route'),
        ST_MakeLine(
            ST_SetSRID(ST_MakePoint((t.item->'start_location'->>'lng')::float, (t.item->'start_location'->>'lat')::float), 4326),
            ST_SetSRID(ST_MakePoint((t.item->'end_location'->>'lng')::float, (t.item->'end_location'->>'lat')::float), 4326)
        )
    )::geometry(LineString, 4326) AS route_geom

FROM raw_trips t
LEFT JOIN {{ ref('stg_dott_vehicles') }} r ON t.item->>'device_id' = r.device_id