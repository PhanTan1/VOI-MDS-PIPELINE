{{ config(materialized='view') }}

WITH raw_trips AS (
    SELECT content FROM {{ source('raw_voi', 'VOI_TRIPS') }}
),

unnested_trips AS (
    SELECT jsonb_array_elements(content->'trips') AS item 
    FROM raw_trips
)

SELECT DISTINCT
    t.item->>'trip_id' AS trip_id,
    t.item->>'device_id' AS device_id,
    
    -- Stealing the readable ID and Type from your Vehicles table!
    v.vehicle_id AS vehicle_short_id,
    v.vehicle_type AS vehicle_type,
    
    TO_TIMESTAMP((t.item->>'start_time')::bigint / 1000.0) AS start_ts,
    TO_TIMESTAMP((t.item->>'end_time')::bigint / 1000.0) AS end_ts,
    (t.item->>'duration')::numeric AS trip_duration,
    (t.item->>'distance')::numeric AS trip_distance,
    
    (t.item->'start_location'->>'lat')::float AS start_lat,
    (t.item->'start_location'->>'lng')::float AS start_lon,
    (t.item->'end_location'->>'lat')::float AS end_lat,
    (t.item->'end_location'->>'lng')::float AS end_lon,
    
    COALESCE(
        ST_GeomFromGeoJSON(t.item->'route'),
        ST_MakeLine(
            ST_SetSRID(ST_MakePoint((t.item->'start_location'->>'lng')::float, (t.item->'start_location'->>'lat')::float), 4326),
            ST_SetSRID(ST_MakePoint((t.item->'end_location'->>'lng')::float, (t.item->'end_location'->>'lat')::float), 4326)
        )
    )::geometry(LineString, 4326) AS route_geom
FROM unnested_trips t
LEFT JOIN {{ ref('stg_voi_vehicles') }} v 
    ON t.item->>'device_id' = v.device_id