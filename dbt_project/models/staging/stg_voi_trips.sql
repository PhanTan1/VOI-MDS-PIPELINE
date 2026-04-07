{{ config(materialized='view') }}

WITH raw_trips AS (
    SELECT jsonb_array_elements(content->'trips') AS item 
    FROM {{ source('raw_voi', 'VOI_TRIPS') }}
),

registry AS (
    SELECT device_id, vehicle_id, vehicle_type 
    FROM {{ ref('stg_voi_vehicles') }}
)

SELECT DISTINCT
    t.item->>'trip_id' AS trip_id,
    r.vehicle_id AS vehicle_short_id,
    r.vehicle_type AS vehicle_type,
    
    -- Extraction
    TO_TIMESTAMP((t.item->>'start_time')::bigint / 1000.0) AS start_ts,
    TO_TIMESTAMP((t.item->>'end_time')::bigint / 1000.0) AS end_ts,
    (t.item->>'duration')::numeric AS trip_duration,
    
    -- Extract raw distance. Postgres ::float handles scientific notation naturally.
    (t.item->>'distance')::float AS trip_distance,
    
    -- Individual coordinates (floats handle '4.305e+00' automatically)
    (t.item->'start_location'->>'lat')::float AS start_lat,
    (t.item->'start_location'->>'lng')::float AS start_lon,
    (t.item->'end_location'->>'lat')::float AS end_lat,
    (t.item->'end_location'->>'lng')::float AS end_lon,
    
    -- Geometry Construction
    COALESCE(
        ST_GeomFromGeoJSON(t.item->'route'),
        ST_MakeLine(
            ST_SetSRID(ST_MakePoint((t.item->'start_location'->>'lng')::float, (t.item->'start_location'->>'lat')::float), 4326),
            ST_SetSRID(ST_MakePoint((t.item->'end_location'->>'lng')::float, (t.item->'end_location'->>'lat')::float), 4326)
        )
    )::geometry(LineString, 4326) AS route_geom

FROM raw_trips t
LEFT JOIN registry r ON (t.item->>'device_id') = r.device_id