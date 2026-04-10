{{ config(materialized='view') }}

WITH base_extraction AS (
    -- Safely identify the trips array
    SELECT 
        CASE 
            WHEN content ? 'data' AND (content->'data') ? 'trips' THEN content->'data'->'trips'
            WHEN content ? 'trips' THEN content->'trips'
            ELSE '[]'::jsonb
        END AS trips_array
    FROM {{ source('raw_mds', 'BOLT_TRIPS') }}
),

unnested_trips AS (
    -- Unnest the array laterally
    SELECT item
    FROM base_extraction,
    LATERAL jsonb_array_elements(trips_array) AS item
)

SELECT DISTINCT
    -- Extract the verified trip_id
    (item->>'trip_id')::TEXT AS trip_id,
    (item->>'device_id')::TEXT AS vehicle_id,
    
    (item->>'duration')::INTEGER AS duration,
    (item->>'distance')::DOUBLE PRECISION AS distance,
    
    TO_TIMESTAMP((item->>'start_time')::BIGINT / 1000.0) AS started_at,
    TO_TIMESTAMP((item->>'end_time')::BIGINT / 1000.0) AS ended_at,
    
    (item->'start_location'->>'lat')::DOUBLE PRECISION AS start_lat,
    (item->'start_location'->>'lng')::DOUBLE PRECISION AS start_lon,
    (item->'end_location'->>'lat')::DOUBLE PRECISION AS end_lat,
    (item->'end_location'->>'lng')::DOUBLE PRECISION AS end_lon,
    
    item->>'route' AS route_geom

FROM unnested_trips
WHERE item IS NOT NULL