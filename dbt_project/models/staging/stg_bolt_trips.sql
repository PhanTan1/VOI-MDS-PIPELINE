{{ config(materialized='view') }}

WITH raw_trips AS (
    -- Safely unnest the JSON array of trips from the raw Bolt payload
    SELECT jsonb_array_elements(
        CASE 
            WHEN content ? 'data' THEN content->'data'->'trips'
            WHEN content ? 'trips' THEN content->'trips'
            ELSE '[]'::jsonb
        END
    ) AS item
    FROM {{ source('raw_mds', 'BOLT_TRIPS') }}
)

SELECT DISTINCT
    item->>'trip_id' AS trip_id,
    item->>'device_id' AS vehicle_id, -- Standardized column name
    
    -- Cast metrics to standard numeric types
    (item->>'duration')::INTEGER AS duration,
    (item->>'distance')::DOUBLE PRECISION AS distance,
    
    -- Bolt uses timestamps in milliseconds
    TO_TIMESTAMP((item->>'start_time')::BIGINT / 1000.0) AS started_at,
    TO_TIMESTAMP((item->>'end_time')::BIGINT / 1000.0) AS ended_at,
    
    -- Extract Geo coordinates
    (item->'start_location'->>'lat')::DOUBLE PRECISION AS start_lat,
    (item->'start_location'->>'lng')::DOUBLE PRECISION AS start_lon,
    (item->'end_location'->>'lat')::DOUBLE PRECISION AS end_lat,
    (item->'end_location'->>'lng')::DOUBLE PRECISION AS end_lon,
    
    -- Extract GeoJSON route
    item->>'route' AS route_geom

FROM raw_trips