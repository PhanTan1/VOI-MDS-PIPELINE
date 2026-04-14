{{ config(materialized='view') }}

WITH raw_trips AS (
    SELECT jsonb_array_elements(
        CASE 
            WHEN content ? 'trips' THEN content->'trips'
            WHEN content ? 'data' AND (content->'data') ? 'trips' THEN content->'data'->'trips'
            ELSE '[]'::jsonb
        END
    ) AS item
    FROM {{ source('raw_mds', 'BOLT_TRIPS') }}
)

SELECT DISTINCT
    -- Using the exact key from your snippet: "trip_id"
    (item->>'trip_id')::TEXT AS trip_id,
    (item->>'device_id')::TEXT AS vehicle_id,
    
    (item->>'duration')::INTEGER AS duration,
    (item->>'distance')::DOUBLE PRECISION AS distance,
    
    -- Ensure milliseconds are converted to seconds for TO_TIMESTAMP
    TO_TIMESTAMP((item->>'start_time')::BIGINT / 1000.0) AS started_at,
    TO_TIMESTAMP((item->>'end_time')::BIGINT / 1000.0) AS ended_at,
    
    (item->'start_location'->>'lat')::DOUBLE PRECISION AS start_lat,
    (item->'start_location'->>'lng')::DOUBLE PRECISION AS start_lon,
    (item->'end_location'->>'lat')::DOUBLE PRECISION AS end_lat,
    (item->'end_location'->>'lng')::DOUBLE PRECISION AS end_lon,
    
    item->>'route' AS route_geom

FROM raw_trips
WHERE item IS NOT NULL