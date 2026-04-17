{{ config(materialized='view') }}

WITH raw_trips AS (
    SELECT jsonb_array_elements(
        CASE 
            WHEN content ? 'trips' THEN content->'trips'
            ELSE content->'data'->'trips'
        END
    ) AS item
    FROM {{ source('raw_mds', 'BOLT_TRIPS') }}
)

SELECT DISTINCT
    item->>'trip_id' AS trip_id,
    -- LOOKUP: Use the readable vehicle_id from the registry
    COALESCE(r.vehicle_id, item->>'device_id') AS vehicle_short_id,
    COALESCE(r.vehicle_type, 'scooter') AS vehicle_type,
    'Bolt' AS provider_name,
    -- ... (rest of your existing logic)
    TO_TIMESTAMP((item->>'start_time')::bigint / 1000.0) AS started_at,
    TO_TIMESTAMP((item->>'end_time')::bigint / 1000.0) AS ended_at,
    (item->>'duration')::integer AS duration,
    (item->>'distance')::double precision AS distance,
    (item->'start_location'->>'lat')::float AS start_lat,
    (item->'start_location'->>'lng')::float AS start_lon,
    (item->'end_location'->>'lat')::float AS end_lat,
    (item->'end_location'->>'lng')::float AS end_lon
FROM raw_trips t
LEFT JOIN {{ ref('stg_bolt_vehicles') }} r ON (t.item->>'device_id') = r.device_id