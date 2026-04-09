{{ config(materialized='view') }}

WITH raw_trips AS (
    SELECT jsonb_array_elements(content->'trips') AS item 
    FROM {{ source('raw_mds', 'BOLT_TRIPS') }}
)

SELECT DISTINCT
    item->>'trip_id' AS trip_id,
    -- Bolt often includes vehicle_id directly in the trip payload
    COALESCE(item->>'vehicle_id', item->>'device_id') AS vehicle_short_id,
    item->>'vehicle_type' AS vehicle_type,
    'Bolt' AS provider_name,
    
    TO_TIMESTAMP((item->>'start_time')::bigint / 1000.0) AS start_ts,
    TO_TIMESTAMP((item->>'end_time')::bigint / 1000.0) AS end_ts,
    (item->>'duration')::numeric AS trip_duration,
    (item->>'distance')::float AS trip_distance_meters,
    
    (item->'start_location'->>'lat')::float AS start_lat,
    (item->'start_location'->>'lng')::float AS start_lon,
    (item->'end_location'->>'lat')::float AS end_lat,
    (item->'end_location'->>'lng')::float AS end_lon,

    COALESCE(
        ST_GeomFromGeoJSON(item->'route'),
        ST_MakeLine(
            ST_SetSRID(ST_MakePoint((item->'start_location'->>'lng')::float, (item->'start_location'->>'lat')::float), 4326),
            ST_SetSRID(ST_MakePoint((item->'end_location'->>'lng')::float, (item->'end_location'->>'lat')::float), 4326)
        )
    )::geometry(LineString, 4326) AS route_geom

FROM raw_trips