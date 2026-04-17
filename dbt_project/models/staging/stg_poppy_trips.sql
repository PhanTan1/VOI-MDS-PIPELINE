{{ config(materialized='view') }}

WITH raw_extracted AS (
    -- 1. Extract every instance of every trip from the raw JSON
    SELECT 
        item->>'trip_id' AS trip_id,
        item->>'vehicle_id' AS vehicle_short_id,
        item->>'vehicle_type' AS vehicle_type,
        'Poppy' AS provider_name,
        TO_TIMESTAMP((item->>'start_time')::bigint / 1000.0) AS start_ts,
        TO_TIMESTAMP((item->>'end_time')::bigint / 1000.0) AS end_ts,
        (item->>'trip_duration')::numeric AS trip_duration,
        (item->>'trip_distance')::float AS trip_distance_meters,
        
        -- Start/End extraction
        (item->'route'->'features'->0->'geometry'->'coordinates'->0->>1)::float AS start_lat,
        (item->'route'->'features'->0->'geometry'->'coordinates'->0->>0)::float AS start_lon,
        (item->'route'->'features'->0->'geometry'->'coordinates'->-1->>1)::float AS end_lat,
        (item->'route'->'features'->0->'geometry'->'coordinates'->-1->>0)::float AS end_lon,

        -- Extract the actual route geometry
        ST_SetSRID(ST_GeomFromGeoJSON(item->'route'->'features'->0->'geometry'), 4326) AS route_geom,
        
        -- Use load_ts to find the most recent snapshot of this trip
        load_ts
    FROM {{ source('raw_mds', 'POPPY_TRIPS') }},
    jsonb_array_elements(content) AS item
)

-- 2. Deduplicate: Pick only one record per trip_id (the newest one)
SELECT DISTINCT ON (trip_id)
    *
FROM raw_extracted
ORDER BY trip_id, load_ts DESC