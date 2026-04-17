{{ config(materialized='view') }}

WITH unnested_pings AS (
    SELECT 
        item->>'device_id' AS device_id,
        (item->>'timestamp')::BIGINT AS ping_ts,
        (item->'location'->>'lat')::FLOAT AS lat,
        (item->'location'->>'lng')::FLOAT AS lon,
        item->'trip_ids'->>0 AS trip_id
    FROM (
        SELECT jsonb_array_elements(content->'telemetry') AS item
        FROM {{ source('raw_mds', 'DOTT_TELEMETRY') }}
    ) raw
    WHERE item->'trip_ids' IS NOT NULL 
)

SELECT
    trip_id,
    ST_MakeLine(
        ST_SetSRID(ST_MakePoint(lon, lat), 4326) 
        ORDER BY ping_ts ASC
    )::geometry(LineString, 4326) AS telemetry_route_geom
FROM unnested_pings
GROUP BY trip_id