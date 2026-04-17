{{ config(materialized='view') }}

WITH unnested_pings AS (
    SELECT 
        item->>'device_id' AS device_id,
        (item->>'timestamp')::BIGINT AS ping_ts,
        (item->'location'->>'lat')::FLOAT AS lat,
        (item->'location'->>'lng')::FLOAT AS lon,
        -- Extract the primary trip_id from the array
        item->'trip_ids'->>0 AS trip_id
    FROM (
        SELECT jsonb_array_elements(content->'telemetry') AS item
        FROM {{ source('raw_mds', 'DOTT_TELEMETRY') }}
    ) raw
    -- FIX: Only include pings that are actually associated with a trip
    -- In JSONB, an empty array [] is NOT null, so we check the length
    WHERE jsonb_array_length(item->'trip_ids') > 0 
)

SELECT
    trip_id,
    ST_MakeLine(
        ST_SetSRID(ST_MakePoint(lon, lat), 4326) 
        ORDER BY ping_ts ASC
    )::geometry(LineString, 4326) AS telemetry_route_geom
FROM unnested_pings
GROUP BY trip_id

LINESTRING (4.380975 50.894442, 4.357792 50.900183)