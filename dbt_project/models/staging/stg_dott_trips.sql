{{ config(materialized='view') }}

WITH raw_trips AS (
    SELECT jsonb_array_elements(content->'trips') AS item 
    FROM {{ source('raw_mds', 'DOTT_TRIPS') }}
),

registry AS (
    -- We assume you will build a stg_dott_vehicles registry soon
    -- For now, we fetch from the raw DOTT_VEHICLES table to avoid circular refs
    SELECT 
        v->>'device_id' as device_id, 
        v->>'vehicle_id' as vehicle_id, 
        v->>'vehicle_type' as vehicle_type
    FROM {{ source('raw_mds', 'DOTT_VEHICLES') }},
    jsonb_array_elements(content->'vehicles') as v
)

SELECT DISTINCT
    t.item->>'trip_id' AS trip_id,
    -- Dott usually puts device_id in the trip, we join for the short ID
    COALESCE(r.vehicle_id, t.item->>'device_id') AS vehicle_short_id,
    COALESCE(r.vehicle_type, 'bicycle') AS vehicle_type,
    'Dott' AS provider_name,
    
    -- MDS 2.0 timestamps are usually in milliseconds
    TO_TIMESTAMP((t.item->>'start_time')::bigint / 1000.0) AS start_ts,
    TO_TIMESTAMP((t.item->>'end_time')::bigint / 1000.0) AS end_ts,
    (t.item->>'duration')::numeric AS trip_duration,
    (t.item->>'distance')::float AS trip_distance_meters,
    
    (t.item->'start_location'->>'lat')::float AS start_lat,
    (t.item->'start_location'->>'lng')::float AS start_lon,
    (t.item->'end_location'->>'lat')::float AS end_lat,
    (t.item->'end_location'->>'lng')::float AS end_lon,

    -- Geometry
    COALESCE(
        ST_GeomFromGeoJSON(t.item->'route'),
        ST_MakeLine(
            ST_SetSRID(ST_MakePoint((t.item->'start_location'->>'lng')::float, (t.item->'start_location'->>'lat')::float), 4326),
            ST_SetSRID(ST_MakePoint((t.item->'end_location'->>'lng')::float, (t.item->'end_location'->>'lat')::float), 4326)
        )
    )::geometry(LineString, 4326) AS route_geom

FROM raw_trips t
LEFT JOIN registry r ON (t.item->>'device_id') = r.device_id