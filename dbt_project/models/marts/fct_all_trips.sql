{{ 
    config(
        materialized='incremental',
        unique_key='"TRIP_ID"',
        schema='MICROMOBILITY_ANALYTICS'
    )
}}

WITH bolt_trips AS (
    SELECT * FROM {{ ref('stg_bolt_trips') }}
),

bolt_registry AS (
    SELECT * FROM {{ ref('stg_bolt_vehicles') }}
),

bolt_normalized AS (
    SELECT
        t.trip_id::TEXT AS trip_id,
        t.vehicle_id::TEXT AS vehicle_id,
        COALESCE(r.vehicle_type, 'scooter')::TEXT AS vehicle_type,
        'Bolt'::TEXT AS provider_name,
        t.started_at::TIMESTAMP AS start_ts,
        t.ended_at::TIMESTAMP AS end_ts,
        t.duration::INTEGER AS trip_duration,
        t.distance::DOUBLE PRECISION AS trip_distance_meters,
        t.start_lat::DOUBLE PRECISION AS start_lat,
        t.start_lon::DOUBLE PRECISION AS start_lon,
        t.end_lat::DOUBLE PRECISION AS end_lat,
        t.end_lon::DOUBLE PRECISION AS end_lon,
        -- BOLT FIX: PostGIS rejects 'FeatureCollection'. 
        -- Keep the rich JSON for the frontend ROUTE, but build a simple line for the database GEOM.
        t.route_geom::TEXT AS route_json, 
        ST_MakeLine(
            ST_SetSRID(ST_MakePoint(t.start_lon, t.start_lat), 4326),
            ST_SetSRID(ST_MakePoint(t.end_lon, t.end_lat), 4326)
        )::GEOMETRY AS route_geom 
    FROM bolt_trips t
    LEFT JOIN bolt_registry r 
        ON t.vehicle_id = r.device_id
),

combined_trips AS (
    -- VOI
    SELECT 
        trip_id::TEXT, 
        vehicle_short_id::TEXT AS vehicle_id, 
        vehicle_type::TEXT, 
        'Voi'::TEXT AS provider_name, 
        start_ts::TIMESTAMP, end_ts::TIMESTAMP, trip_duration::INTEGER, trip_distance_meters::DOUBLE PRECISION, 
        start_lat::DOUBLE PRECISION, start_lon::DOUBLE PRECISION, end_lat::DOUBLE PRECISION, end_lon::DOUBLE PRECISION, 
        ST_AsGeoJSON(route_geom)::TEXT AS route_json, 
        route_geom::GEOMETRY AS route_geom
    FROM {{ ref('stg_voi_trips') }}

    UNION ALL

    -- DOTT
    SELECT 
        trip_id::TEXT, 
        vehicle_short_id::TEXT AS vehicle_id, 
        vehicle_type::TEXT, 
        'Dott'::TEXT AS provider_name, 
        start_ts::TIMESTAMP, end_ts::TIMESTAMP, trip_duration::INTEGER, trip_distance_meters::DOUBLE PRECISION, 
        start_lat::DOUBLE PRECISION, start_lon::DOUBLE PRECISION, end_lat::DOUBLE PRECISION, end_lon::DOUBLE PRECISION, 
        ST_AsGeoJSON(route_geom)::TEXT AS route_json,
        route_geom::GEOMETRY AS route_geom
    FROM {{ ref('stg_dott_trips') }}

    UNION ALL

    -- BOLT
    SELECT 
        trip_id, vehicle_id, vehicle_type, provider_name, 
        start_ts, end_ts, trip_duration, trip_distance_meters, 
        start_lat, start_lon, end_lat, end_lon, 
        route_json, 
        route_geom
    FROM bolt_normalized
)

-- 2. Final Selection and Column Casing
SELECT
    trip_id AS "TRIP_ID",
    vehicle_id AS "VEHICLE_ID",
    vehicle_type AS "VEHICLE_TYPE",
    provider_name AS "PROVIDER_NAME",
    start_ts AS "START_TS",
    end_ts AS "END_TS",
    trip_duration AS "TRIP_DURATION",
    trip_distance_meters AS "TRIP_DISTANCE",
    start_lat AS "START_LAT",
    start_lon AS "START_LON",
    end_lat AS "END_LAT",
    end_lon AS "END_LON",
    
    -- NULLIF prevents empty strings from breaking the JSONB cast
    COALESCE(NULLIF(route_json, ''), ST_AsGeoJSON(route_geom))::JSONB AS "ROUTE",
    ST_SetSRID(route_geom::geometry, 4326) AS "GEOM" 
FROM combined_trips

{% if is_incremental() %}
  WHERE end_ts >= (SELECT MAX("END_TS") - INTERVAL '3 days' FROM {{ this }})
{% endif %}