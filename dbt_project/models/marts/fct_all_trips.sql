{{ 
    config(
        materialized='incremental',
        unique_key='"TRIP_ID"',
        schema='MICROMOBILITY_ANALYTICS'
    )
}}

WITH all_providers AS (
    -- 1. VOI (13 Columns)
    SELECT 
        trip_id::TEXT, 
        vehicle_short_id::TEXT AS vehicle_id, 
        vehicle_type::TEXT, 
        provider_name::TEXT,
        start_ts::TIMESTAMP, 
        end_ts::TIMESTAMP, 
        trip_duration::NUMERIC, 
        trip_distance_meters::FLOAT,
        start_lat::FLOAT, 
        start_lon::FLOAT, 
        end_lat::FLOAT, 
        end_lon::FLOAT, 
        route_geom::GEOMETRY
    FROM {{ ref('stg_voi_trips') }}

    UNION ALL

    -- 2. DOTT (13 Columns) - Stitched & Cleaned
    SELECT 
        t.trip_id::TEXT, 
        t.vehicle_short_id::TEXT AS vehicle_id,
        t.vehicle_type::TEXT, 
        t.provider_name::TEXT,
        t.start_ts::TIMESTAMP, 
        t.end_ts::TIMESTAMP, 
        t.trip_duration::NUMERIC, 
        t.trip_distance_meters::FLOAT,
        t.start_lat::FLOAT, 
        t.start_lon::FLOAT, 
        t.end_lat::FLOAT, 
        t.end_lon::FLOAT,
        -- Stitch App Start/End to IoT Route and remove stationary pings
        ST_RemoveRepeatedPoints(
            CASE 
                WHEN tel.telemetry_route_geom IS NOT NULL THEN
                    ST_MakeLine(
                        ST_SetSRID(ST_MakePoint(t.start_lon, t.start_lat), 4326), 
                        ST_MakeLine(tel.telemetry_route_geom, ST_SetSRID(ST_MakePoint(t.end_lon, t.end_lat), 4326))
                    )
                ELSE t.route_geom
            END
        )::GEOMETRY AS route_geom
    FROM {{ ref('stg_dott_trips') }} t
    LEFT JOIN {{ ref('stg_dott_telemetry') }} tel ON t.trip_id = tel.trip_id

    UNION ALL

    -- 3. BOLT (13 Columns) - Standardized vehicle_type
    SELECT 
        trip_id::TEXT, 
        vehicle_short_id::TEXT AS vehicle_id, 
        -- Mapping Bolt scooter standing to standard scooter
        CASE 
            WHEN vehicle_type = 'scooter_standing' THEN 'scooter'
            ELSE vehicle_type 
        END::TEXT AS vehicle_type, 
        provider_name::TEXT,
        started_at::TIMESTAMP AS start_ts, 
        ended_at::TIMESTAMP AS end_ts, 
        duration::NUMERIC AS trip_duration, 
        distance::FLOAT AS trip_distance_meters,
        start_lat::FLOAT, 
        start_lon::FLOAT, 
        end_lat::FLOAT, 
        end_lon::FLOAT,
        ST_MakeLine(ST_SetSRID(ST_MakePoint(start_lon, start_lat), 4326), ST_SetSRID(ST_MakePoint(end_lon, end_lat), 4326))::GEOMETRY AS route_geom
    FROM {{ ref('stg_bolt_trips') }}

    UNION ALL

    -- 4. POPPY (13 Columns)
    SELECT 
        trip_id::TEXT, 
        vehicle_short_id::TEXT AS vehicle_id, 
        vehicle_type::TEXT, 
        provider_name::TEXT,
        start_ts::TIMESTAMP, 
        end_ts::TIMESTAMP, 
        trip_duration::NUMERIC, 
        trip_distance_meters::FLOAT,
        start_lat::FLOAT, 
        start_lon::FLOAT, 
        end_lat::FLOAT, 
        end_lon::FLOAT,
        -- Uses native route_geom from the corrected stg_poppy_trips
        route_geom::GEOMETRY AS route_geom
    FROM {{ ref('stg_poppy_trips') }}
)

SELECT
    trip_id AS "TRIP_ID",
    vehicle_id AS "VEHICLE_ID",
    vehicle_type AS "VEHICLE_TYPE",
    provider_name AS "PROVIDER_NAME",
    start_ts AS "START_TS",
    end_ts AS "END_TS",
    trip_duration AS "TRIP_DURATION",
    trip_distance_meters AS "TRIP_DISTANCE",
    -- Dynamically extract start/end from geometry for 100% consistency
    ST_Y(ST_StartPoint(route_geom))::FLOAT AS "START_LAT",
    ST_X(ST_StartPoint(route_geom))::FLOAT AS "START_LON",
    ST_Y(ST_EndPoint(route_geom))::FLOAT AS "END_LAT",
    ST_X(ST_EndPoint(route_geom))::FLOAT AS "END_LON",
    ST_AsGeoJSON(route_geom)::jsonb AS "ROUTE", 
    route_geom AS "GEOM"
FROM all_providers