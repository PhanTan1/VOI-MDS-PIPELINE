{{ 
    config(
        materialized='incremental',
        unique_key='"TRIP_ID"',
        schema='MICROMOBILITY_ANALYTICS'
    )
}}

WITH all_providers AS (
    -- 1. VOI (13 Columns) - Patched with historical seed data
    SELECT 
        t.trip_id::TEXT, 
        -- THE FIX: Double quotes force Postgres to respect the exact case from the seed CSV
        COALESCE(t.vehicle_short_id, p."VEHICLE_ID")::TEXT AS vehicle_id, 
        COALESCE(t.vehicle_type, p."VEHICLE_TYPE")::TEXT AS vehicle_type, 
        t.provider_name::TEXT,
        t.start_ts::TIMESTAMP, 
        t.end_ts::TIMESTAMP, 
        t.trip_duration::NUMERIC, 
        t.trip_distance_meters::FLOAT,
        t.start_lat::FLOAT, 
        t.start_lon::FLOAT, 
        t.end_lat::FLOAT, 
        t.end_lon::FLOAT, 
        t.route_geom::GEOMETRY
    FROM {{ ref('stg_voi_trips') }} t
    -- THE FIX: Double quotes around "TRIP_ID"
    LEFT JOIN {{ ref('voi_lost_vehicles') }} p ON t.trip_id = p."TRIP_ID"

    UNION ALL

   -- 2. DOTT (13 Columns) - Stitched & Cleaned
    SELECT 
        t.trip_id::TEXT, 
        -- THE FIX: Check the Dott seed for the UUID replacement, otherwise use the original ID
        COALESCE(t.vehicle_short_id, p_dott."VEHICLE_ID")::TEXT AS vehicle_id,
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
    -- JOIN the Dott seed specifically here
    LEFT JOIN {{ ref('dott_lost_vehicles') }} p_dott ON t.trip_id = p_dott."TRIP_ID"

    UNION ALL

    -- 3. BOLT (13 Columns) - Standardized vehicle_type
    SELECT 
        t.trip_id::TEXT, 
        -- THE FIX: Check the Bolt seed for the replacement, otherwise use original
        COALESCE(t.vehicle_short_id, p_bolt."VEHICLE_ID")::TEXT AS vehicle_id, 
        CASE 
            WHEN t.vehicle_type = 'scooter_standing' THEN 'scooter'
            ELSE t.vehicle_type 
        END::TEXT AS vehicle_type, 
        t.provider_name::TEXT,
        t.started_at::TIMESTAMP AS start_ts, 
        t.ended_at::TIMESTAMP AS end_ts, 
        t.duration::NUMERIC AS trip_duration, 
        t.distance::FLOAT AS trip_distance_meters,
        t.start_lat::FLOAT, 
        t.start_lon::FLOAT, 
        t.end_lat::FLOAT, 
        t.end_lon::FLOAT,
        COALESCE(
            (
                SELECT ST_MakeLine(ST_SetSRID(ST_MakePoint(tel.lon, tel.lat), 4326) ORDER BY tel.reported_at)
                FROM {{ ref('stg_bolt_telemetry') }} tel
                WHERE tel.trip_id = t.trip_id
            ),
            ST_MakeLine(
                ST_SetSRID(ST_MakePoint(t.start_lon, t.start_lat), 4326), 
                ST_SetSRID(ST_MakePoint(t.end_lon, t.end_lat), 4326)
            )
        )::GEOMETRY AS route_geom
    FROM {{ ref('stg_bolt_trips') }} t
    -- JOIN the Bolt seed specifically here
    LEFT JOIN {{ ref('bolt_lost_vehicles') }} p_bolt ON t.trip_id = p_bolt."TRIP_ID"

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
        route_geom::GEOMETRY AS route_geom
    FROM {{ ref('stg_poppy_trips') }}
),

deduplicated_trips AS (
    SELECT DISTINCT ON (trip_id)
        *
    FROM all_providers
    ORDER BY trip_id, start_ts DESC
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
    ST_Y(ST_StartPoint(route_geom))::FLOAT AS "START_LAT",
    ST_X(ST_StartPoint(route_geom))::FLOAT AS "START_LON",
    ST_Y(ST_EndPoint(route_geom))::FLOAT AS "END_LAT",
    ST_X(ST_EndPoint(route_geom))::FLOAT AS "END_LON",
    ST_AsGeoJSON(route_geom)::jsonb AS "ROUTE", 
    route_geom AS "GEOM"
FROM deduplicated_trips