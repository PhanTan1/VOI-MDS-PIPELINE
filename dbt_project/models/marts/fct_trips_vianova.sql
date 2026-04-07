{{ config(materialized='table', schema='PROD_MICROMOBILITY_ANALYTICS', alias='FCT_TRIPS_VIANOVA') }}

SELECT
    LOWER(vehicle_short_id) || '_' || TO_CHAR(start_ts, 'YYYYMMDD') || '_' || TO_CHAR(start_ts, 'HH24MISSMS') AS "UID",
    trip_id AS "TRIP_ID",
    vehicle_short_id AS "VEHICLE_ID",
    vehicle_type AS "VEHICLE_TYPE",
    'Voi' AS "PROVIDER_NAME",
    start_ts AS "START_TS",
    end_ts AS "END_TS",
    trip_duration AS "TRIP_DURATION",
    trip_distance AS "TRIP_DISTANCE",
    start_lat AS "START_LAT",
    start_lon AS "START_LON",
    end_lat AS "END_LAT",
    end_lon AS "END_LON",
    ST_AsGeoJSON(route_geom)::jsonb AS "ROUTE"
FROM {{ ref('stg_voi_trips') }}
WHERE vehicle_short_id IS NOT NULL