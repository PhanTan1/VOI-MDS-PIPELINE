{{
    config(
        materialized='incremental',
        unique_key='TRIP_ID',
        alias='F_TRIP',
        schema='PROD_MICROMOBILITY_ANALYTICS',
        on_schema_change='append_new_columns'
    )
}}

SELECT
    trip_id AS "TRIP_ID",
    vehicle_short_id AS "VEHICLE_ID",
    vehicle_type AS "VEHICLE_TYPE",
    'Voi' AS "PROVIDER_NAME",
    start_ts AS "START_TS",
    end_ts AS "END_TS",
    trip_duration AS "TRIP_DURATION",

    -- DISTANCE FIX: Calculate precise distance in meters using Geography
    -- This will give you the high-precision decimal (e.g., 1050.247...)
    ST_Distance(
        ST_SetSRID(ST_MakePoint(start_lon, start_lat), 4326)::geography,
        ST_SetSRID(ST_MakePoint(end_lon, end_lat), 4326)::geography
    )::float AS "TRIP_DISTANCE",

    start_lat AS "START_LAT",
    start_lon AS "START_LON",
    end_lat AS "END_LAT",
    end_lon AS "END_LON",

    -- ROUTE FIX: This MUST be cast to jsonb to avoid the LINESTRING text format
    ST_AsGeoJSON(route_geom)::jsonb AS "ROUTE"

FROM {{ ref('stg_voi_trips') }}

{% if is_incremental() %}
  WHERE start_ts >= (SELECT MAX("START_TS") - INTERVAL '3 days' FROM {{ this }})
{% endif %}