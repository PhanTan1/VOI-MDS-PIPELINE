{{ config(materialized='table', schema='PROD_MICROMOBILITY_ANALYTICS', alias='FCT_VEHICLES_STATUS') }}

SELECT
    LOWER(vehicle_short_id) || '_' || TO_CHAR(reported_at, 'YYYYMMDD') || '_' || TO_CHAR(reported_at, 'HH24MISSMS') AS "UID",
    vehicle_short_id AS "VEHICLE_ID",
    vehicle_type AS "VEHICLE_TYPE",
    'Voi' AS "PROVIDER_NAME",
    vehicle_state AS "VEHICLE_STATE",
    event_type AS "EVENT_TYPE",
    lat AS "LAT",
    lon AS "LON",
    trip_id AS "TRIP_ID",
    reported_at AS "VALID_FROM_TS",
    LEAD(reported_at) OVER (PARTITION BY vehicle_short_id ORDER BY reported_at ASC) AS "VALID_TO_TS"
FROM {{ ref('stg_voi_vehicles_status') }}
WHERE vehicle_short_id IS NOT NULL