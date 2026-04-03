{{ config(
    materialized='table',
    schema='PROD_MICROMOBILITY_ANALYTICS',
    alias='F_VEHICLE_STATUS_VIANOVA'
) }}

WITH voi_data AS (
    SELECT 
        device_id, 
        'Voi' AS provider_name, 
        'bicycle' AS vehicle_type, 
        vehicle_state, 
        event_type, 
        lat, 
        lon, 
        trip_id, 
        reported_at
    FROM {{ ref('stg_voi_vehicles_status') }}
),

combined_data AS (
    SELECT * FROM voi_data
)

SELECT
    -- Final Vianova Mapping with Double Quotes for Uppercase Headers
    MD5(CONCAT(device_id, provider_name, reported_at::text)) AS "UID",
    device_id AS "VEHICLE_ID",
    vehicle_type AS "VEHICLE_TYPE",
    provider_name AS "PROVIDER_NAME",
    vehicle_state AS "VEHICLE_STATE",
    event_type AS "EVENT_TYPE",
    lat AS "LAT",
    lon AS "LON",
    trip_id AS "TRIP_ID",
    reported_at AS "VALID_FROM_TS",
    
    -- FIXED LOGIC: 
    -- Use 'device_id' and 'provider_name' (from combined_data)
    -- rather than their aliases in the PARTITION BY.
    LEAD(reported_at) OVER (
        PARTITION BY device_id, provider_name 
        ORDER BY reported_at ASC
    ) AS "VALID_TO_TS"
FROM combined_data