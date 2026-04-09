-- Performance: BRIN for time-series, B-Tree for Joins
{{ 
    config(
        materialized='incremental',
        unique_key='"UID"',
        alias='FCT_VEHICLES_STATUS',
        schema='MICROMOBILITY_ANALYTICS',
        post_hook=[
          "CREATE INDEX IF NOT EXISTS idx_status_brin_ts ON {{ this }} USING BRIN (\"VALID_FROM_TS\")",
          "CREATE INDEX IF NOT EXISTS idx_status_bt_vehicle ON {{ this }} (\"VEHICLE_ID\")"
        ]
    )
}}

WITH combined_staging AS (
    -- VOI
    SELECT 
        vehicle_short_id, vehicle_type, 'Voi' AS provider_name, 
        vehicle_state, event_type, lat, lon, trip_id, reported_at
    FROM {{ ref('stg_voi_vehicles_status') }}

    UNION ALL

    -- DOTT
    SELECT 
        vehicle_short_id, vehicle_type, 'Dott' AS provider_name, 
        vehicle_state, event_type, lat, lon, trip_id, reported_at
    FROM {{ ref('stg_dott_vehicles_status') }}
)

SELECT
    LOWER(vehicle_short_id) || '_' || provider_name || '_' || TO_CHAR(reported_at, 'YYYYMMDDHH24MISSMS') AS "UID",
    vehicle_short_id AS "VEHICLE_ID",
    vehicle_type AS "VEHICLE_TYPE",
    provider_name AS "PROVIDER_NAME",
    vehicle_state AS "VEHICLE_STATE",
    event_type AS "EVENT_TYPE",
    lat AS "LAT",
    lon AS "LON",
    trip_id AS "TRIP_ID",
    reported_at AS "VALID_FROM_TS",
    -- Calculate window per vehicle AND provider
    LEAD(reported_at) OVER (PARTITION BY vehicle_short_id, provider_name ORDER BY reported_at ASC) AS "VALID_TO_TS"
FROM combined_staging
WHERE vehicle_short_id IS NOT NULL

{% if is_incremental() %}
  -- Use the 3-day lookback logic you established
  AND reported_at >= (SELECT MAX("VALID_FROM_TS") - INTERVAL '3 days' FROM {{ this }})
{% endif %}