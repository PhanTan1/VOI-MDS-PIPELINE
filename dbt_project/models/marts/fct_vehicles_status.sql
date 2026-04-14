-- Performance: BRIN for time-series, B-Tree for Joins
{{ 
    config(
        materialized='incremental',
        unique_key='"UID"',
        alias='FCT_VEHICLES_STATUS',
        schema='MICROMOBILITY_ANALYTICS'
    )
}}

WITH bolt_telemetry AS (
    SELECT * FROM {{ ref('stg_bolt_events') }}
),

bolt_registry AS (
    SELECT * FROM {{ ref('stg_bolt_vehicles') }}
),

bolt_normalized AS (
    SELECT
        t.device_id::TEXT AS vehicle_id,
        COALESCE(r.vehicle_type, 'scooter')::TEXT AS vehicle_type,
        'Bolt'::TEXT AS provider_name,
        t.vehicle_state::TEXT AS vehicle_state,
        t.event_type::TEXT AS event_type,
        t.lat::DOUBLE PRECISION AS lat,
        t.lon::DOUBLE PRECISION AS lon,
        t.trip_id::TEXT AS trip_id, -- Grab from staging instead of hardcoded NULL
        t.reported_at::TIMESTAMP AS reported_at
    FROM bolt_telemetry t
    LEFT JOIN bolt_registry r ON t.device_id = r.device_id
),

combined_staging AS (
    SELECT 
        vehicle_short_id::TEXT AS vehicle_id, vehicle_type::TEXT, 'Voi'::TEXT AS provider_name, 
        vehicle_state::TEXT, event_type::TEXT, lat::DOUBLE PRECISION, lon::DOUBLE PRECISION, 
        trip_id::TEXT, reported_at::TIMESTAMP
    FROM {{ ref('stg_voi_vehicles_status') }}
    UNION ALL
    SELECT 
        vehicle_id::TEXT, vehicle_type::TEXT, 'Dott'::TEXT AS provider_name, 
        vehicle_state::TEXT, event_type::TEXT, lat::DOUBLE PRECISION, lon::DOUBLE PRECISION, 
        trip_id::TEXT, reported_at::TIMESTAMP
    FROM {{ ref('stg_dott_vehicles_status') }}
    UNION ALL
    SELECT 
        vehicle_id, vehicle_type, provider_name, 
        vehicle_state, event_type, lat, lon, trip_id, reported_at
    FROM bolt_normalized
),

-- DEDUPLICATION: Removes same-millisecond duplicates that cause UID collisions
deduplicated_staging AS (
    SELECT DISTINCT ON (vehicle_id, provider_name, reported_at)
        *
    FROM combined_staging
    ORDER BY vehicle_id, provider_name, reported_at, event_type -- Deterministic pick
)

SELECT
    LOWER(vehicle_id) || '_' || provider_name || '_' || TO_CHAR(reported_at, 'YYYYMMDDHH24MISSMS') AS "UID",
    vehicle_id AS "VEHICLE_ID",
    vehicle_type AS "VEHICLE_TYPE",
    provider_name AS "PROVIDER_NAME",
    vehicle_state AS "VEHICLE_STATE",
    event_type AS "EVENT_TYPE",
    lat AS "LAT",
    lon AS "LON",
    trip_id AS "TRIP_ID",
    reported_at AS "VALID_FROM_TS",
    -- LEAD function now works correctly on deduplicated data
    LEAD(reported_at) OVER (PARTITION BY vehicle_id, provider_name ORDER BY reported_at ASC) AS "VALID_TO_TS"
FROM deduplicated_staging
WHERE vehicle_id IS NOT NULL

{% if is_incremental() %}
  AND reported_at >= (SELECT MAX("VALID_FROM_TS") - INTERVAL '3 days' FROM {{ this }})
{% endif %}