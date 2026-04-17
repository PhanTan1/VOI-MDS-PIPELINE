-- Performance: BRIN for time-series, B-Tree for Joins
{{ 
    config(
        materialized='incremental',
        unique_key='"UID"',
        schema='MICROMOBILITY_ANALYTICS'
    )
}}

WITH bolt_normalized AS (
    SELECT
        t.device_id::TEXT AS device_id,
        COALESCE(r.vehicle_id, t.device_id)::TEXT AS vehicle_id,
        -- Standardizing to 'scooter' and handling nulls
        CASE 
            WHEN r.vehicle_type = 'scooter_standing' THEN 'scooter'
            ELSE COALESCE(r.vehicle_type, 'scooter')
        END::TEXT AS vehicle_type,
        'Bolt'::TEXT AS provider_name,
        t.vehicle_state::TEXT AS vehicle_state,
        t.event_type::TEXT AS event_type,
        t.lat::DOUBLE PRECISION AS lat,
        t.lon::DOUBLE PRECISION AS lon,
        t.trip_id::TEXT AS trip_id,
        t.reported_at::TIMESTAMP AS reported_at
    FROM {{ ref('stg_bolt_events') }} t
    LEFT JOIN {{ ref('stg_bolt_vehicles') }} r ON t.device_id = r.device_id
),

combined_staging AS (
    -- VOI
    SELECT 
        vehicle_short_id::TEXT AS vehicle_id, vehicle_type::TEXT, 'Voi'::TEXT AS provider_name, 
        vehicle_state::TEXT, event_type::TEXT, lat::DOUBLE PRECISION, lon::DOUBLE PRECISION, 
        trip_id::TEXT, reported_at::TIMESTAMP
    FROM {{ ref('stg_voi_vehicles_status') }}
    
    UNION ALL
    
    -- DOTT STATUS
    SELECT 
        COALESCE(v.vehicle_id, s.device_id)::TEXT AS vehicle_id, 
        s.vehicle_type::TEXT, 'Dott'::TEXT AS provider_name, 
        s.vehicle_state::TEXT, s.event_type::TEXT, s.lat, s.lon, s.trip_id, s.reported_at
    FROM {{ ref('stg_dott_vehicles_status') }} s
    LEFT JOIN {{ ref('stg_dott_vehicles') }} v ON s.device_id = v.device_id
    
    UNION ALL
    
    -- DOTT EVENTS (Historical stream)
    SELECT 
        COALESCE(v.vehicle_id, e.device_id)::TEXT AS vehicle_id, 
        COALESCE(v.vehicle_type, 'bicycle')::TEXT, 'Dott' AS provider_name, 
        e.vehicle_state, e.event_type, e.lat, e.lon, e.trip_id, e.reported_at
    FROM {{ ref('stg_dott_events') }} e
    LEFT JOIN {{ ref('stg_dott_vehicles') }} v ON e.device_id = v.device_id

    UNION ALL
    
    -- BOLT
    SELECT 
        vehicle_id, vehicle_type, provider_name, 
        vehicle_state, event_type, lat, lon, trip_id, reported_at
    FROM bolt_normalized

    UNION ALL

    -- POPPY
    SELECT 
        vehicle_id::TEXT, vehicle_type::TEXT, 'Poppy'::TEXT AS provider_name,
        'available' AS vehicle_state, 'telemetry' AS event_type, lat, lon, NULL AS trip_id, reported_at
    FROM {{ ref('stg_poppy_vehicles_status') }}
),

-- DEDUPLICATION: Removes same-millisecond duplicates
deduplicated_staging AS (
    SELECT DISTINCT ON (vehicle_id, provider_name, reported_at)
        *
    FROM combined_staging
    ORDER BY vehicle_id, provider_name, reported_at, event_type
)

SELECT
    LOWER(vehicle_id) || '_' || TO_CHAR(reported_at, 'YYYYMMDD_HH24MISSMS') AS "UID",
    vehicle_id AS "VEHICLE_ID",
    vehicle_type AS "VEHICLE_TYPE",
    provider_name AS "PROVIDER_NAME",
    vehicle_state AS "VEHICLE_STATE",
    event_type AS "EVENT_TYPE",
    lat AS "LAT",
    lon AS "LON",
    trip_id AS "TRIP_ID",
    reported_at AS "VALID_FROM_TS",
    LEAD(reported_at) OVER (PARTITION BY vehicle_id, provider_name ORDER BY reported_at ASC) AS "VALID_TO_TS"
FROM deduplicated_staging
WHERE vehicle_id IS NOT NULL

{% if is_incremental() %}
  AND reported_at >= (SELECT MAX("VALID_FROM_TS") - INTERVAL '3 days' FROM {{ this }})
{% endif %}