{{ 
    config(
        materialized='incremental',
        unique_key='"UID"',
        schema='MICROMOBILITY_ANALYTICS'
    )
}}

WITH combined_staging AS (
    -- 1. VOI (Fixed: Now unions Events + Status)
    SELECT 
        COALESCE(v.vehicle_id, s.device_id)::TEXT AS vehicle_id, 
        COALESCE(v.vehicle_type, 'scooter')::TEXT AS vehicle_type, 
        'Voi'::TEXT AS provider_name, s.vehicle_state, s.event_type, s.lat, s.lon, s.trip_id, s.reported_at
    FROM (
        SELECT device_id, vehicle_state, event_type, trip_id, lat, lon, reported_at FROM {{ ref('stg_voi_events') }}
        UNION ALL
        SELECT device_id, vehicle_state, event_type, trip_id, lat, lon, reported_at FROM {{ ref('stg_voi_vehicles_status') }}
    ) s
    LEFT JOIN {{ ref('stg_voi_vehicles') }} v ON s.device_id = v.device_id

    UNION ALL

    -- 2. DOTT 
    SELECT 
        COALESCE(v.vehicle_id, d.device_id)::TEXT AS vehicle_id, 
        COALESCE(v.vehicle_type, d.vehicle_type, 'bicycle')::TEXT AS vehicle_type, 
        'Dott'::TEXT AS provider_name, d.vehicle_state, d.event_type, d.lat, d.lon, d.trip_id, d.reported_at
    FROM (
        -- THE FIX: Changed 'NULL as vehicle_type' to 'vehicle_type'
        SELECT device_id, vehicle_state, event_type, trip_id, lat, lon, reported_at, vehicle_type FROM {{ ref('stg_dott_events') }}
        UNION ALL
        SELECT device_id, vehicle_state, event_type, NULL as trip_id, lat, lon, reported_at, vehicle_type FROM {{ ref('stg_dott_vehicles_status') }}
    ) d
    LEFT JOIN {{ ref('stg_dott_vehicles') }} v ON d.device_id = v.device_id

    UNION ALL

    -- 3. BOLT
    SELECT 
        COALESCE(v.vehicle_id, b.device_id)::TEXT AS vehicle_id, 
        CASE WHEN COALESCE(v.vehicle_type, b.vehicle_type) = 'scooter_standing' THEN 'scooter' ELSE 'scooter' END::TEXT AS vehicle_type, 
        'Bolt'::TEXT AS provider_name, b.vehicle_state, b.event_type, b.lat, b.lon, b.trip_id, b.reported_at
    FROM (
        SELECT device_id, vehicle_state, event_type, trip_id, lat, lon, reported_at, NULL as vehicle_type FROM {{ ref('stg_bolt_events') }}
        UNION ALL
        SELECT device_id, vehicle_state, event_type, trip_id, lat, lon, reported_at, vehicle_type FROM {{ ref('stg_bolt_status_changes') }}
    ) b
    LEFT JOIN {{ ref('stg_bolt_vehicles') }} v ON b.device_id = v.device_id

    UNION ALL

    -- 4. POPPY
    SELECT 
        vehicle_id::TEXT, vehicle_type::TEXT, 'Poppy'::TEXT AS provider_name,
        'available' AS vehicle_state, 'telemetry' AS event_type, lat, lon, NULL AS trip_id, reported_at
    FROM {{ ref('stg_poppy_vehicles_status') }}
),

deduplicated_staging AS (
    SELECT DISTINCT ON (vehicle_id, provider_name, reported_at)
        *
    FROM combined_staging
    -- TIE-BREAKER: Prioritize 'real' events over generic 'telemetry' or 'unknown' states
    ORDER BY 
        vehicle_id, 
        provider_name, 
        reported_at DESC, 
        CASE 
            WHEN event_type IN ('trip_start', 'trip_end', 'user_pick_up', 'user_drop_off') THEN 1
            WHEN event_type IS NOT NULL THEN 2
            ELSE 3
        END ASC
),

-- STEP 2: Point Change Tracking to the DEDUPLICATED data, not the raw combined data
change_tracking AS (
    SELECT *,
        LAG(vehicle_state) OVER (PARTITION BY vehicle_id, provider_name ORDER BY reported_at ASC) as prev_state,
        LAG(lat) OVER (PARTITION BY vehicle_id, provider_name ORDER BY reported_at ASC) as prev_lat,
        LAG(lon) OVER (PARTITION BY vehicle_id, provider_name ORDER BY reported_at ASC) as prev_lon
    FROM deduplicated_staging -- <--- THIS WAS THE ERROR (It was pointing to combined_staging)
),

filtered_staging AS (
    SELECT * FROM change_tracking
    WHERE prev_state IS NULL -- Keep first row
       OR vehicle_state != prev_state -- Keep state changes
       OR ABS(lat - prev_lat) > 0.0005 -- Keep movement > ~50m
       OR ABS(lon - prev_lon) > 0.0005
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
FROM filtered_staging
WHERE vehicle_id IS NOT NULL
{% if is_incremental() %}
  AND reported_at >= (SELECT MAX("VALID_FROM_TS") - INTERVAL '1 day' FROM {{ this }})
{% endif %}