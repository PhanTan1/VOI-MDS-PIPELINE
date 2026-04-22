{{ 
    config(
        materialized='incremental',
        unique_key='"UID"',
        schema='MICROMOBILITY_ANALYTICS'
    )
}}

WITH combined_staging AS (
    -- 1. VOI (Fixed: Fallback changed to bicycle)
    SELECT 
        COALESCE(v.vehicle_id, s.device_id)::TEXT AS vehicle_id, 
        COALESCE(v.vehicle_type, 'bicycle')::TEXT AS vehicle_type, 
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
        SELECT device_id, vehicle_state, event_type, trip_id, lat, lon, reported_at, vehicle_type FROM {{ ref('stg_dott_events') }}
        UNION ALL
        SELECT device_id, vehicle_state, event_type, NULL as trip_id, lat, lon, reported_at, vehicle_type FROM {{ ref('stg_dott_vehicles_status') }}
    ) d
    LEFT JOIN {{ ref('stg_dott_vehicles') }} v ON d.device_id = v.device_id

    UNION ALL

    -- 3. BOLT (Standardizing inner column to device_id)
    SELECT 
        COALESCE(v.vehicle_id, b.device_id)::TEXT AS vehicle_id, 
        CASE 
            WHEN COALESCE(v.vehicle_type, b.vehicle_type, 'scooter') = 'scooter_standing' THEN 'scooter'
            ELSE COALESCE(v.vehicle_type, b.vehicle_type, 'scooter')
        END::TEXT AS vehicle_type, 
        'Bolt'::TEXT AS provider_name, b.vehicle_state, b.event_type, b.lat, b.lon, b.trip_id, b.reported_at
    FROM (
        SELECT device_id, vehicle_state, event_type, trip_id, lat, lon, reported_at, NULL as vehicle_type FROM {{ ref('stg_bolt_events') }}
        UNION ALL
        SELECT device_id, vehicle_state, event_type, trip_id, lat, lon, reported_at, vehicle_type FROM {{ ref('stg_bolt_status_changes') }}
    ) b
    LEFT JOIN {{ ref('stg_bolt_vehicles') }} v ON b.device_id = v.device_id

    UNION ALL

    -- 4. POPPY (The Final Fix: Type comes from staging, No outer join needed)
    SELECT 
        device_id::TEXT AS vehicle_id, 
        vehicle_type::TEXT, 
        'Poppy'::TEXT AS provider_name, 
        vehicle_state, 
        event_type, 
        lat, 
        lon, 
        trip_id, 
        reported_at
    FROM (
        -- GBFS Status snapshots
        SELECT device_id, vehicle_type, vehicle_state, event_type, trip_id, lat, lon, reported_at FROM {{ ref('stg_poppy_vehicles_status') }}
        UNION ALL
        -- Trip Starts (Unpivoted)
        SELECT vehicle_short_id as device_id, vehicle_type, 'reserved' as vehicle_state, 'trip_start' as event_type, trip_id, start_lat as lat, start_lon as lon, start_ts as reported_at FROM {{ ref('stg_poppy_trips') }}
        UNION ALL
        -- Trip Ends (Unpivoted)
        SELECT vehicle_short_id as device_id, vehicle_type, 'available' as vehicle_state, 'trip_end' as event_type, trip_id, end_lat as lat, end_lon as lon, end_ts as reported_at FROM {{ ref('stg_poppy_trips') }}
    ) p
),

deduplicated_staging AS (
    SELECT DISTINCT ON (vehicle_id, provider_name, reported_at)
        *
    FROM combined_staging
    ORDER BY 
        vehicle_id, 
        provider_name, 
        reported_at ASC, 
        CASE 
            WHEN event_type IN ('trip_start', 'trip_end', 'user_pick_up', 'user_drop_off') THEN 1
            WHEN event_type IN ('maintenance_drop_off', 'maintenance_pick_up') THEN 2
            ELSE 3
        END ASC
),

change_tracking AS (
    SELECT *,
        LAG(vehicle_state) OVER (PARTITION BY vehicle_id, provider_name ORDER BY reported_at ASC) as prev_state,
        LAG(lat) OVER (PARTITION BY vehicle_id, provider_name ORDER BY reported_at ASC) as prev_lat,
        LAG(lon) OVER (PARTITION BY vehicle_id, provider_name ORDER BY reported_at ASC) as prev_lon
    FROM deduplicated_staging
),

filtered_staging AS (
    SELECT * FROM change_tracking
    WHERE prev_state IS NULL 
       OR vehicle_state != prev_state 
       OR ABS(lat - prev_lat) > 0.0005 
       OR ABS(lon - prev_lon) > 0.0005
       OR event_type NOT IN ('telemetry', 'ping', 'location_update', 'unspecified') 
)

SELECT
    -- UID: Back to your original vehicle_id format
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