{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT content, file_ts, load_ts
    FROM {{ source('raw_voi', 'VOI_VEHICLES_STATUS') }}
),

unnested AS (
    SELECT
        file_ts,
        jsonb_array_elements(content->'vehicles_status') AS item
    FROM raw_data
)

SELECT
    item->>'device_id' AS device_id,
    item->'last_event'->>'vehicle_state' AS vehicle_state,
    -- Extract the first event type from the array
    item->'last_event'->'event_types'->>0 AS event_type, 
    -- Extract the first trip id if it exists
    item->'last_event'->'trip_ids'->>0 AS trip_id,
    (item->'last_telemetry'->'location'->>'lat')::float AS lat,
    (item->'last_telemetry'->'location'->>'lng')::float AS lon,
    -- Unix ms to Timestamp
    TO_TIMESTAMP((item->'last_telemetry'->>'timestamp')::bigint / 1000.0) AS reported_at
FROM unnested