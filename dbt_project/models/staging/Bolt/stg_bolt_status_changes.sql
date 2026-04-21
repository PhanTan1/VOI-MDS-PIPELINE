{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        jsonb_array_elements(
            CASE 
                WHEN content ? 'data' AND (content->'data') ? 'status_changes' THEN content->'data'->'status_changes'
                ELSE content->'status_changes'
            END
        ) AS item 
    FROM {{ source('raw_mds', 'BOLT_STATUS_CHANGES') }}
)

SELECT
    item->>'device_id' AS device_id,
    item->>'vehicle_id' AS vehicle_id,
    item->>'vehicle_type' AS vehicle_type,
    (item->'event_types')->>0 AS event_type, -- Grabbing primary event from array
    item->>'vehicle_state' AS vehicle_state,
    -- GeoJSON extraction: [lon, lat]
    (item->'event_location'->'geometry'->'coordinates'->>0)::DOUBLE PRECISION AS lon,
    (item->'event_location'->'geometry'->'coordinates'->>1)::DOUBLE PRECISION AS lat,
    item->>'trip_id' AS trip_id,
    TO_TIMESTAMP((item->>'event_time')::BIGINT / 1000.0) AS reported_at
FROM base_extraction