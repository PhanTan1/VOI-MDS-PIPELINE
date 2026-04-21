{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        jsonb_array_elements(
            CASE 
                WHEN content ? 'data' AND (content->'data') ? 'telemetry' THEN content->'data'->'telemetry'
                WHEN content ? 'telemetry' THEN content->'telemetry'
                ELSE '[]'::jsonb
            END
        ) AS item
    FROM {{ source('raw_mds', 'BOLT_TELEMETRY') }}
)

SELECT
    item->>'trip_id' AS trip_id,
    item->>'device_id' AS device_id,
    (item->'location'->>'lat')::DOUBLE PRECISION AS lat,
    (item->'location'->>'lng')::DOUBLE PRECISION AS lon,
    TO_TIMESTAMP((item->>'timestamp')::BIGINT / 1000.0) AS reported_at
FROM base_extraction
WHERE (item->'location'->>'lat') IS NOT NULL