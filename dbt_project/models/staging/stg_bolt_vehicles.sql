{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT 
        jsonb_array_elements(
            CASE 
                WHEN content ? 'data' AND (content->'data') ? 'vehicles' THEN content->'data'->'vehicles'
                WHEN content ? 'vehicles' THEN content->'vehicles'
                ELSE '[]'::jsonb
            END
        ) AS item 
    FROM {{ source('raw_mds', 'BOLT_VEHICLES') }}
)

SELECT DISTINCT
    item->>'device_id' AS device_id,
    item->>'vehicle_id' AS vehicle_id,
    COALESCE(item->>'vehicle_type', 'scooter') AS vehicle_type
FROM base_extraction
WHERE item IS NOT NULL