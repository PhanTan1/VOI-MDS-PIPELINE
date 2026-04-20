{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT jsonb_array_elements(content->'data'->'vehicle_types') AS item 
    FROM {{ source('raw_mds', 'POPPY_VEHICLE_TYPES') }}
)

SELECT DISTINCT
    item->>'vehicle_type_id' AS vehicle_type_id,
    item->>'form_factor' AS vehicle_type
FROM base_extraction