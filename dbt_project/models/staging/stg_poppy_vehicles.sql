{{ config(materialized='view') }}

SELECT DISTINCT
    v->>'vehicle_id' AS vehicle_id, -- Poppy usually uses the same for both
    v->>'vehicle_id' AS device_id,
    COALESCE(v->>'vehicle_type', 'car') AS vehicle_type
FROM {{ source('raw_mds', 'POPPY_VEHICLE_TYPES') }},
jsonb_array_elements(content->'data'->'vehicle_types') v