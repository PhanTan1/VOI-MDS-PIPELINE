{{ config(materialized='view') }}

WITH base_extraction AS (
    SELECT jsonb_array_elements(content->'data'->'vehicles') AS v 
    FROM {{ source('raw_mds', 'DOTT_VEHICLES') }}
)

SELECT DISTINCT
    v->>'device_id' AS device_id,       
    v->>'vehicle_id' AS vehicle_id,     -- This will successfully pull "5GTK84"
    COALESCE(v->>'vehicle_type', 'bicycle') AS vehicle_type
FROM base_extraction