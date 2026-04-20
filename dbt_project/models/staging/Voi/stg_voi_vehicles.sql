{{ config(materialized='view') }}

WITH raw_vehicles AS (
    SELECT content FROM {{ source('raw_mds', 'VOI_VEHICLES') }}
),

unnested_vehicles AS (
    SELECT jsonb_array_elements(content->'vehicles') AS item 
    FROM raw_vehicles
)

SELECT DISTINCT
    item->>'device_id' AS device_id,       -- The long UUID
    item->>'vehicle_id' AS vehicle_id,     -- The short name (zje2)
    item->>'vehicle_type' AS vehicle_type  -- bicycle, scooter, etc.
FROM unnested_vehicles