{{ config(materialized='view') }}

SELECT
    content->>'device_id' AS device_id,
    content->>'vehicle_id' AS vehicle_id,
    content->>'vehicle_type' AS vehicle_type
FROM {{ source('raw_mds', 'BOLT_VEHICLES') }}