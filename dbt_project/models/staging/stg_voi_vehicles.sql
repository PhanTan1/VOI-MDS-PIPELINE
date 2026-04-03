{{ config(materialized='table', schema='PROD_MICROMOBILITY_STAGING') }}

-- Use the source definition we discussed earlier
WITH raw_data AS (
    SELECT 
        content,
        file_ts,
        load_ts
    FROM {{ source('raw_voi', 'VOI_VEHICLES_STATUS') }}
)

SELECT
    -- PostgreSQL JSONB extraction: ->> returns text, -> returns JSONB object
    content->>'vehicle_id' AS vehicle_id,
    content->>'current_range_meters' AS range_meters,
    (content->>'battery_level')::integer AS battery_pct,
    content->>'state' AS vehicle_status,
    -- Extracting nested coordinates
    (content->'location'->>'lat')::numeric AS lat,
    (content->'location'->>'lng')::numeric AS lng,
    file_ts AS data_timestamp,
    load_ts AS ingested_at
FROM raw_data