SELECT * FROM {{ ref('stg_voi_trips') }}
UNION ALL
SELECT * FROM {{ ref('stg_dott_trips') }}
UNION ALL
SELECT * FROM {{ ref('stg_bolt_trips') }}