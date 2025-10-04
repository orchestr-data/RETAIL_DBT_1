{{ config(
    schema='STAGING',
    materialized='table',
    tags=['staging']
) }}

SELECT
    src:return_id::STRING AS return_id,
    src:transaction_id::STRING AS transaction_id,
    trim(src:reason_code::STRING) as reason_code,
    trim(src:resolution_status::STRING) as resolution_status,
    src:return_date::DATETIME AS return_date,
    src:resolved_date::DATETIME AS resolved_date,
    CURRENT_TIMESTAMP() as etl_loaded_at
FROM {{ source('raw', 'returns') }}
