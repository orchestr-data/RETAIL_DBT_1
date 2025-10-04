{{ config(
    schema='STAGING',
    materialized='table',
    tags=['staging']
) }}

SELECT
    store_id,
    trim(location) as location,
    trim(region) as region,
    trim(store_manager) as store_manager,
    CAST(open_date AS DATE) AS open_date,
    trim(store_type) AS store_type,
    CURRENT_TIMESTAMP() AS etl_loaded_at
FROM {{ source('raw', 'stores') }}