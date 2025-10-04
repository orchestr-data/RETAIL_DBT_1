{{ config(
    schema='STAGING',
    materialized='table',
    tags=['staging']
) }}

SELECT 
   product_id,
   trim(name) AS name,
   trim(category) AS category,
   CAST(price AS NUMBER(10,2)) AS price,      
   CAST(created_at AS DATE) AS created_at,
   CURRENT_TIMESTAMP() as etl_loaded_at
FROM {{ source('raw', 'products') }}