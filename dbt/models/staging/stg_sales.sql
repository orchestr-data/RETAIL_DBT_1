{{ config(
    schema='STAGING',
    materialized='table',
    tags=['staging']
) }}

SELECT
    sale_id,
    store_id,
    product_id,
    cust_id,
    CAST(sale_date AS DATE) AS sale_date,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(total_sale_amount AS NUMBER(10,2)) AS total_sale_amount,
    CURRENT_TIMESTAMP() AS etl_loaded_at
FROM {{ source('raw', 'sales') }}