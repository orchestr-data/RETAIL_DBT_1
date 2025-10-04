{{ config(
    schema='STAGING',
    materialized='table',
    tags=['staging']
) }}

SELECT
    shipment_id,
    order_id,
    product_id,
    from_warehouse,
    to_store,
    CAST(ship_date AS DATE) AS ship_date,
    CAST(arrival_date AS DATE) AS arrival_date,
    CURRENT_TIMESTAMP() as etl_loaded_at
FROM {{ source('raw', 'shipments') }}