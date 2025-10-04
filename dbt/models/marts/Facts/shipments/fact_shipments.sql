{{ config(
    schema = "marts",
    materialized = "incremental",
    unique_key = "shipment_id",
    incremental_strategy='merge',
    on_schema_change='sync',
    tags=['marts']
)}}

SELECT 
  shipment_id,
  order_id,
  product_id,
  from_warehouse,
  to_store,
  ship_date,
  arrival_date,
  DATEDIFF('day', ship_date, arrival_date) AS shipping_duration_days,
  
  CASE 
    WHEN DATEDIFF('day', ship_date, arrival_date) <= 1 THEN 'Express'
    WHEN DATEDIFF('day', ship_date, arrival_date) <= 3 THEN 'Fast'
    WHEN DATEDIFF('day', ship_date, arrival_date) <= 7 THEN 'Standard'
    ELSE 'Slow'
  END AS shipping_speed_category,

  CASE WHEN DATEDIFF('day', ship_date, arrival_date) > 7 
    THEN TRUE ELSE FALSE END AS is_delayed_shipment,

  CASE 
    WHEN arrival_date IS NULL AND ship_date <= CURRENT_DATE() THEN 'In Transit'
    WHEN arrival_date IS NULL AND ship_date > CURRENT_DATE() THEN 'Scheduled'
    WHEN arrival_date IS NOT NULL THEN 'Delivered'
    ELSE 'Unknown'
  END AS shipment_status,

  CURRENT_TIMESTAMP() AS etl_loaded_at

FROM {{ ref("stg_shipments") }}

{% if is_incremental() %}
  where etl_loaded_at > (select max(etl_loaded_at) from {{ this }})
{% endif %}