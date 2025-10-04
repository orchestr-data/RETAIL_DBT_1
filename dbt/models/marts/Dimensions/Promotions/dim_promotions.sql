{{ config (
    schema = "marts",
    materialized = "table",
    tags=['marts']
)}}

SELECT 
   promotion_id,
   promotion_type,
   product_scope,
   discount_type,
   start_date,
   end_date,

   CASE
      WHEN current_date < start_date THEN 'YET TO ACTIVATE'
      WHEN current_date BETWEEN start_date AND end_date THEN 'ACTIVE'
      WHEN current_date > end_date THEN 'EXPIRED'
   END AS  promo_status,

   datediff('day', start_date, end_date) AS promo_duration_days,

   CASE
      WHEN datediff('day', start_date, end_date) <= 7 THEN 'Short Term'
      WHEN datediff('day', start_date, end_date) <= 45 THEN 'Medium Term'
      ELSE 'Long Term'
   END AS promo_duration_category,

   CURRENT_TIMESTAMP() etl_loaded_at

FROM {{ ref('stg_promotions') }}
