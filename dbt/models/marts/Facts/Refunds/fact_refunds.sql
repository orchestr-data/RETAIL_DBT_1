{{ config(
    schema = "marts",
    materialized = "incremental",
    unique_key = "payment_id",
    tags=['marts']
)}}

SELECT 
   payment_id,
   transaction_id,
   refund_method,
   refund_status,
   refund_flag,
   refund_amount,
   refund_timestamp,

   DATEDIFF('day', refund_timestamp, CURRENT_DATE()) AS refund_age_days,

   CASE WHEN refund_amount >= 100 THEN TRUE ELSE FALSE END AS refund_greater_than_100,

   CASE 
     WHEN refund_method IN ('CREDIT_CARD', 'BANK_TRANSFER') THEN 'Electronic'
     WHEN refund_method IN ('CASH', 'STORE_CREDIT') THEN 'In-Store'  
     WHEN refund_method = 'GIFT_CARD' THEN 'Gift Card'
     ELSE 'Other'
   END AS refund_method_category,

   1 AS refund_count,
   CURRENT_TIMESTAMP() AS etl_loaded_at

from {{ ref('stg_refunds') }}

{% if is_incremental() %}
  where etl_loaded_at > (select max(etl_loaded_at) from {{ this }})
{% endif %}

