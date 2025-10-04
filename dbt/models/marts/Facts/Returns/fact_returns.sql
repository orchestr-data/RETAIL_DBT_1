{{ config(
    schema = "marts",
    materialized = "incremental",
    unique_key = "return_id",
    tags=['marts']
)}}

SELECT 
   return_id,
   transaction_id,
   reason_code,
   resolution_status,
   return_date,
   resolved_date,

   CASE
      WHEN resolved_date IS NOT NULL
      THEN DATEDIFF('day', return_date, resolved_date)
      ELSE DATEDIFF('day', return_date, CURRENT_DATE())
   END AS days_to_resolve,

    CASE 
        WHEN reason_code IN ('DEFECTIVE', 'DAMAGED', 'NOT_AS_DESC') THEN 'Product Quality'
        WHEN reason_code IN ('WRONG_SIZE', 'CHANGED_MIND') THEN 'Customer Decision'
        WHEN reason_code IN ('LATE_DELIVERY', 'DUPLICATE') THEN 'Service Issue'
        ELSE 'Other'
    END AS return_category,

    CASE 
        WHEN DATEDIFF('day', return_date, COALESCE(resolved_date, CURRENT_DATE())) <= 2 THEN 'Fast'
        WHEN DATEDIFF('day', return_date, COALESCE(resolved_date, CURRENT_DATE())) <= 7 THEN 'Standard'
        ELSE 'Slow'
    END AS processing_speed,

    CASE 
        WHEN resolution_status IN ('pending', 'escalated') 
             AND DATEDIFF('day', return_date, CURRENT_DATE()) > 14 
        THEN TRUE 
        ELSE FALSE 
    END AS is_overdue_return,

    1 AS return_count,

    CURRENT_TIMESTAMP() AS etl_loaded_at

from {{ ref('stg_returns') }}

{% if is_incremental() %}
  where etl_loaded_at > (select max(etl_loaded_at) from {{ this }})
{% endif %}


