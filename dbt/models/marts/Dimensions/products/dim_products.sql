{{ config(
    schema='marts',
    materialized='incremental',
    unique_key='product_id',
    tags=['marts']
) }}

SELECT
    product_id,
    name AS product_name,
    category AS product_category,
    price,
    created_at,
    
    CASE 
        WHEN price < 350 THEN 'Budget'
        WHEN price BETWEEN 350 AND 700 THEN 'Mid-Range'
        ELSE 'Premium'
    END AS price_tier,
    
    CASE 
        WHEN datediff('day', created_at, current_date()) <= 90 THEN 'New'
        WHEN datediff('day', created_at, current_date()) <= 365 THEN 'Established'  
        ELSE 'Legacy'
    END AS product_age_group,
    
    CURRENT_TIMESTAMP() etl_loaded_at
    
FROM {{ ref('stg_products') }}

{% if is_incremental() %}
    WHERE etl_loaded_at >= (SELECT MAX(etl_loaded_at) FROM {{ this }})
{% endif %}