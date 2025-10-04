-- models/marts/dim_store.sql
{{ config (
    schema = "marts",
    materialized = "table",
    tags=['marts']
)}}

SELECT 
    -- Core store details
    store_id,
    location,
    region,
    store_manager,
    open_date,
    store_type,
    
    -- Calculated business attributes
    DATEDIFF('day', open_date, CURRENT_DATE()) AS days_since_opening,
    
    CASE 
        WHEN DATEDIFF('year', open_date, CURRENT_DATE()) < 1 THEN 'New Store'
        WHEN DATEDIFF('year', open_date, CURRENT_DATE()) <= 5 THEN 'Established'
        ELSE 'Mature'
    END AS store_age_category,
    
    CASE 
        WHEN store_type = 'FLAGSHIP' THEN 'Premium'
        WHEN store_type = 'OUTLET' THEN 'Discount'  
        WHEN store_type = 'MALL' THEN 'Retail'
        WHEN store_type = 'STANDALONE' THEN 'Standalone'
        ELSE 'Other'
    END AS store_category,
    
    CASE 
        WHEN DATEDIFF('day', open_date, CURRENT_DATE()) < 365 THEN TRUE
        ELSE FALSE
    END AS is_new_store,
    
    -- Audit fields
    CURRENT_TIMESTAMP() AS etl_loaded_at

FROM {{ ref('stg_stores') }}
