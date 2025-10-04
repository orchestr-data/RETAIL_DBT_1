-- models/metrics/store_metrics.sql
{{ config(
    materialized='table',
    schema='metrics'
) }}

WITH store_performance AS (
    SELECT 
        st.store_id,
        st.location,
        st.region,
        st.store_type,
        st.store_age_category,
        
        -- Revenue Performance
        SUM(s.total_sale_amount) as total_revenue,
        COUNT(s.sale_id) as total_transactions,
        ROUND(AVG(s.total_sale_amount),2) as avg_transaction_value,
        SUM(s.quantity) as total_units_sold,
        
        -- Customer Metrics
        COUNT(DISTINCT s.cust_id) as unique_customers,
        
        -- Product Variety
        COUNT(DISTINCT s.product_id) as unique_products_sold
        
    FROM {{ ref('dim_stores') }} st
    LEFT JOIN {{ ref('fact_sales') }} s ON st.store_id = s.store_id
    GROUP BY 1,2,3,4,5
),

regional_performance AS (
    SELECT 
        region,
        SUM(total_revenue) as regional_revenue,
        ROUND(AVG(total_revenue),2) as avg_store_revenue_in_region,
        COUNT(store_id) as stores_in_region
    FROM store_performance
    GROUP BY 1
)

SELECT 
    sp.*,
    rp.regional_revenue,
    rp.avg_store_revenue_in_region,
    
    -- Store Rankings
    RANK() OVER (ORDER BY sp.total_revenue DESC) as revenue_rank_overall,
    RANK() OVER (PARTITION BY sp.region ORDER BY sp.total_revenue DESC) as revenue_rank_in_region,
    
    -- Performance vs Regional Average
    ROUND((sp.total_revenue / rp.avg_store_revenue_in_region * 100),2) as performance_vs_regional_avg
    
FROM store_performance sp
LEFT JOIN regional_performance rp ON sp.region = rp.region