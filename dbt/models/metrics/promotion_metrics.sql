-- models/metrics/promotion_metrics.sql  
{{ config(
    materialized='table',
    schema='metrics'
) }}

WITH promotion_performance AS (
    SELECT 
        pr.promotion_id,
        pr.promotion_type,
        pr.product_scope,
        pr.discount_type,
        pr.promo_status,
        pr.promo_duration_category,
        pr.promo_duration_days,
        pr.start_date,
        pr.end_date,
        
        -- Sales during promotion (you'd need to join with sales on date ranges)
        COUNT(s.sale_id) as promotional_transactions,
        SUM(s.total_sale_amount) as promotional_revenue,
        SUM(s.quantity) as promotional_units_sold,
        COUNT(DISTINCT s.cust_id) as customers_acquired_via_promo
        
    FROM {{ ref('dim_promotions') }} pr
    LEFT JOIN {{ ref('fact_sales') }} s ON (
        s.sale_date BETWEEN pr.start_date AND pr.end_date 
        -- Add additional join logic based on how you track promotional sales
    )
    WHERE pr.promo_status IN ('ACTIVE', 'EXPIRED')
    GROUP BY 1,2,3,4,5,6,7,8,9
),

baseline_performance AS (
    -- Calculate average performance for non-promotional periods
    SELECT 
        ROUND(AVG(daily_revenue),2) as avg_daily_revenue_baseline,
        AVG(daily_transactions) as avg_daily_transactions_baseline
    FROM (
        SELECT 
            sale_date,
            SUM(total_sale_amount) as daily_revenue,
            COUNT(sale_id) as daily_transactions
        FROM {{ ref('fact_sales') }}
        GROUP BY 1
    )
)

SELECT 
    pp.promotion_id,
    pp.promotion_type,
    pp.product_scope,
    pp.discount_type,
    pp.promo_status,
    pp.promo_duration_category,
    pp.promo_duration_days,
    pp.start_date,
    pp.end_date,
    pp.promotional_transactions,
    pp.promotional_revenue,
    pp.promotional_units_sold,
    pp.customers_acquired_via_promo,
    
    -- Baseline comparison
    bp.avg_daily_revenue_baseline,
    
    -- Promotion Effectiveness
    ROUND(
    CASE 
        WHEN pp.promo_duration_days > 0 
        THEN pp.promotional_revenue / pp.promo_duration_days 
        ELSE NULL 
    END, 2)  as avg_daily_revenue_during_promo,
    
    ROUND(
    CASE 
        WHEN pp.promo_duration_days > 0 AND bp.avg_daily_revenue_baseline > 0
        THEN ((pp.promotional_revenue / pp.promo_duration_days) / bp.avg_daily_revenue_baseline - 1) * 100
        ELSE NULL 
    END, 2) as revenue_lift_percentage,
    
    -- Additional metrics
    ROUND(
    CASE 
        WHEN pp.promotional_transactions > 0 
        THEN pp.promotional_revenue / pp.promotional_transactions 
        ELSE NULL 
    END, 2) as avg_promotional_order_value,
    
    CURRENT_TIMESTAMP() as etl_loaded_at
    
FROM promotion_performance pp
CROSS JOIN baseline_performance bp