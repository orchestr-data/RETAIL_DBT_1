-- models/metrics/product_metrics.sql
{{ config(
    materialized='table',
    schema='metrics'
) }}

WITH product_performance AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.product_category,
        p.price_tier,
        p.product_age_group,
        
        -- Sales Performance
        COUNT(s.sale_id) as total_transactions,
        SUM(s.quantity) as total_units_sold,
        SUM(s.total_sale_amount) as total_revenue,
        ROUND(AVG(s.total_sale_amount),2) as avg_transaction_value,
        
        -- Rankings
        RANK() OVER (ORDER BY SUM(s.total_sale_amount) DESC) as revenue_rank,
        RANK() OVER (ORDER BY SUM(s.quantity) DESC) as volume_rank
        
    FROM {{ ref('dim_products') }} p
    LEFT JOIN {{ ref('fact_sales') }} s ON p.product_id = s.product_id
    GROUP BY 1,2,3,4,5
),

category_performance AS (
    SELECT 
        product_category,
        SUM(total_revenue) as category_revenue,
        SUM(total_units_sold) as category_units_sold,
        COUNT(DISTINCT product_id) as products_in_category,
        AVG(total_revenue) as avg_revenue_per_product
    FROM product_performance
    GROUP BY 1
),

total_revenue AS (
    SELECT SUM(total_revenue) as company_total_revenue 
    FROM product_performance
)

SELECT 
    pp.*,
    cp.category_revenue,
    cp.products_in_category,
    -- Product Revenue Contribution
    ROUND((pp.total_revenue / tr.company_total_revenue * 100),2) as revenue_contribution_pct
    
FROM product_performance pp
LEFT JOIN category_performance cp ON pp.product_category = cp.product_category
CROSS JOIN total_revenue tr