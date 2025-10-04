-- models/metrics/sales_metrics.sql
{{ config(
    materialized='table',
    schema='metrics'
) }}

WITH daily_sales AS (
    SELECT 
        s.sale_date,
        st.region,
        st.store_type,
        p.product_category,
        
        -- Revenue Metrics
        SUM(s.total_sale_amount) as total_revenue,
        ROUND(AVG(s.total_sale_amount),2) as average_order_value,
        ROUND(SUM(s.total_sale_amount) / COUNT(DISTINCT s.cust_id),2) as revenue_per_customer,
        
        -- Volume Metrics
        COUNT(s.sale_id) as total_transactions,
        SUM(s.quantity) as units_sold,
        ROUND(COUNT(s.sale_id) / COUNT(DISTINCT s.cust_id),2) as transactions_per_customer,
        
        COUNT(DISTINCT s.cust_id) as unique_customers,
        COUNT(DISTINCT s.product_id) as unique_products_sold
        
    FROM {{ ref('fact_sales') }} s
    LEFT JOIN {{ ref('dim_stores') }} st ON s.store_id = st.store_id
    LEFT JOIN {{ ref('dim_products') }} p ON s.product_id = p.product_id
    GROUP BY 1,2,3,4
),

growth_metrics AS (
    SELECT 
        *,
        -- Revenue Growth Rate (MoM)
        (total_revenue - LAG(total_revenue, 30) OVER (ORDER BY sale_date)) / 
        LAG(total_revenue, 30) OVER (ORDER BY sale_date) * 100 as revenue_growth_rate_mom
    FROM daily_sales
)

SELECT * FROM growth_metrics