-- models/metrics/customer_metrics.sql
{{ config(
    materialized='table',
    schema='metrics'
) }}

WITH customer_behavior AS (
    SELECT 
        DATE_TRUNC('month', CURRENT_DATE()) as metric_month,
        
        -- Customer Counts
        COUNT(DISTINCT customer_id) as total_customers,
        COUNT(DISTINCT CASE WHEN customer_activity_status = 'Active' 
              THEN customer_id END) as active_customers_count,
        
        -- Customer Tenure
        AVG(customer_tenure_days) as average_customer_tenure,
        AVG(days_since_last_login) as avg_days_since_last_login,
        
        -- Loyalty Distribution
        COUNT(DISTINCT CASE WHEN loyalty_tier = 'Gold' 
              THEN customer_id END) as gold_customers,
        COUNT(DISTINCT CASE WHEN loyalty_tier = 'Silver' 
              THEN customer_id END) as silver_customers,
        COUNT(DISTINCT CASE WHEN loyalty_tier = 'Bronze' 
              THEN customer_id END) as bronze_customers,
              
        -- Geographic Distribution
        COUNT(DISTINCT CASE WHEN country = 'USA' 
              THEN customer_id END) as usa_customers,
        COUNT(DISTINCT state) as unique_states,
        COUNT(DISTINCT city) as unique_cities
        
    FROM {{ ref('dim_customers') }}
    GROUP BY 1
),

customer_acquisition AS (
    SELECT 
        DATE_TRUNC('month', signup_date) as signup_month,
        channel as acquisition_channel,
        COUNT(DISTINCT customer_id) as new_customers_acquired
    FROM {{ ref('dim_customers') }}
    WHERE signup_date >= DATE_TRUNC('month', CURRENT_DATE()) - INTERVAL '12 months'
    GROUP BY 1,2
)

SELECT 
    cb.*,
    -- Customer Acquisition Rate (new customers this month vs last month)
    LAG(cb.total_customers, 1) OVER (ORDER BY cb.metric_month) as prev_month_customers,
    (cb.total_customers - LAG(cb.total_customers, 1) OVER (ORDER BY cb.metric_month)) /
    LAG(cb.total_customers, 1) OVER (ORDER BY cb.metric_month) * 100 as customer_acquisition_rate
    
FROM customer_behavior cb