-- models/metrics/operational_metrics.sql
{{ config(
    materialized='table',
    schema='metrics'
) }}

WITH shipping_metrics AS (
    SELECT 
        DATE_TRUNC('month', ship_date) as metric_month,
        
        -- Shipping Performance
        COUNT(shipment_id) as total_shipments,
        ROUND(AVG(shipping_duration_days),2) as average_shipping_duration,
        ROUND(COUNT(CASE WHEN is_delayed_shipment = TRUE 
              THEN shipment_id END),2) as delayed_shipments,
        ROUND((COUNT(CASE WHEN is_delayed_shipment = TRUE 
              THEN shipment_id END) / COUNT(shipment_id) * 100),2) as delayed_shipments_percentage,
              
        -- On-time Delivery Rate
        ROUND((COUNT(CASE WHEN shipping_speed_category IN ('Express', 'Fast') 
              THEN shipment_id END) / COUNT(shipment_id) * 100),2) as on_time_delivery_rate
              
    FROM {{ ref('fact_shipments') }}
    WHERE ship_date IS NOT NULL
    GROUP BY 1
),

returns_metrics AS (
    SELECT 
        DATE_TRUNC('month', return_date) as metric_month,
        
        -- Returns Analysis
        COUNT(return_id) as total_returns,
        ROUND(AVG(days_to_resolve),2) as average_return_processing_time,
        COUNT(CASE WHEN is_overdue_return = TRUE 
              THEN return_id END) as overdue_returns,
              
        -- Return Categories
        COUNT(CASE WHEN return_category = 'Product Quality' 
              THEN return_id END) as quality_returns,
        COUNT(CASE WHEN return_category = 'Customer Decision' 
              THEN return_id END) as decision_returns,
        COUNT(CASE WHEN return_category = 'Service Issue' 
              THEN return_id END) as service_returns
              
    FROM {{ ref('fact_returns') }}
    GROUP BY 1
),

refunds_metrics AS (
    SELECT 
        DATE_TRUNC('month', refund_timestamp) as metric_month,
        
        -- Refunds Analysis
        COUNT(payment_id) as total_refunds,
        ROUND(SUM(refund_amount),2) as total_refund_amount,
        ROUND(AVG(refund_amount),2) as average_refund_amount,
        COUNT(CASE WHEN refund_greater_than_100 = TRUE 
              THEN payment_id END) as large_refunds
              
    FROM {{ ref('fact_refunds') }}
    WHERE refund_flag = TRUE
    GROUP BY 1
),

-- Calculate rates by joining with sales
sales_volume AS (
    SELECT 
        DATE_TRUNC('month', sale_date) as metric_month,
        COUNT(sale_id) as total_sales,
        SUM(total_sale_amount) as total_sales_amount
    FROM {{ ref('fact_sales') }}
    GROUP BY 1
)

SELECT 
    COALESCE(sm.metric_month, rm.metric_month, ref.metric_month, sv.metric_month) as metric_month,
    
    -- Shipping Metrics
    sm.total_shipments,
    sm.average_shipping_duration,
    sm.delayed_shipments_percentage,
    sm.on_time_delivery_rate,
    
    -- Returns Metrics  
    rm.total_returns,
    rm.average_return_processing_time,
    rm.quality_returns,
    rm.decision_returns,
    rm.service_returns,
    
    -- Refunds Metrics
    ref.total_refunds,
    ref.total_refund_amount,
    ref.average_refund_amount,
    
    -- Operational Rates
    ROUND((rm.total_returns / NULLIF(sv.total_sales, 0) * 100),2) as return_rate,
    ROUND((ref.total_refunds / NULLIF(sv.total_sales, 0) * 100),2) as refund_rate,
    ROUND((ref.total_refund_amount / NULLIF(sv.total_sales_amount, 0) * 100),2) as cost_of_returns_pct
    
FROM shipping_metrics sm
FULL OUTER JOIN returns_metrics rm ON sm.metric_month = rm.metric_month  
FULL OUTER JOIN refunds_metrics ref ON sm.metric_month = ref.metric_month
FULL OUTER JOIN sales_volume sv ON sm.metric_month = sv.metric_month