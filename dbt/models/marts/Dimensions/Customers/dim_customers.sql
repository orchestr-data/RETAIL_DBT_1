{{ config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='sync_all_columns',
    schema='marts',
    tags=['marts']
) }}

SELECT
    -- Primary key
    cust_id as customer_id,
    
    -- Personal information
    name,
    email,
    phone,
    dob,
    age,
    gender,
    signup_date,
    cust_status,
    
    -- Address information  
    address,
    city,
    state,
    country,
    postal_code,
    
    -- Marketing & preferences
    loyalty_tier,
    pref_pay_method,
    mark_opt_in,
    channel,
    last_login,
    
    -- Calculated fields
    datediff('day', signup_date, current_date()) AS customer_tenure_days,
    datediff('day', last_login, current_date()) AS days_since_last_login,
    
    CASE 
        WHEN datediff('day', last_login, current_date()) <= 30 THEN 'Active'
        WHEN datediff('day', last_login, current_date()) <= 90 THEN 'At Risk'
        ELSE 'Inactive'
    END AS customer_activity_status,
    
    -- Audit fields
    CURRENT_TIMESTAMP() etl_loaded_at

FROM {{ ref('stg_cust_details') }}

{% if is_incremental() %}
    WHERE etl_loaded_at >= (SELECT max(etl_loaded_at) FROM {{ this }})
{% endif %}