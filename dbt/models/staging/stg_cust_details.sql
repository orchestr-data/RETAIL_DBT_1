{{ config(
    schema='STAGING',
    materialized='table',
    tags=['staging']
) }}

SELECT
    cust_id,
    initcap(name) as name,
    lower(email) as email,
    phone_number AS phone,
    dob,
    age,
    gender,
    signup_date,
    customer_status AS cust_status,
    ADDRESS_LINE1 as Address,
    initcap(city) as city,
    UPPER(state) as state,
    UPPER(country) as country,
    postal_code,
    loyalty_tier,
    preferred_payment_method as pref_pay_method,
    marketing_opt_in as mark_opt_in,
    acquisition_channel as channel,
    last_login_date as last_login,
    CURRENT_TIMESTAMP() as etl_loaded_at
FROM {{ source('raw', 'customers') }}