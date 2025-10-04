{{ config(
    materialized='incremental',
    unique_key='sale_id',
    incremental_strategy='merge',
    on_schema_change='sync',
    tags=['marts']
) }}

select
    sale_id,
    store_id,
    product_id,
    cust_id,
    sale_date,
    quantity,
    total_sale_amount,

    case 
        when total_sale_amount < 100 then 'Small Sale'
        when total_sale_amount between 100 and 1000 then 'Medium Sale'
        else 'Large Sale'
    end as sale_amount_tier,

    case 
        when quantity = 1 then 'Single Item'
        when quantity between 2 and 5 then 'Multi Item'
        else 'Bulk Purchase'
    end as purchase_type,

    CURRENT_TIMESTAMP() as etl_loaded_at

from {{ ref('stg_sales') }}

{% if is_incremental() %}
  where etl_loaded_at > (select max(etl_loaded_at) from {{ this }})
{% endif %}