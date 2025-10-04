{{ config(
    schema='STAGING',
    materialized='table',
    tags=['staging']
) }}

SELECT
    src:payment_id::STRING AS payment_id,
    src:transaction_id::STRING AS transaction_id,
    src:method::STRING AS refund_method,
    src:status::STRING AS refund_status,
    src:refund_flag::BOOLEAN AS refund_flag,
    src:refund_amount::NUMBER(10,2) AS refund_amount,
    src:timestamp::TIMESTAMP AS refund_timestamp,
    CURRENT_TIMESTAMP() as etl_loaded_at
FROM {{ source('raw', 'refunds') }}
