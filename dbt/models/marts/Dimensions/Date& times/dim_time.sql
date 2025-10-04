{{ config(
    materialized='table',
    schema='marts'
) }}

WITH date_range AS (
    SELECT 
        '2024-01-01'::date + (row_number() over (order by 1) - 1) AS date_key
    FROM TABLE(GENERATOR(ROWCOUNT => 730))  -- 2 years only
),

dim_time AS (
    SELECT
        date_key,
        EXTRACT(YEAR FROM date_key) AS year,
        EXTRACT(MONTH FROM date_key) AS month, 
        EXTRACT(QUARTER FROM date_key) AS quarter,
        DAYNAME(date_key) AS day_of_week,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM date_key) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS weekend_flag
    FROM date_range
    WHERE date_key <= '2025-12-31'  -- Only 2 years
)

SELECT * FROM dim_time