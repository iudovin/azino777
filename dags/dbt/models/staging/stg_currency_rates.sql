{{ config(materialized='view', schema='dds') }}

SELECT 
    date::DATE AS rate_date,
    TRIM(currency)::VARCHAR(10) AS currency,
    rate_to_usd::DECIMAL(12, 6) AS rate_to_usd
FROM {{ source('raw', 'currency_rates') }}