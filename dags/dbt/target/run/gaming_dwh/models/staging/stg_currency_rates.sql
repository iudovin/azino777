
  create view "datastack"."dds"."stg_currency_rates__dbt_tmp"
    
    
  as (
    

SELECT 
    date::DATE AS rate_date,
    TRIM(currency)::VARCHAR(10) AS currency,
    rate_to_usd::DECIMAL(12, 6) AS rate_to_usd
FROM "datastack"."raw"."currency_rates"
  );