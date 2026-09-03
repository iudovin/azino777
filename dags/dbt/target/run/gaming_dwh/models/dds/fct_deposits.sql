
  
    

  create  table "datastack"."dds"."fct_deposits__dbt_tmp"
  
  
    as
  
  (
    

WITH ranked_rates AS (
    SELECT
        d.id::INT AS deposit_id,
        cr.rate_to_usd,
        ROW_NUMBER() OVER (
            PARTITION BY d.id 
            ORDER BY cr.rate_date DESC
        ) AS rn
    FROM "datastack"."raw"."deposits" d
    LEFT JOIN "datastack"."dds"."stg_currency_rates" cr
        ON TRIM(d.currency) = cr.currency
       AND cr.rate_date <= d.deposit_date::DATE
)
SELECT
    d.id::INT AS deposit_id,
    d.player_id::INT AS player_id,
    d.deposit_date::DATE AS deposit_date,
    d.provider_id::INT AS provider_id,
    d.amount::DECIMAL(15, 2) AS amount,
    TRIM(d.currency)::VARCHAR(10) AS currency,
    (d.amount::DECIMAL(15, 2) * COALESCE(r.rate_to_usd, 1.0))::DECIMAL(15, 2) AS amount_usd
FROM "datastack"."raw"."deposits" d
LEFT JOIN ranked_rates r 
    ON d.id::INT = r.deposit_id 
   AND r.rn = 1
  );
  