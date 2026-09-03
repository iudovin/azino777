

WITH ranked_rates AS (
    SELECT
        w.id::INT AS withdrawal_id,
        cr.rate_to_usd,
        ROW_NUMBER() OVER (
            PARTITION BY w.id 
            ORDER BY cr.rate_date DESC
        ) AS rn
    FROM "datastack"."raw"."withdrawals" w
    LEFT JOIN "datastack"."dds"."stg_currency_rates" cr
        ON TRIM(w.currency) = cr.currency
       AND cr.rate_date <= w.withdrawal_date::DATE
)
SELECT
    w.id::INT AS withdrawal_id,
    w.player_id::INT AS player_id,
    w.withdrawal_date::DATE AS withdrawal_date,
    w.provider_id::INT AS provider_id,
    w.amount::DECIMAL(15, 2) AS amount,
    TRIM(w.currency)::VARCHAR(10) AS currency,
    (w.amount::DECIMAL(15, 2) * COALESCE(r.rate_to_usd, 1.0))::DECIMAL(15, 2) AS amount_usd
FROM "datastack"."raw"."withdrawals" w
LEFT JOIN ranked_rates r 
    ON w.id::INT = r.withdrawal_id 
   AND r.rn = 1