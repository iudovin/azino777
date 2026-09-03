{{
    config(
        materialized='table',
        schema='dds',
        indexes=[
            {'columns': ['game_date']},
            {'columns': ['player_id']},
            {'columns': ['provider_id']},
            {'columns': ['game_id']}
        ]
    )
}}

WITH ranked_rates AS (
    SELECT
        g.id::INT AS game_transaction_id,
        cr.rate_to_usd,
        ROW_NUMBER() OVER (
            PARTITION BY g.id 
            ORDER BY cr.rate_date DESC
        ) AS rn
    FROM {{ source('raw', 'games') }} g
    LEFT JOIN {{ ref('stg_currency_rates') }} cr
        ON TRIM(g.currency) = cr.currency
       AND cr.rate_date <= g.game_date::DATE
)
SELECT
    g.id::INT AS game_transaction_id,
    g.player_id::INT AS player_id,
    g.game_date::DATE AS game_date,
    g.provider_id::INT AS provider_id,
    g.game_id::INT AS game_id,
    g.amount::DECIMAL(15, 2) AS amount,
    TRIM(g.currency)::VARCHAR(10) AS currency,
    (g.amount::DECIMAL(15, 2) * COALESCE(r.rate_to_usd, 1.0))::DECIMAL(15, 2) AS amount_usd
FROM {{ source('raw', 'games') }} g
LEFT JOIN ranked_rates r 
    ON g.id::INT = r.game_transaction_id 
   AND r.rn = 1