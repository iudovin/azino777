{{ config(materialized='view', schema='dm') }}

WITH dep AS (
    SELECT 
        DATE_TRUNC('month', d.deposit_date)::DATE AS report_month,
        p.country,
        COALESCE(SUM(d.amount_usd), 0.00) AS total_deposits_usd
    FROM {{ ref('fct_deposits') }} d
    JOIN {{ ref('dim_players') }} p ON d.player_id = p.player_id
    GROUP BY 1, 2
),
wth AS (
    SELECT 
        DATE_TRUNC('month', w.withdrawal_date)::DATE AS report_month,
        p.country,
        COALESCE(SUM(w.amount_usd), 0.00) AS total_withdrawals_usd
    FROM {{ ref('fct_withdrawals') }} w
    JOIN {{ ref('dim_players') }} p ON w.player_id = p.player_id
    GROUP BY 1, 2
),
gms AS (
    SELECT 
        DATE_TRUNC('month', g.game_date)::DATE AS report_month,
        p.country,
        COALESCE(SUM(g.amount_usd), 0.00) AS total_bets_usd
    FROM {{ ref('fct_games') }} g
    JOIN {{ ref('dim_players') }} p ON g.player_id = p.player_id
    GROUP BY 1, 2
),
all_keys AS (
    SELECT report_month, country FROM dep
    UNION
    SELECT report_month, country FROM wth
    UNION
    SELECT report_month, country FROM gms
)
SELECT
    k.report_month,
    COALESCE(k.country, 'Unknown') AS country,
    COALESCE(d.total_deposits_usd, 0.00) AS sum_deposits_usd,
    COALESCE(w.total_withdrawals_usd, 0.00) AS sum_withdrawals_usd,
    COALESCE(g.total_bets_usd, 0.00) AS sum_bets_usd
FROM all_keys k
LEFT JOIN dep d ON k.report_month = d.report_month AND k.country = d.country
LEFT JOIN wth w ON k.report_month = w.report_month AND k.country = w.country
LEFT JOIN gms g ON k.report_month = g.report_month AND k.country = g.country
ORDER BY 1 DESC, 2 ASC