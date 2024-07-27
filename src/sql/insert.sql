INSERT INTO STV2024031225__DWH.global_metrics
WITH t0 AS (
    SELECT * 
    FROM STV2024031225__STAGING.currencies c
    WHERE currency_code_with = 420 -- USD
        AND date_update = '{{ ds }}'::date - 1
),
t1 AS (
    SELECT
        t0.date_update,
        t.currency_code AS currency_from,
        t.account_number_from,
        (t.amount * t0.currency_with_div) AS amount
    FROM STV2024031225__STAGING.transactions t
    JOIN t0 ON t.transaction_dt::date = t0.date_update
        AND t.currency_code = t0.currency_code
    WHERE t.status = 'done'
        AND t.account_number_from > 0
        AND t.transaction_dt::date = '{{ ds }}'::date - 1
    UNION ALL
    SELECT
        transaction_dt::date AS date_update,
        currency_code AS currency_from,
        account_number_from,
        amount
    FROM STV2024031225__STAGING.transactions
    WHERE currency_code = 420 -- USD
        AND status = 'done'
        AND account_number_from > 0
        AND transaction_dt::date = '{{ ds }}'::date - 1
)
SELECT
    date_update,
    currency_from,
    SUM(amount) AS amount_total,
    COUNT(*) AS cnt_transactions,
    ROUND(SUM(amount) / COUNT(DISTINCT account_number_from), 2) AS avg_transactions_per_account,
    COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
FROM t1
GROUP BY date_update, currency_from;
