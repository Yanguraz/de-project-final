DROP TABLE IF EXISTS STV2024031225__STAGING.transactions;
DROP TABLE IF EXISTS STV2024031225__STAGING.currencies;
DROP TABLE IF EXISTS STV2024031225__DWH.global_metrics;

CREATE TABLE STV2024031225__STAGING.transactions (
    operation_id VARCHAR(1000) NOT NULL,
    account_number_from INTEGER NOT NULL,
    account_number_to INTEGER NOT NULL,
    currency_code INTEGER NOT NULL,
    country VARCHAR(100) NOT NULL,
    status VARCHAR(100) NOT NULL,
    transaction_type VARCHAR(100) NOT NULL,
    amount INTEGER NOT NULL,
    transaction_dt TIMESTAMP(3) NOT NULL,
    UNIQUE(operation_id, status) ENABLED
)
ORDER BY transaction_dt, operation_id, status
SEGMENTED BY hash(operation_id, status) ALL NODES
PARTITION BY TRUNC(transaction_dt, 'MM')::DATE;

CREATE TABLE STV2024031225__STAGING.currencies (
    currency_code INTEGER NOT NULL,
    currency_code_with INTEGER NOT NULL,
    date_update DATE NOT NULL,
    currency_with_div DECIMAL(3, 2) NOT NULL,
    UNIQUE(currency_code, currency_code_with, date_update) ENABLED
)
ORDER BY currency_code, currency_code_with, date_update
SEGMENTED BY hash(currency_code, currency_code_with, date_update) ALL NODES;

CREATE TABLE STV2024031225__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from INTEGER NOT NULL,
    amount_total DECIMAL(16, 2) NOT NULL,
    cnt_transactions INTEGER NOT NULL,
    avg_transactions_per_account DECIMAL(12, 2) NOT NULL,
    cnt_accounts_make_transactions INTEGER NOT NULL
)
ORDER BY date_update, currency_from
SEGMENTED BY hash(date_update, currency_from) ALL NODES;
