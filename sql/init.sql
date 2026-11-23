CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS bronze.moex_candles_raw (
    ticker TEXT,
    trade_date DATE,
    open NUMERIC,
    close NUMERIC,
    high NUMERIC,
    low NUMERIC,
    volume BIGINT,
    value NUMERIC,
    PRIMARY KEY (ticker, trade_date)
);