-- Создание слоев (схем)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS service;

-- СЛОЙ BRONZE (Сырые данные)
CREATE TABLE bronze.raw_moex (
    trade_date DATE,
    ticker VARCHAR(10),
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    value FLOAT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, ticker)
);

CREATE TABLE bronze.raw_cbr_key_rate (
    rate_date DATE PRIMARY KEY,
    key_rate FLOAT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bronze.raw_cbr_currency (
    rate_date DATE,
    char_code VARCHAR(3),
    nominal INT,
    value FLOAT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (rate_date, char_code)
);

-- СЛОЙ SILVER (Очищенные данные)
CREATE TABLE silver.clean_moex (
    trade_date DATE,
    ticker VARCHAR(10),
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume BIGINT,
    daily_return FLOAT, -- расчетный показатель
    PRIMARY KEY (trade_date, ticker)
);

CREATE TABLE silver.clean_macro (
    rate_date DATE PRIMARY KEY,
    key_rate FLOAT,
    usd_rate FLOAT,
    eur_rate FLOAT
);

-- СЛОЙ GOLD (Витрины для аналитики и ML)
CREATE TABLE gold.mart_daily_metrics (
    trade_date DATE PRIMARY KEY,
    ticker VARCHAR(10),
    close_price FLOAT,
    volume BIGINT,
    daily_return FLOAT,
    key_rate FLOAT,
    usd_rate FLOAT
);

CREATE TABLE gold.fact_forecast (
    forecast_date DATE,      -- на какой день прогноз
    target_date DATE,        -- дата, на которую делается прогноз
    ticker VARCHAR(10),
    model_name VARCHAR(50),
    predicted_close FLOAT,
    predicted_volume BIGINT,
    PRIMARY KEY (forecast_date, target_date, ticker, model_name)
);

-- СЛУЖЕБНЫЕ ТАБЛИЦЫ (Метаданные, DQ, Lineage)
CREATE TABLE service.etl_load_log (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(50),
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20), -- 'RUNNING', 'SUCCESS', 'FAILED'
    rows_loaded INT,
    error_message TEXT
);

CREATE TABLE service.dq_results (
    id SERIAL PRIMARY KEY,
    check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(100),
    check_type VARCHAR(50), -- 'completeness', 'uniqueness', etc.
    status VARCHAR(20),     -- 'PASSED', 'FAILED'
    details TEXT
);

CREATE TABLE service.data_lineage (
    id SERIAL PRIMARY KEY,
    source_object VARCHAR(100),
    target_object VARCHAR(100),
    process_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SCD2: История изменения ключевой ставки
CREATE TABLE silver.dim_key_rate_history (
    id SERIAL PRIMARY KEY,
    key_rate FLOAT,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE DATABASE airflow_meta;