import pandas as pd
from sqlalchemy import text
from src.db import get_db_engine
from src.ingestion.logger import log_etl_start, log_etl_end

def transform_moex_to_silver():
    """Очистка MOEX и расчет доходности."""
    log_id = log_etl_start("SILVER_MOEX")
    engine = get_db_engine()
    
    try:
        # 1. Читаем данные из Bronze
        with engine.connect() as conn:
            df_moex = pd.read_sql("SELECT * FROM bronze.raw_moex ORDER BY trade_date", conn)
        
        if df_moex.empty:
            print("Нет данных в Bronze для MOEX.")
            log_etl_end(log_id, "SUCCESS", 0)
            return

        # 2. Очистка и обогащение
        # Приводим типы (на всякий случай)
        df_moex["trade_date"] = pd.to_datetime(df_moex["trade_date"]).dt.date
        numeric_cols = ["open", "close", "high", "low", "volume"]
        df_moex[numeric_cols] = df_moex[numeric_cols].apply(pd.to_numeric, errors='coerce')
        
        # Удаляем строки, где нет цены закрытия (критическая ошибка)
        df_moex = df_moex.dropna(subset=["close"])
        
        # Считаем дневную доходность (в процентах)
        df_moex["daily_return"] = df_moex["close"].pct_change() * 100
        
        # Оставляем нужные колонки для Silver
        df_silver = df_moex[["trade_date", "ticker", "open", "close", "high", "low", "volume", "daily_return"]]
        
        # 3. Записываем в Silver (идемпотентно)
        records = df_silver.to_dict("records")
        with engine.begin() as conn:
            query = text("""
                INSERT INTO silver.clean_moex (trade_date, ticker, open, close, high, low, volume, daily_return)
                VALUES (:trade_date, :ticker, :open, :close, :high, :low, :volume, :daily_return)
                ON CONFLICT (trade_date, ticker) DO UPDATE SET
                    open = EXCLUDED.open,
                    close = EXCLUDED.close,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    volume = EXCLUDED.volume,
                    daily_return = EXCLUDED.daily_return
            """)
            conn.execute(query, records)
            
        print(f"Silver MOEX: обработано {len(df_silver)} строк.")
        log_etl_end(log_id, "SUCCESS", len(df_silver))
        
    except Exception as e:
        log_etl_end(log_id, "FAILED", 0, str(e))
        raise e

def transform_macro_to_silver():
    """Объединение курсов валют и ключевой ставки."""
    log_id = log_etl_start("SILVER_MACRO")
    engine = get_db_engine()
    
    try:
        # 1. Читаем из Bronze
        with engine.connect() as conn:
            df_currency = pd.read_sql("SELECT * FROM bronze.raw_cbr_currency", conn)
            df_key_rate = pd.read_sql("SELECT * FROM bronze.raw_cbr_key_rate", conn)
            
        if df_currency.empty and df_key_rate.empty:
            print("Нет макро-данных в Bronze.")
            log_etl_end(log_id, "SUCCESS", 0)
            return

        # 2. Разворачиваем валюты в колонки (USD, EUR)
        if not df_currency.empty:
            df_currency["rate_date"] = pd.to_datetime(df_currency["rate_date"]).dt.date
            # Pivot: строки -> колонки usd_rate, eur_rate
            df_currency_pivot = df_currency.pivot_table(
                index="rate_date", 
                columns="char_code", 
                values="value"
            ).reset_index()
            df_currency_pivot = df_currency_pivot.rename(columns={"USD": "usd_rate", "EUR": "eur_rate"})
        else:
            df_currency_pivot = pd.DataFrame(columns=["rate_date", "usd_rate", "eur_rate"])

        # 3. Ключевая ставка
        if not df_key_rate.empty:
            df_key_rate["rate_date"] = pd.to_datetime(df_key_rate["rate_date"]).dt.date
            df_key_rate = df_key_rate[["rate_date", "key_rate"]]
        else:
            df_key_rate = pd.DataFrame(columns=["rate_date", "key_rate"])

        # 4. Объединяем по дате (outer join, чтобы не терять данные)
        df_macro = pd.merge(df_key_rate, df_currency_pivot, on="rate_date", how="outer")
        
        # Заполняем пропуски (ключевая ставка не меняется каждый день, курс в выходные может отсутствовать)
        df_macro = df_macro.sort_values("rate_date")
        df_macro["key_rate"] = df_macro["key_rate"].ffill()
        df_macro["usd_rate"] = df_macro["usd_rate"].ffill()
        df_macro["eur_rate"] = df_macro["eur_rate"].ffill()
        
        # Удаляем строки, где вообще нет данных
        df_macro = df_macro.dropna(subset=["rate_date"])
        
        # 5. Записываем в Silver
        records = df_macro.to_dict("records")
        with engine.begin() as conn:
            query = text("""
                INSERT INTO silver.clean_macro (rate_date, key_rate, usd_rate, eur_rate)
                VALUES (:rate_date, :key_rate, :usd_rate, :eur_rate)
                ON CONFLICT (rate_date) DO UPDATE SET
                    key_rate = EXCLUDED.key_rate,
                    usd_rate = EXCLUDED.usd_rate,
                    eur_rate = EXCLUDED.eur_rate
            """)
            conn.execute(query, records)
            
        print(f"Silver MACRO: обработано {len(df_macro)} строк.")
        log_etl_end(log_id, "SUCCESS", len(df_macro))
        
    except Exception as e:
        log_etl_end(log_id, "FAILED", 0, str(e))
        raise e

if __name__ == "__main__":
    print("--- SILVER: MOEX ---")
    transform_moex_to_silver()
    print("\n--- SILVER: MACRO ---")
    transform_macro_to_silver()