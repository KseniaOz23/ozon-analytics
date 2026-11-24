import pandas as pd
from sqlalchemy import text
from src.db import get_db_engine
from src.ingestion.logger import log_etl_start, log_etl_end

def build_gold_mart():
    """Сборка витрины gold.mart_daily_metrics."""
    log_id = log_etl_start("GOLD_MART")
    engine = get_db_engine()
    
    try:
        # 1. Читаем Silver-данные
        with engine.connect() as conn:
            df_moex = pd.read_sql("SELECT * FROM silver.clean_moex", conn)
            df_macro = pd.read_sql("SELECT * FROM silver.clean_macro", conn)
            
        if df_moex.empty:
            print("Нет данных в Silver для сборки витрины.")
            log_etl_end(log_id, "SUCCESS", 0)
            return

        # 2. Джойним MOEX и макро-данные по дате
        df_moex["trade_date"] = pd.to_datetime(df_moex["trade_date"]).dt.date
        df_macro["rate_date"] = pd.to_datetime(df_macro["rate_date"]).dt.date
        
        # Left join: оставляем все торговые дни, даже если нет макро-данных
        df_gold = pd.merge(
            df_moex, 
            df_macro, 
            left_on="trade_date", 
            right_on="rate_date", 
            how="left"
        )
        
        # Убираем дублирующую колонку rate_date
        df_gold = df_gold.drop(columns=["rate_date"])
        
        # Заполняем пропуски в макро-данных (если для торгового дня не было курса)
        df_gold["key_rate"] = df_gold["key_rate"].ffill()
        df_gold["usd_rate"] = df_gold["usd_rate"].ffill()
        df_gold["eur_rate"] = df_gold["eur_rate"].ffill()
        
        # Оставляем нужные колонки
        df_gold = df_gold[["trade_date", "ticker", "close", "volume", "daily_return", "key_rate", "usd_rate"]]
        df_gold = df_gold.rename(columns={"close": "close_price"})
        
        # 3. Записываем в Gold (идемпотентно)
        records = df_gold.to_dict("records")
        with engine.begin() as conn:
            query = text("""
                INSERT INTO gold.mart_daily_metrics (trade_date, ticker, close_price, volume, daily_return, key_rate, usd_rate)
                VALUES (:trade_date, :ticker, :close_price, :volume, :daily_return, :key_rate, :usd_rate)
                ON CONFLICT (trade_date) DO UPDATE SET
                    ticker = EXCLUDED.ticker,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    daily_return = EXCLUDED.daily_return,
                    key_rate = EXCLUDED.key_rate,
                    usd_rate = EXCLUDED.usd_rate
            """)
            conn.execute(query, records)
            
        print(f"Gold MART: собрано {len(df_gold)} строк.")
        log_etl_end(log_id, "SUCCESS", len(df_gold))
        
    except Exception as e:
        log_etl_end(log_id, "FAILED", 0, str(e))
        raise e

if __name__ == "__main__":
    build_gold_mart()