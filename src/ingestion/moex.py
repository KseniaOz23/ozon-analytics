import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import urllib3

from src.config import load_config
from src.db import get_db_engine
from src.ingestion.logger import log_etl_start, log_etl_end

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

cfg = load_config("config/project.yaml")
retry_cfg = cfg.get("retry", {"max_attempts": 5, "wait_seconds": 5})

@retry(
    stop=stop_after_attempt(retry_cfg["max_attempts"]),
    wait=wait_fixed(retry_cfg["wait_seconds"]),
    retry=retry_if_exception_type((requests.RequestException, ValueError)),
    reraise=True
)
def fetch_moex_data_for_day(ticker: str, target_date: str) -> pd.DataFrame:
    """Загружает данные за ОДИН конкретный день."""
    url = (
        f"https://iss.moex.com/iss/engines/stock/markets/shares/"
        f"boards/TQBR/securities/{ticker}/candles.json"
    )
    # Запрашиваем ровно один день
    params = {"from": target_date, "till": target_date, "interval": 24, "iss.meta": "off"}
    
    print(f"Запрос к MOEX за {target_date}...")
    resp = requests.get(url, params=params, timeout=30, verify=False)
    resp.raise_for_status()
    
    payload = resp.json().get("candles", {})
    if not payload.get("data"):
        print(f"  -> Нет данных за {target_date} (возможно, выходной).")
        return pd.DataFrame()
        
    df = pd.DataFrame(payload["data"], columns=payload["columns"])
    print(f"  -> Получено {len(df)} строк.")
    return df

def get_last_loaded_date(ticker: str, default_start: str) -> str:
    """Определяет, с какой даты продолжать загрузку."""
    engine = get_db_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT MAX(trade_date) FROM bronze.raw_moex WHERE ticker = :ticker"),
            {"ticker": ticker}
        )
        max_date = result.scalar()
        
    if max_date is None:
        return default_start
    
    next_date = max_date + timedelta(days=1)
    return next_date.strftime("%Y-%m-%d")

def load_moex_to_bronze(start_date: str = None, end_date: str = None):
    ticker = cfg["moex"]["ticker"]
    
    # 1. Определяем даты
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
        
    if start_date is None:
        # Если дата не передана, смотрим в БД или берем дефолт из конфига
        start_date = get_last_loaded_date(ticker, cfg["moex"]["default_start_date"])
    
    # Если start_date больше end_date, ничего не делаем
    if start_date > end_date:
        print(f"Данные уже актуальны (последняя дата в БД: {start_date}, сегодня: {end_date}).")
        return

    log_id = log_etl_start("MOEX_API")
    total_rows = 0
    
    try:
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # 2. Идем по дням
        while current_date <= end_date_dt:
            date_str = current_date.strftime("%Y-%m-%d")
            
            try:
                df = fetch_moex_data_for_day(ticker, date_str)
                
                if not df.empty:
                    df["trade_date"] = pd.to_datetime(df["begin"]).dt.date
                    df["ticker"] = ticker
                    
                    records = df[["trade_date", "ticker", "open", "close", "high", "low", "volume", "value"]].to_dict("records")
                    
                    engine = get_db_engine()
                    with engine.begin() as conn:
                        query = text("""
                            INSERT INTO bronze.raw_moex (trade_date, ticker, open, close, high, low, volume, value)
                            VALUES (:trade_date, :ticker, :open, :close, :high, :low, :volume, :value)
                            ON CONFLICT (trade_date, ticker) DO NOTHING
                        """)
                        conn.execute(query, records)
                    total_rows += len(df)
            except Exception as e:
                print(f"Ошибка за {date_str}: {e}. Пропускаем день.")
            
            current_date += timedelta(days=1)
            
        print(f"Успешно загружено {total_rows} строк.")
        log_etl_end(log_id, "SUCCESS", rows_loaded=total_rows)
        
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        log_etl_end(log_id, "FAILED", rows_loaded=total_rows, error_message=str(e))
        raise e

if __name__ == "__main__":
    # ЗАПУСК 1: Загружаем неделю (с 1 по 7 октября 2026)
    print("--- ЗАГРУЗКА НЕДЕЛИ ---")
    load_moex_to_bronze(start_date="2026-10-01", end_date="2026-10-07")
    
    # ЗАПУСК 2: Загружаем сегодня (если не передать даты, возьмет сегодняшнюю)
    print("\n--- ЗАГРУЗКА СЕГОДНЯШНЕГО ДНЯ ---")
    load_moex_to_bronze()