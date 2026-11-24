import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from src.config import load_config
from src.db import get_db_engine
from src.ingestion.logger import log_etl_start, log_etl_end

# Настройка ретраев
cfg = load_config("config/project.yaml")
retry_cfg = cfg.get("retry", {"max_attempts": 5, "wait_seconds": 5})

@retry(
    stop=stop_after_attempt(retry_cfg["max_attempts"]),
    wait=wait_fixed(retry_cfg["wait_seconds"]),
    retry=retry_if_exception_type((requests.RequestException, ValueError)),
    reraise=True # Если все попытки провалились, кидаем ошибку дальше (пайплайн упадет)
)
def fetch_moex_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    url = (
        f"https://iss.moex.com/iss/engines/stock/markets/shares/"
        f"boards/TQBR/securities/{ticker}/candles.json"
    )
    params = {"from": start_date, "till": end_date, "interval": 24, "iss.meta": "off"}
    
    print(f"Запрос к MOEX: с {start_date} по {end_date}...")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status() # Вызовет исключение при 4xx/5xx
    
    payload = resp.json().get("candles", {})
    if not payload.get("data"):
        return pd.DataFrame()
        
    return pd.DataFrame(payload["data"], columns=payload["columns"])

def get_last_loaded_date(ticker: str) -> str:
    """Определяет дату, с которой нужно начать загрузку."""
    engine = get_db_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT MAX(trade_date) FROM bronze.raw_moex WHERE ticker = :ticker"),
            {"ticker": ticker}
        )
        max_date = result.scalar()
        
    if max_date is None:
        # Холодный старт
        print(f"БД пуста для {ticker}. Начинаем с {cfg['moex']['default_start_date']}")
        return cfg['moex']['default_start_date']
    
    # Если данные есть, начинаем со следующего дня
    next_date = max_date + timedelta(days=1)
    return next_date.strftime("%Y-%m-%d")

def load_moex_to_bronze():
    ticker = cfg["moex"]["ticker"]
    end_date = cfg["moex"].get("end_date") or datetime.today().strftime("%Y-%m-%d")
    
    log_id = log_etl_start("MOEX_API")
    try:
        start_date = get_last_loaded_date(ticker)
        
        # Если start_date больше end_date, значит данные уже актуальны
        if start_date > end_date:
            print("Данные уже актуальны, загрузка не требуется.")
            log_etl_end(log_id, "SUCCESS", rows_loaded=0)
            return

        df = fetch_moex_data(ticker, start_date, end_date)
        
        if df.empty:
            print("Нет новых данных от MOEX.")
            log_etl_end(log_id, "SUCCESS", rows_loaded=0)
            return

        # Приводим типы и переименовываем колонки под нашу схему bronze
        df["trade_date"] = pd.to_datetime(df["begin"]).dt.date
        df["ticker"] = ticker
        df = df.rename(columns={"value": "value"}) # Оставляем как есть
        
        # Запись в БД (Идемпотентность)
        engine = get_db_engine()
        with engine.begin() as conn:
            # Используем to_sql с методом append, но лучше через execute для ON CONFLICT
            # Для простоты в pandas можно использовать execute_values, но покажем базовый подход
            records = df[["trade_date", "ticker", "open", "close", "high", "low", "volume", "value"]].to_dict("records")
            
            query = text("""
                INSERT INTO bronze.raw_moex (trade_date, ticker, open, close, high, low, volume, value)
                VALUES (:trade_date, :ticker, :open, :close, :high, :low, :volume, :value)
                ON CONFLICT (trade_date, ticker) DO NOTHING
            """)
            conn.execute(query, records)
            
        print(f"Успешно загружено {len(df)} строк.")
        log_etl_end(log_id, "SUCCESS", rows_loaded=len(df))
        
    except Exception as e:
        print(f"Ошибка при загрузке MOEX: {e}")
        log_etl_end(log_id, "FAILED", rows_loaded=0, error_message=str(e))
        raise e # Пробрасываем ошибку, чтобы Airflow пометил таску как Failed

if __name__ == "__main__":
    load_moex_to_bronze()