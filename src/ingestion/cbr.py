import io
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import urllib3
import cbrapi as cbr

from src.config import load_config
from src.db import get_db_engine
from src.ingestion.logger import log_etl_start, log_etl_end

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

cfg = load_config("config/project.yaml")
retry_cfg = cfg.get("retry", {"max_attempts": 5, "wait_seconds": 5})

@retry(
    stop=stop_after_attempt(retry_cfg["max_attempts"]),
    wait=wait_fixed(retry_cfg["wait_seconds"]),
    retry=retry_if_exception_type((requests.RequestException, ValueError)),
    reraise=True
)
def fetch_cbr_currency_rates_for_day(date_req: str) -> pd.DataFrame:
    """Загружает курсы валют ЦБ РФ на ОДИН день."""
    url = "https://www.cbr.ru/scripts/XML_daily.asp"
    # ЦБ требует формат DD/MM/YYYY
    print(f"Запрос курсов валют ЦБ на {date_req}...")
    resp = requests.get(url, params={"date_req": date_req}, timeout=30, verify=False)
    resp.raise_for_status()
    
    # Если ответ пустой (например, праздник)
    if len(resp.content) < 100:
        print(f"  -> Нет данных на {date_req}.")
        return pd.DataFrame()
        
    return pd.read_xml(
        io.BytesIO(resp.content),
        xpath=".//Valute",
        encoding="windows-1251",
    )

def get_last_cbr_date(table: str, date_col: str, default_start: str) -> str:
    engine = get_db_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT MAX({date_col}) FROM {table}"))
        max_date = result.scalar()
    
    if max_date is None:
        return default_start
    return (max_date + timedelta(days=1)).strftime("%Y-%m-%d")

def load_cbr_currency_to_bronze(start_date: str = None, end_date: str = None):
    """Загрузка курсов валют (USD, EUR) по дням."""
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = get_last_cbr_date("bronze.raw_cbr_currency", "rate_date", cfg["cbr"]["default_start_date"])
        
    if start_date > end_date:
        print(f"Курсы валют актуальны (БД: {start_date}, конец: {end_date}).")
        return

    log_id = log_etl_start("CBR_CURRENCY")
    total_rows = 0
    
    try:
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        while current_date <= end_date_dt:
            # ЦБ ждет формат DD/MM/YYYY
            date_req = current_date.strftime("%d/%m/%Y")
            date_str = current_date.strftime("%Y-%m-%d")
            
            try:
                df = fetch_cbr_currency_rates_for_day(date_req)
                
                if not df.empty:
                    # Нам нужны только USD и EUR (CharCode)
                    df = df[df["CharCode"].isin(["USD", "EUR"])]
                    
                    if not df.empty:
                        df["rate_date"] = current_date.date()
                        # Value в ЦБ приходит строкой с запятой (например, "92,5")
                        df["Value"] = df["Value"].astype(str).str.replace(",", ".").astype(float)
                        df["Nominal"] = df["Nominal"].astype(int)
                        # Приводим к курсу за 1 единицу
                        df["value"] = df["Value"] / df["Nominal"]
                        
                        # Оставляем только нужные колонки под нашу схему
                        df_clean = df[["rate_date", "CharCode", "Nominal", "value"]]
                        df_clean = df_clean.rename(columns={"CharCode": "char_code"})
                        
                        records = df_clean.to_dict("records")
                        
                        engine = get_db_engine()
                        with engine.begin() as conn:
                            query = text("""
                                INSERT INTO bronze.raw_cbr_currency (rate_date, char_code, nominal, value)
                                VALUES (:rate_date, :char_code, :Nominal, :value)
                                ON CONFLICT (rate_date, char_code) DO NOTHING
                            """)
                            conn.execute(query, records)
                        total_rows += len(df_clean)
            except Exception as e:
                print(f"Ошибка за {date_str}: {e}. Пропускаем день.")
            
            current_date += timedelta(days=1)
            
        print(f"Успешно загружено {total_rows} строк курсов валют.")
        log_etl_end(log_id, "SUCCESS", rows_loaded=total_rows)
        
    except Exception as e:
        log_etl_end(log_id, "FAILED", rows_loaded=total_rows, error_message=str(e))
        raise e

def load_cbr_key_rate_to_bronze(start_date: str = None, end_date: str = None):
    """Загрузка ключевой ставки."""
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = get_last_cbr_date("bronze.raw_cbr_key_rate", "rate_date", cfg["cbr"]["default_start_date"])
        
    if start_date > end_date:
        print(f"Ключевая ставка актуальна (БД: {start_date}, конец: {end_date}).")
        return

    log_id = log_etl_start("CBR_KEY_RATE")
    
    try:
        print(f"Запрос ключевой ставки с {start_date} по {end_date}...")
        series = cbr.get_key_rate(start_date, end_date)
        
        if series.empty:
            print("Нет новых данных по ключевой ставке.")
            log_etl_end(log_id, "SUCCESS", 0)
            return
        
        # Преобразуем Series в DataFrame. Индекс называется 'DATE', значения - 'KEY_RATE'
        df = series.to_frame(name="key_rate").reset_index()
        # Переименовываем 'DATE' в 'rate_date'
        df = df.rename(columns={"DATE": "rate_date"})
        
        # Проверка, что колонка появилась
        if "rate_date" not in df.columns:
            print(f"ОШИБКА: Не удалось найти колонку с датой. Колонки: {df.columns.tolist()}")
            log_etl_end(log_id, "FAILED", 0, "Column rate_date not found")
            return
            
        df["rate_date"] = df["rate_date"].dt.to_timestamp().dt.date
        records = df.to_dict("records")
        
        engine = get_db_engine()
        with engine.begin() as conn:
            query = text("""
                INSERT INTO bronze.raw_cbr_key_rate (rate_date, key_rate)
                VALUES (:rate_date, :key_rate)
                ON CONFLICT (rate_date) DO NOTHING
            """)
            conn.execute(query, records)
            
        print(f"Успешно загружено {len(df)} строк ключевой ставки.")
        log_etl_end(log_id, "SUCCESS", len(df))
        
    except Exception as e:
        print(f"Ошибка: {e}")
        log_etl_end(log_id, "FAILED", 0, str(e))
        raise e

if __name__ == "__main__":
    print("--- ЗАГРУЗКА КУРСОВ ВАЛЮТ (неделя) ---")
    load_cbr_currency_to_bronze(start_date="2026-10-01", end_date="2026-10-07")
    
    print("\n--- ЗАГРУЗКА КЛЮЧЕВОЙ СТАВКИ (неделя) ---")
    load_cbr_key_rate_to_bronze(start_date="2026-10-01", end_date="2026-10-07")
    
    print("\n--- ЗАГРУЗКА СЕГОДНЯШНЕГО ДНЯ ---")
    load_cbr_currency_to_bronze()
    load_cbr_key_rate_to_bronze()