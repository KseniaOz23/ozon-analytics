import io
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import cbrapi as cbr

from src.config import load_config
from src.db import get_db_engine
from src.ingestion.logger import log_etl_start, log_etl_end

cfg = load_config("config/project.yaml")
retry_cfg = cfg.get("retry", {"max_attempts": 5, "wait_seconds": 5})

@retry(
    stop=stop_after_attempt(retry_cfg["max_attempts"]),
    wait=wait_fixed(retry_cfg["wait_seconds"]),
    retry=retry_if_exception_type((requests.RequestException, ValueError)),
    reraise=True
)
def fetch_cbr_currency_rates(date_req: str) -> pd.DataFrame:
    url = "https://www.cbr.ru/scripts/XML_daily.asp"
    resp = requests.get(url, params={"date_req": date_req}, timeout=30)
    resp.raise_for_status()
    return pd.read_xml(
        io.BytesIO(resp.content),
        xpath=".//Valute",
        encoding="windows-1251",
    )

def get_last_cbr_date(table: str, date_col: str) -> str:
    engine = get_db_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT MAX({date_col}) FROM {table}"))
        max_date = result.scalar()
    
    if max_date is None:
        return cfg['cbr']['default_start_date']
    return (max_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

def load_cbr_key_rate_to_bronze():
    log_id = log_etl_start("CBR_KEY_RATE")
    try:
        start_date = get_last_cbr_date("bronze.raw_cbr_key_rate", "rate_date")
        end_date = datetime.today().strftime("%Y-%m-%d")
        
        if start_date > end_date:
            log_etl_end(log_id, "SUCCESS", 0)
            return

        # cbrapi возвращает pandas Series
        series = cbr.get_key_rate(start_date, end_date)
        df = series.to_frame(name="key_rate").reset_index()
        df = df.rename(columns={"index": "rate_date"})
        
        if df.empty:
            log_etl_end(log_id, "SUCCESS", 0)
            return

        df["rate_date"] = pd.to_datetime(df["rate_date"]).dt.date
        records = df.to_dict("records")
        
        engine = get_db_engine()
        with engine.begin() as conn:
            query = text("""
                INSERT INTO bronze.raw_cbr_key_rate (rate_date, key_rate)
                VALUES (:rate_date, :key_rate)
                ON CONFLICT (rate_date) DO NOTHING
            """)
            conn.execute(query, records)
            
        log_etl_end(log_id, "SUCCESS", len(df))
        print(f"Ключевая ставка загружена: {len(df)} строк.")
        
    except Exception as e:
        log_etl_end(log_id, "FAILED", 0, str(e))
        raise e

# Функцию загрузки курсов валют можно реализовать аналогично