import pandas as pd
from sqlalchemy import text
from src.db import get_db_engine
from src.ingestion.logger import log_etl_start, log_etl_end

def run_dq_checks():
    log_id = log_etl_start("DQ_CHECKS")
    engine = get_db_engine()
    results = []
    
    try:
        with engine.connect() as conn:
            # 1. Читаем данные для проверки (например, Gold-витрину)
            df_gold = pd.read_sql("SELECT * FROM gold.mart_daily_metrics", conn)
            df_silver_moex = pd.read_sql("SELECT * FROM silver.clean_moex", conn)
            
        if df_gold.empty:
            print("Нет данных для DQ-проверок.")
            log_etl_end(log_id, "SUCCESS", 0)
            return

        # ==========================================
        # 1. ПОЛНОТА (Completeness)
        # ==========================================
        # Проверяем, что в витрине нет пропусков в ключевых колонках
        critical_cols = ["close_price", "volume", "key_rate"]
        missing_counts = df_gold[critical_cols].isna().sum()
        for col, count in missing_counts.items():
            status = "PASSED" if count == 0 else "FAILED"
            details = f"Пропусков в {col}: {count}"
            results.append(("gold.mart_daily_metrics", "completeness", status, details))

        # ==========================================
        # 2. УНИКАЛЬНОСТЬ (Uniqueness)
        # ==========================================
        # Проверяем, что trade_date уникальна в витрине
        dup_count = df_gold.duplicated(subset=["trade_date"]).sum()
        status = "PASSED" if dup_count == 0 else "FAILED"
        results.append(("gold.mart_daily_metrics", "uniqueness", status, f"Дубликатов по trade_date: {dup_count}"))

        # ==========================================
        # 3. ДОПУСТИМОСТЬ ЗНАЧЕНИЙ (Validity)
        # ==========================================
        # Цена и объем должны быть больше нуля
        invalid_prices = (df_gold["close_price"] <= 0).sum()
        invalid_volumes = (df_gold["volume"] <= 0).sum()
        status = "PASSED" if (invalid_prices == 0 and invalid_volumes == 0) else "FAILED"
        results.append(("gold.mart_daily_metrics", "validity", status, 
                        f"Цен <= 0: {invalid_prices}, Объемов <= 0: {invalid_volumes}"))

        # ==========================================
        # 4. СООТВЕТСТВИЕ ТИПАМ (Data Types)
        # ==========================================
        # Проверяем, что колонки имеют правильный тип (в базе это уже гарантировано схемой, 
        # но проверим в pandas)
        type_ok = pd.api.types.is_numeric_dtype(df_gold["close_price"]) and pd.api.types.is_numeric_dtype(df_gold["volume"])
        status = "PASSED" if type_ok else "FAILED"
        results.append(("gold.mart_daily_metrics", "data_types", status, f"Типы данных корректны: {type_ok}"))

        # ==========================================
        # 5. СООТВЕТСТВИЕ ДИАПАЗОНАМ (Range)
        # ==========================================
        # Цена акции OZON в разумных пределах (например, от 1000 до 10000 руб.)
        out_of_range = ((df_gold["close_price"] < 1000) | (df_gold["close_price"] > 10000)).sum()
        status = "PASSED" if out_of_range == 0 else "FAILED"
        results.append(("gold.mart_daily_metrics", "range", status, f"Цен вне диапазона [1000, 10000]: {out_of_range}"))

        # ==========================================
        # 6. ССЫЛОЧНАЯ ЦЕЛОСТНОСТЬ (Referential Integrity)
        # ==========================================
        # Проверяем, что все тикеры в витрине есть в справочнике (у нас пока один тикер, 
        # но проверим, что он не пустой)
        null_tickers = df_gold["ticker"].isna().sum()
        status = "PASSED" if null_tickers == 0 else "FAILED"
        results.append(("gold.mart_daily_metrics", "referential_integrity", status, f"Пустых тикеров: {null_tickers}"))

        # ==========================================
        # 7. АКТУАЛЬНОСТЬ (Freshness)
        # ==========================================
        # Проверяем, что последняя дата в витрине не старше 5 дней от сегодняшней
        max_date = pd.to_datetime(df_gold["trade_date"]).max().date()
        today = pd.Timestamp.today().date()
        days_diff = (today - max_date).days
        status = "PASSED" if days_diff <= 5 else "FAILED"
        results.append(("gold.mart_daily_metrics", "freshness", status, f"Последняя дата: {max_date}, дней назад: {days_diff}"))

        # ==========================================
        # СОХРАНЕНИЕ РЕЗУЛЬТАТОВ В БД
        # ==========================================
        with engine.begin() as conn:
            query = text("""
                INSERT INTO service.dq_results (table_name, check_type, status, details)
                VALUES (:table_name, :check_type, :status, :details)
            """)
            for res in results:
                conn.execute(query, {
                    "table_name": res[0],
                    "check_type": res[1],
                    "status": res[2],
                    "details": res[3]
                })
                
        # Вывод в консоль
        print("\n--- РЕЗУЛЬТАТЫ DQ-ПРОВЕРОК ---")
        for res in results:
            print(f"[{res[2]}] {res[1]}: {res[3]}")
            
        log_etl_end(log_id, "SUCCESS", len(results))
        
    except Exception as e:
        log_etl_end(log_id, "FAILED", 0, str(e))
        raise e

if __name__ == "__main__":
    run_dq_checks()