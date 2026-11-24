import pandas as pd
from sqlalchemy import text
from src.db import get_db_engine
from src.ingestion.logger import log_etl_start, log_etl_end

def update_key_rate_scd2():
    log_id = log_etl_start("SCD2_KEY_RATE")
    engine = get_db_engine()
    
    try:
        # 1. Читаем уникальные значения ключевой ставки из Bronze
        with engine.connect() as conn:
            df_bronze = pd.read_sql(
                "SELECT DISTINCT rate_date, key_rate FROM bronze.raw_cbr_key_rate ORDER BY rate_date", 
                conn
            )
        
        if df_bronze.empty:
            print("Нет данных для SCD2.")
            log_etl_end(log_id, "SUCCESS", 0)
            return

        # 2. Читаем текущую историю из SCD2
        with engine.connect() as conn:
            df_current = pd.read_sql(
                "SELECT * FROM silver.dim_key_rate_history WHERE is_current = TRUE", 
                conn
            )

        # 3. Определяем, изменилась ли ставка
        # Берем последнюю ставку из Bronze
        latest_bronze = df_bronze.iloc[-1]
        latest_rate = latest_bronze["key_rate"]
        latest_date = pd.to_datetime(latest_bronze["rate_date"])

        if not df_current.empty:
            current_rate = df_current.iloc[0]["key_rate"]
            current_valid_from = pd.to_datetime(df_current.iloc[0]["valid_from"])
            
            if current_rate == latest_rate:
                print(f"Ставка не изменилась ({latest_rate}). SCD2 не требует обновления.")
                log_etl_end(log_id, "SUCCESS", 0)
                return
            else:
                # Ставка изменилась: закрываем старую запись
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            UPDATE silver.dim_key_rate_history 
                            SET valid_to = :valid_to, is_current = FALSE 
                            WHERE is_current = TRUE
                        """),
                        {"valid_to": latest_date.to_pydatetime()}
                    )
                print(f"Ставка изменилась: {current_rate} -> {latest_rate}")

        # 4. Добавляем новую запись
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO silver.dim_key_rate_history (key_rate, valid_from, is_current)
                    VALUES (:key_rate, :valid_from, TRUE)
                """),
                {
                    "key_rate": float(latest_rate),        # <-- float()
                    "valid_from": latest_date.to_pydatetime() # <-- обычный datetime
                }
            )
        
        print(f"SCD2 обновлен: новая ставка {latest_rate} с {latest_date.date()}.")
        log_etl_end(log_id, "SUCCESS", 1)
        
    except Exception as e:
        log_etl_end(log_id, "FAILED", 0, str(e))
        raise e

if __name__ == "__main__":
    update_key_rate_scd2()