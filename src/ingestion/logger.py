from datetime import datetime
from sqlalchemy import text
from src.db import get_db_engine

def log_etl_start(source_name: str) -> int:
    """Создает запись о начале загрузки и возвращает ID лога."""
    engine = get_db_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO service.etl_load_log (source_name, status) 
                VALUES (:source_name, 'RUNNING') 
                RETURNING id
            """),
            {"source_name": source_name}
        )
        return result.fetchone()[0]

def log_etl_end(log_id: int, status: str, rows_loaded: int = 0, error_message: str = None):
    """Обновляет запись о загрузке."""
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE service.etl_load_log 
                SET end_time = :end_time, status = :status, rows_loaded = :rows_loaded, error_message = :error_message
                WHERE id = :id
            """),
            {
                "end_time": datetime.now(),
                "status": status,
                "rows_loaded": rows_loaded,
                "error_message": error_message,
                "id": log_id
            }
        )