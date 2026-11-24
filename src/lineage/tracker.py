from sqlalchemy import text
from src.db import get_db_engine

def add_lineage(source_object: str, target_object: str, process_name: str):
    """Записывает связь между источником и целью в таблицу lineage."""
    engine = get_db_engine()
    with engine.begin() as conn:
        query = text("""
            INSERT INTO service.data_lineage (source_object, target_object, process_name)
            VALUES (:source_object, :target_object, :process_name)
        """)
        conn.execute(query, {
            "source_object": source_object,
            "target_object": target_object,
            "process_name": process_name
        })
    print(f"Lineage записан: {source_object} -> {target_object} ({process_name})")

def init_lineage():
    """Инициализация базовых связей (вызывается один раз или при необходимости)."""
    # Связи Bronze -> Silver
    add_lineage("bronze.raw_moex", "silver.clean_moex", "transform_moex_to_silver")
    add_lineage("bronze.raw_cbr_currency", "silver.clean_macro", "transform_macro_to_silver")
    add_lineage("bronze.raw_cbr_key_rate", "silver.clean_macro", "transform_macro_to_silver")
    
    # Связи Silver -> Gold
    add_lineage("silver.clean_moex", "gold.mart_daily_metrics", "build_gold_mart")
    add_lineage("silver.clean_macro", "gold.mart_daily_metrics", "build_gold_mart")

if __name__ == "__main__":
    init_lineage()