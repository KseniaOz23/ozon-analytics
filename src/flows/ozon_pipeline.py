from prefect import flow, task
import time

# Импортируем наши функции
from src.ingestion.moex import load_moex_to_bronze
from src.ingestion.cbr import load_cbr_currency_to_bronze, load_cbr_key_rate_to_bronze
from src.transformations.silver import transform_moex_to_silver, transform_macro_to_silver
from src.transformations.gold import build_gold_mart
from src.transformations.scd2 import update_key_rate_scd2
from src.dq.checks import run_dq_checks
from src.ml.models import train_and_forecast

# Оборачиваем каждую функцию в Prefect @task
@task(name="Загрузка MOEX")
def task_ingest_moex():
    load_moex_to_bronze()

@task(name="Загрузка курсов ЦБ")
def task_ingest_cbr_currency():
    load_cbr_currency_to_bronze()

@task(name="Загрузка ключевой ставки")
def task_ingest_cbr_key_rate():
    load_cbr_key_rate_to_bronze()

@task(name="Очистка MOEX (Silver)")
def task_silver_moex():
    transform_moex_to_silver()

@task(name="Очистка макро (Silver)")
def task_silver_macro():
    transform_macro_to_silver()

@task(name="Сборка витрины (Gold)")
def task_gold_mart():
    build_gold_mart()

@task(name="Обновление SCD2")
def task_scd2():
    update_key_rate_scd2()

@task(name="Проверки качества (DQ)")
def task_dq():
    run_dq_checks()

@task(name="Прогнозирование (ML)")
def task_ml():
    train_and_forecast()

# Главный пайплайн (Flow)
@flow(name="OZON Analytics Pipeline", log_prints=True)
def ozon_pipeline():
    print("Старт пайплайна...")
    
    # 1. Загрузка данных (параллельно)
    moex_future = task_ingest_moex.submit()
    cbr_cur_future = task_ingest_cbr_currency.submit()
    cbr_rate_future = task_ingest_cbr_key_rate.submit()
    
    # Ждем завершения загрузки
    moex_future.result()
    cbr_cur_future.result()
    cbr_rate_future.result()
    
    # 2. Трансформации Silver (параллельно)
    silver_moex_future = task_silver_moex.submit()
    silver_macro_future = task_silver_macro.submit()
    
    # Ждем завершения Silver
    silver_moex_future.result()
    silver_macro_future.result()
    
    # 3. Сборка Gold
    task_gold_mart()
    
    # 4. Профильные компоненты и ML (параллельно)
    scd2_future = task_scd2.submit()
    dq_future = task_dq.submit()
    ml_future = task_ml.submit()
    
    # Ждем завершения
    scd2_future.result()
    dq_future.result()
    ml_future.result()
    
    print("Пайплайн успешно завершен!")

if __name__ == "__main__":
    # Запуск пайплайна по расписанию (каждый день в 00:00)
    ozon_pipeline.serve(
        name="daily-ozon-pipeline",
        cron="0 0 * * *",
        tags=["ozon", "daily"]
    )